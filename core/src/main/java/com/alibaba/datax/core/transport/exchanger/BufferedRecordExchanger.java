package com.alibaba.datax.core.transport.exchanger;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 可以看到 sender/receiver 内部是通过channel 来实现数据传输的
 */
public class BufferedRecordExchanger implements RecordSender, RecordReceiver {

	/**
	 * 该对象负责缓存传输中的数据  默认实现是一个阻塞队列
	 */
	private final Channel channel;

	private final Configuration configuration;

	private final List<Record> buffer;

	private int bufferSize ;

	protected final int byteCapacity;

	private final AtomicInteger memoryBytes = new AtomicInteger(0);

	private int bufferIndex = 0;

	private static Class<? extends Record> RECORD_CLASS;

	private volatile boolean shutdown = false;

	/**
	 * 采集插件在执行过程中的失败信息
	 */
	private final TaskPluginCollector pluginCollector;

	/**
	 * 初始化 exchanger对象
	 * @param channel
	 * @param pluginCollector
	 */
	@SuppressWarnings("unchecked")
	public BufferedRecordExchanger(final Channel channel, final TaskPluginCollector pluginCollector) {
		assert null != channel;
		assert null != channel.getConfiguration();

		this.channel = channel;
		this.pluginCollector = pluginCollector;
		// 这个conf 对应的是TG级别的
		this.configuration = channel.getConfiguration();

		// 设置交换区的大小
		this.bufferSize = configuration
				.getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);
		this.buffer = new ArrayList<Record>(bufferSize);

		//channel的queue默认大小为8M，原来为64M
		this.byteCapacity = configuration.getInt(
				CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 8 * 1024 * 1024);

		try {
			// 从TG对象中获取传输的记录类型
			BufferedRecordExchanger.RECORD_CLASS = ((Class<? extends Record>) Class
					.forName(configuration.getString(
                            CoreConstant.DATAX_CORE_TRANSPORT_RECORD_CLASS,
                            "com.alibaba.datax.core.transport.record.DefaultRecord")));
		} catch (Exception e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.CONFIG_ERROR, e);
		}
	}

	@Override
	public Record createRecord() {
		try {
			return BufferedRecordExchanger.RECORD_CLASS.newInstance();
		} catch (Exception e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.CONFIG_ERROR, e);
		}
	}

	@Override
	public void sendToWriter(Record record) {
		if(shutdown){
			throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
		}

		Validate.notNull(record, "record不能为空.");

		if (record.getMemorySize() > this.byteCapacity) {
			this.pluginCollector.collectDirtyRecord(record, new Exception(String.format("单条记录超过大小限制，当前限制为:%s", this.byteCapacity)));
			return;
		}

		boolean isFull = (this.bufferIndex >= this.bufferSize || this.memoryBytes.get() + record.getMemorySize() > this.byteCapacity);
		if (isFull) {
			flush();
		}

		this.buffer.add(record);
		this.bufferIndex++;
		memoryBytes.addAndGet(record.getMemorySize());
	}

	@Override
	public void flush() {
		if(shutdown){
			throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
		}
		this.channel.pushAll(this.buffer);
		this.buffer.clear();
		this.bufferIndex = 0;
		this.memoryBytes.set(0);
	}

	@Override
	public void terminate() {
		if(shutdown){
			throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
		}
		flush();
		this.channel.pushTerminate(TerminateRecord.get());
	}

	@Override
	public Record getFromReader() {
		if(shutdown){
			throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
		}
		boolean isEmpty = (this.bufferIndex >= this.buffer.size());
		if (isEmpty) {
			receive();
		}

		Record record = this.buffer.get(this.bufferIndex++);
		if (record instanceof TerminateRecord) {
			record = null;
		}
		return record;
	}

	@Override
	public void shutdown(){
		shutdown = true;
		try{
			buffer.clear();
			channel.clear();
		}catch(Throwable t){
			t.printStackTrace();
		}
	}

	private void receive() {
		this.channel.pullAll(this.buffer);
		this.bufferIndex = 0;
		this.bufferSize = this.buffer.size();
	}
}
