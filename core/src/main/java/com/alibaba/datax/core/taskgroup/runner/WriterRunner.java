package com.alibaba.datax.core.taskgroup.runner;

import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jingxing on 14-9-1.
 * <p/>
 * 同样的只是定义写入的模板  具体逻辑由插件实现
 */
public class WriterRunner extends AbstractRunner implements Runnable {

    private static final Logger LOG = LoggerFactory
            .getLogger(WriterRunner.class);

    /**
     * 通过该对象接收reader读取的数据 并通过writerPlugin进行写入
     */
    private RecordReceiver recordReceiver;

    public void setRecordReceiver(RecordReceiver receiver) {
        this.recordReceiver = receiver;
    }

    public WriterRunner(AbstractTaskPlugin abstractTaskPlugin) {
        super(abstractTaskPlugin);
    }

    /**
     * 每个启动的task 会专门为读写生成2条线程 并使用专门的类加载器 不期望被复用 所以不采用线程池
     */
    @Override
    public void run() {
        Validate.isTrue(this.recordReceiver != null);

        // 定义了如何写入数据的逻辑
        Writer.Task taskWriter = (Writer.Task) this.getPlugin();
        //统计waitReadTime，并且在finally end
        PerfRecord channelWaitRead = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WAIT_READ_TIME);
        try {
            channelWaitRead.start();
            LOG.debug("task writer starts to do init ...");
            PerfRecord initPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WRITE_TASK_INIT);
            // 执行插件的初始化操作 并记录耗时
            initPerfRecord.start();
            taskWriter.init();
            initPerfRecord.end();

            LOG.debug("task writer starts to do prepare ...");
            PerfRecord preparePerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WRITE_TASK_PREPARE);
            preparePerfRecord.start();
            taskWriter.prepare();
            preparePerfRecord.end();
            LOG.debug("task writer starts to write ...");

            PerfRecord dataPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WRITE_TASK_DATA);
            dataPerfRecord.start();
            // 开始阻塞读取接收器中的数据
            taskWriter.startWrite(recordReceiver);

            // 当从startWrite中返回时 代表任务已经正常完成了
            dataPerfRecord.addCount(CommunicationTool.getTotalReadRecords(super.getRunnerCommunication()));
            dataPerfRecord.addSize(CommunicationTool.getTotalReadBytes(super.getRunnerCommunication()));
            dataPerfRecord.end();

            LOG.debug("task writer starts to do post ...");
            PerfRecord postPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WRITE_TASK_POST);
            postPerfRecord.start();
            taskWriter.post();
            postPerfRecord.end();

            // 标记正常完成
            super.markSuccess();
        } catch (Throwable e) {
            LOG.error("Writer Runner Received Exceptions:", e);
            super.markFail(e);
        } finally {
            LOG.debug("task writer starts to do destroy ...");
            PerfRecord desPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WRITE_TASK_DESTROY);
            desPerfRecord.start();
            super.destroy();
            desPerfRecord.end();
            channelWaitRead.end(super.getRunnerCommunication().getLongCounter(CommunicationTool.WAIT_READER_TIME));
        }
    }
    
    public boolean supportFailOver(){
    	Writer.Task taskWriter = (Writer.Task) this.getPlugin();
    	return taskWriter.supportFailOver();
    }

    public void shutdown(){
        recordReceiver.shutdown();
    }
}
