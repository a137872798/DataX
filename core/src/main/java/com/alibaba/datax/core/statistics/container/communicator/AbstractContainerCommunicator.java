package com.alibaba.datax.core.statistics.container.communicator;


import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.collector.AbstractCollector;
import com.alibaba.datax.core.statistics.container.report.AbstractReporter;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

import java.util.List;
import java.util.Map;

/**
 * 容器沟通者 外部应该是通过该对象获取一些运行信息
 * 既可以针对job级别 也可以针对TG级别
 */
public abstract class AbstractContainerCommunicator {

    /**
     * 相关的全局配置
     */
    private Configuration configuration;
    /**
     * 该对象维护的沟通对象跟容器级别挂钩 可能是TG级别 也可能是 Job级别
     */
    private AbstractCollector collector;
    /**
     * 该对象默认就是覆盖Communicator的旧数据
     */
    private AbstractReporter reporter;

    /**
     * 如果当前是job级别容器 此时对应的jobId
     * 如果当前是TG级别的容器 此时所属的Job
     */
    private Long jobId;

    private VMInfo vmInfo = VMInfo.getVmInfo();
    private long lastReportTime = System.currentTimeMillis();


    public Configuration getConfiguration() {
        return this.configuration;
    }

    public AbstractCollector getCollector() {
        return collector;
    }

    public AbstractReporter getReporter() {
        return reporter;
    }

    public void setCollector(AbstractCollector collector) {
        this.collector = collector;
    }

    public void setReporter(AbstractReporter reporter) {
        this.reporter = reporter;
    }

    public Long getJobId() {
        return jobId;
    }

    public AbstractContainerCommunicator(Configuration configuration) {
        this.configuration = configuration;
        this.jobId = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
    }


    public abstract void registerCommunication(List<Configuration> configurationList);

    public abstract Communication collect();

    public abstract void report(Communication communication);

    public abstract State collectState();

    public abstract Communication getCommunication(Integer id);

    /**
     * 当 实现是 TGContainerCommunicator 时，返回的 Map: key=taskId, value=Communication
     * 当 实现是 JobContainerCommunicator 时，返回的 Map: key=taskGroupId, value=Communication
     */
    public abstract Map<Integer, Communication> getCommunicationMap();

    public void resetCommunication(Integer id){
        Map<Integer, Communication> map = getCommunicationMap();
        map.put(id, new Communication());
    }

    public void reportVmInfo(){
        long now = System.currentTimeMillis();
        //每5分钟打印一次
        if(now - lastReportTime >= 300000) {
            //当前仅打印
            if (vmInfo != null) {
                vmInfo.getDelta(true);
            }
            lastReportTime = now;
        }
    }
}