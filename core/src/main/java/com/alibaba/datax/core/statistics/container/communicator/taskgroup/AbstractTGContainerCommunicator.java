package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerCollector;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang.Validate;

import java.util.List;
import java.util.Map;

/**
 * 当ContainerCommunicator对应的容器是TG级别时 对应的沟通对象也是TG级别
 */
public abstract class AbstractTGContainerCommunicator extends AbstractContainerCommunicator {

    protected long jobId;

    /**
     * 由于taskGroupContainer是进程内部调度
     * 其registerCommunication()，getCommunication()，
     * getCommunications()，collect()等方法是一致的
     * 所有TG的Collector都是ProcessInnerCollector
     */
    protected int taskGroupId;

    public AbstractTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        this.jobId = configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        super.setCollector(new ProcessInnerCollector(this.jobId));

        // 区别就是TG沟通管理对象 会读取TGid
        this.taskGroupId = configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        super.getCollector().registerTaskCommunication(configurationList);
    }

    @Override
    public final Communication collect() {
        return this.getCollector().collectFromTask();
    }

    @Override
    public final State collectState() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication :
                super.getCollector().getTaskCommunicationMap().values()) {
            communication.mergeStateFrom(taskCommunication);
        }

        return communication.getState();
    }

    @Override
    public final Communication getCommunication(Integer taskId) {
        Validate.isTrue(taskId >= 0, "注册的taskId不能小于0");

        return super.getCollector().getTaskCommunication(taskId);
    }

    @Override
    public final Map<Integer, Communication> getCommunicationMap() {
        return super.getCollector().getTaskCommunicationMap();
    }

}
