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

    /**
     * 对应 TG沟通者的骨架类
     * @param configuration
     */
    public AbstractTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        this.jobId = configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        // 每个TG/job 内部都有更小的任务单元 collector对象为更新粒度的对象单独维护统计数据 并在外部访问数据时，合并collector的数据
        super.setCollector(new ProcessInnerCollector(this.jobId));

        // 因为本对象针对的范围是TG级别 所以可以获取TGid
        this.taskGroupId = configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
    }

    /**
     * 可以看到 TG级别的沟通对象 对外暴露的api是插入task级别
     * @param configurationList
     */
    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        super.getCollector().registerTaskCommunication(configurationList);
    }

    /**
     * 将相关的collector下所有task的统计信息合并并返回 (因为本对象本身就是针对TG级别 所以统计数据时也只会存储task级别)
     * @return
     */
    @Override
    public final Communication collect() {
        return this.getCollector().collectFromTask();
    }

    /**
     * 通过合并此时维护的所有task级别的数据 判断此时该TG下所有任务的统一状态
     * @return
     */
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

    /**
     * 检索某个task对应的统计信息
     * @param taskId
     * @return
     */
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
