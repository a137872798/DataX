package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 将沟通对象的级别细化到task
 */
public abstract class AbstractCollector {

    /**
     * 每个taskId 对应一个沟通对象 外部通过访问该对象获取当前任务进行状态
     * 这里可能是某个job下所有的task 也可能是某个TG下所有的task
     */
    private Map<Integer, Communication> taskCommunicationMap = new ConcurrentHashMap<Integer, Communication>();

    /**
     * 每个容器对应一个job
     */
    private Long jobId;

    public Map<Integer, Communication> getTaskCommunicationMap() {
        return taskCommunicationMap;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    /**
     * 可以看到 容器级别可以以TG为单位， 每个容器对应一个configuration  那么这里对应一组configuration 就是对应多个TG
     * @param taskGroupConfigurationList
     */
    public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
        for (Configuration config : taskGroupConfigurationList) {
            // 每个任务组存在一个id
            int taskGroupId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            LocalTGCommunicationManager.registerTaskGroupCommunication(taskGroupId, new Communication());
        }
    }

    /**
     * 插入task级别的沟通对象
     * @param taskConfigurationList
     */
    public void registerTaskCommunication(List<Configuration> taskConfigurationList) {
        for (Configuration taskConfig : taskConfigurationList) {
            int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
            this.taskCommunicationMap.put(taskId, new Communication());
        }
    }

    /**
     * 该对象是关联在 ContainerCommunicator上的 根据容器的级别不同 这里可能返回的是TG级别 也可能是Job级别
     * @return
     */
    public Communication collectFromTask() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication :
                this.taskCommunicationMap.values()) {
            communication.mergeFrom(taskCommunication);
        }

        return communication;
    }

    public abstract Communication collectFromTaskGroup();

    public Map<Integer, Communication> getTGCommunicationMap() {
        return LocalTGCommunicationManager.getTaskGroupCommunicationMap();
    }

    public Communication getTGCommunication(Integer taskGroupId) {
        return LocalTGCommunicationManager.getTaskGroupCommunication(taskGroupId);
    }

    public Communication getTaskCommunication(Integer taskId) {
        return this.taskCommunicationMap.get(taskId);
    }
}
