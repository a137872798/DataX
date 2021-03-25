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
 * 将内部维护的数据细化到task级别  本容器可能是TG级别 也可能是job级别
 */
public abstract class AbstractCollector {

    /**
     * 每个taskId 对应一个沟通对象 外部通过访问该对象获取当前任务进行状态
     * 这里可能是某个job下所有的task 也可能是某个TG下所有的task
     */
    private Map<Integer, Communication> taskCommunicationMap = new ConcurrentHashMap<Integer, Communication>();

    /**
     * 无论是job 还是TG 他们都拥有jobId
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
     * 目前只有针对 job 级别的沟通者会调用该方法 该方法通过读取TGConfiguration
     * @param taskGroupConfigurationList
     */
    public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
        for (Configuration config : taskGroupConfigurationList) {
            int taskGroupId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            // 这里为什么要用一个额外的对象去存储 而不是直接用本对象
            LocalTGCommunicationManager.registerTaskGroupCommunication(taskGroupId, new Communication());
        }
    }

    /**
     * 插入task级别的沟通对象  该方法会在TG级别的沟通对象中调用
     * @param taskConfigurationList 对应task级别的config
     */
    public void registerTaskCommunication(List<Configuration> taskConfigurationList) {
        for (Configuration taskConfig : taskConfigurationList) {
            int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
            this.taskCommunicationMap.put(taskId, new Communication());
        }
    }

    /**
     * 将TG级别下 或者job级别下所有的task信息收集起来并返回
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

    /**
     * 返回TG级别的沟通对象 实际上针对TG级别的collector 应该是返回空Communication 因为TG级别的collector不会调用生成TG沟通对象的方法
     * @return
     */
    public abstract Communication collectFromTaskGroup();

    /**
     * 同上 但是没有将数据合并
     * @return
     */
    public Map<Integer, Communication> getTGCommunicationMap() {
        return LocalTGCommunicationManager.getTaskGroupCommunicationMap();
    }

    /**
     * 获取某个指定的TG
     * @param taskGroupId
     * @return
     */
    public Communication getTGCommunication(Integer taskGroupId) {
        return LocalTGCommunicationManager.getTaskGroupCommunication(taskGroupId);
    }

    /**
     * 获取任务级别的沟通对象
     * @param taskId
     * @return
     */
    public Communication getTaskCommunication(Integer taskId) {
        return this.taskCommunicationMap.get(taskId);
    }
}
