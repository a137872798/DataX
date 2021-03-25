package com.alibaba.datax.common.plugin;

/**
 * Created by jingxing on 14-8-24.
 * 真正使用的应该是 AbstractJobPlugin/AbstractTaskPlugin
 * 也就是dataX 具备2种维度的数据 一种针对job 一种针对task  job代表定义的一个数据传输规则
 * task则是数据传输的最小单位
 */
public abstract class AbstractTaskPlugin extends AbstractPlugin {

    /**
     * job->taskGroup->task
     */
    private int taskGroupId;
    private int taskId;
    /**
     * 这里存储的数据就是以task为单位
     */
    private TaskPluginCollector taskPluginCollector;

    public TaskPluginCollector getTaskPluginCollector() {
        return taskPluginCollector;
    }

    public void setTaskPluginCollector(
            TaskPluginCollector taskPluginCollector) {
        this.taskPluginCollector = taskPluginCollector;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    public void setTaskGroupId(int taskGroupId) {
        this.taskGroupId = taskGroupId;
    }
}
