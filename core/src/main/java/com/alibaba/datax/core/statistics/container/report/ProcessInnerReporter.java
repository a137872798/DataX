package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

/**
 * 默认情况下 当TG级别的沟通对象发生变化时 会更新全局容器
 */
public class ProcessInnerReporter extends AbstractReporter {

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
        // do nothing
    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroupId, communication);
    }
}