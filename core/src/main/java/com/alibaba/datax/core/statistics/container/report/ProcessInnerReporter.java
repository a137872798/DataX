package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

/**
 * 这里的报告逻辑就是更新 TG级别的沟通数据
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