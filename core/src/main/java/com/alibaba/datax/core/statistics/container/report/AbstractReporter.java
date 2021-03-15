package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;

/**
 * 该对象负责报告每个job/TG 此时执行的状况
 */
public abstract class AbstractReporter {

    public abstract void reportJobCommunication(Long jobId, Communication communication);

    public abstract void reportTGCommunication(Integer taskGroupId, Communication communication);

}
