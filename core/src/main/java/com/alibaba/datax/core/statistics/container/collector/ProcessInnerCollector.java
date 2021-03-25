package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

/**
 * 无论是TG级别的沟通对象 还是job级别的沟通对象 内部的collector都是该对象 内部针对更细粒度级别的单位单独维护数据
 */
public class ProcessInnerCollector extends AbstractCollector {

    public ProcessInnerCollector(Long jobId) {
        super.setJobId(jobId);
    }

    /**
     * 通过该对象将所有TG级别的沟通数据合并后返回
     * @return
     */
    @Override
    public Communication collectFromTaskGroup() {
        return LocalTGCommunicationManager.getJobCommunication();
    }

}
