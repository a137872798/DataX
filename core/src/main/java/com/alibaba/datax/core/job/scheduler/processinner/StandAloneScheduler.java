package com.alibaba.datax.core.job.scheduler.processinner;

import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;

/**
 * Created by hongjiao.hj on 2014/12/22.
 */
public class StandAloneScheduler extends ProcessInnerScheduler{

    public StandAloneScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    /**
     * 代表该对象不支持关闭job
     * @param jobId
     * @return
     */
    @Override
    protected boolean isJobKilling(Long jobId) {
        return false;
    }

}
