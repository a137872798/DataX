package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerReporter;
import com.alibaba.datax.core.statistics.communication.Communication;

/**
 * 应该是认为dataX目前只适合运行在单节点上 不支持集群模式
 */
public class StandaloneTGContainerCommunicator extends AbstractTGContainerCommunicator {

    public StandaloneTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        // 沟通者对象在创建后 会设置一个 collector 管理每个task对应的沟通对象  并且会生成一个reporter对象
        // reporter负责更新TG级别的沟通对象
        super.setReporter(new ProcessInnerReporter());
    }

    /**
     * 此时报告就是更新 TG沟通者对象
     * @param communication
     */
    @Override
    public void report(Communication communication) {
        super.getReporter().reportTGCommunication(super.taskGroupId, communication);
    }

}
