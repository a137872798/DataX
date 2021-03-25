package com.alibaba.datax.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import org.apache.commons.lang.Validate;

/**
 * 该对象从configuration中抽取必要的信息用于描述 TG/job
 */
public abstract class AbstractContainer {

    /**
     * 初始化该对象时使用的配置项
     * 不同的级别对应不同的配置 job对应全局配置 TG对应任务组级别配置
      */
    protected Configuration configuration;

    /**
     * 相匹配的沟通者 维护的信息维度也不同
     */
    protected AbstractContainerCommunicator containerCommunicator;

    public AbstractContainer(Configuration configuration) {
        Validate.notNull(configuration, "Configuration can not be null.");

        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public AbstractContainerCommunicator getContainerCommunicator() {
        return containerCommunicator;
    }

    public void setContainerCommunicator(AbstractContainerCommunicator containerCommunicator) {
        this.containerCommunicator = containerCommunicator;
    }

    public abstract void start();

}
