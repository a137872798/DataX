package com.alibaba.datax.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import org.apache.commons.lang.Validate;

/**
 * 容器的抽象类 容器既可以是针对taskGroup的 也可以是针对job的
 * Engine来定义整个数据转换/传输逻辑 而容器对象将他们包裹起来 外部通过协调者对象获取任务运行信息
 */
public abstract class AbstractContainer {

    /**
     * 全局配置对象
     */
    protected Configuration configuration;

    /**
     * 当容器是针对taskGroup级别时 对应的协调者应该也是TG级别
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

    /**
     * 该容器对外暴露了一个start方法  在调用engine.start 后会转发到该方法
     */
    public abstract void start();

}
