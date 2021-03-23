package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.util.Configuration;

/**
 * 插件对外暴露的公共接口
 */
public interface Pluginable {

    /**
     * 获取本插件的开发者信息
     * @return
     */
    String getDeveloper();

    /**
     * 插件的描述信息
     * @return
     */
    String getDescription();

    /**
     * 插件会从config中读取对应的配置项
     * @param pluginConf
     */
    void setPluginConf(Configuration pluginConf);

    // init destroy 对应插件的生命周期
	void init();

	void destroy();

    String getPluginName();

    /**
     * 仅获取插件job相关的配置项
     * @return
     */
    Configuration getPluginJobConf();

    /**
     * 什么是peer-config
     * @return
     */
    Configuration getPeerPluginJobConf();

    public String getPeerPluginName();

    // 设置job级别的插件
    void setPluginJobConf(Configuration jobConf);

    void setPeerPluginJobConf(Configuration peerPluginJobConf);

    public void setPeerPluginName(String peerPluginName);

}
