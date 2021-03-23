package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.base.BaseObject;
import com.alibaba.datax.common.util.Configuration;

/**
 * 插件骨架类
 */
public abstract class AbstractPlugin extends BaseObject implements Pluginable {

    // 从插件接口层可以发现 插件应该是有一个插件级别的config 和一个job级别的config
    private Configuration pluginJobConf;

    private Configuration pluginConf;

    // 对端的config指的是什么
    private Configuration peerPluginJobConf;

    private String peerPluginName;

    @Override
    public String getPluginName() {
        assert null != this.pluginConf;
        return this.pluginConf.getString("name");
    }

    @Override
    public String getDeveloper() {
        assert null != this.pluginConf;
        return this.pluginConf.getString("developer");
    }

    @Override
    public String getDescription() {
        assert null != this.pluginConf;
        return this.pluginConf.getString("description");
    }

    // 下面都是一些简单的bean操作

    @Override
    public Configuration getPluginJobConf() {
        return pluginJobConf;
    }

    @Override
    public void setPluginJobConf(Configuration pluginJobConf) {
        this.pluginJobConf = pluginJobConf;
    }

    @Override
    public void setPluginConf(Configuration pluginConf) {
        this.pluginConf = pluginConf;
    }

    @Override
    public Configuration getPeerPluginJobConf() {
        return peerPluginJobConf;
    }

    @Override
    public void setPeerPluginJobConf(Configuration peerPluginJobConf) {
        this.peerPluginJobConf = peerPluginJobConf;
    }

    @Override
    public String getPeerPluginName() {
        return peerPluginName;
    }

    @Override
    public void setPeerPluginName(String peerPluginName) {
        this.peerPluginName = peerPluginName;
    }

    public void preCheck() {
    }

    public void prepare() {
    }

    public void post() {
    }

    public void preHandler(Configuration jobConfiguration) {

    }

    public void postHandler(Configuration jobConfiguration) {

    }
}
