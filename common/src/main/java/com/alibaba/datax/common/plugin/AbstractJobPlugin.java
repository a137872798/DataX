package com.alibaba.datax.common.plugin;

/**
 * Created by jingxing on 14-8-24.
 * job级别的插件对象
 */
public abstract class AbstractJobPlugin extends AbstractPlugin {
	/**
	 * @return the jobPluginCollector
	 * job级别的插件对象可能需要一些task运行时数据的支撑才可以实现一些功能 所以需要collector对象
	 */
	public JobPluginCollector getJobPluginCollector() {
		return jobPluginCollector;
	}

	/**
	 * @param jobPluginCollector
	 *            the jobPluginCollector to set
	 */
	public void setJobPluginCollector(
            JobPluginCollector jobPluginCollector) {
		this.jobPluginCollector = jobPluginCollector;
	}

	private JobPluginCollector jobPluginCollector;

}
