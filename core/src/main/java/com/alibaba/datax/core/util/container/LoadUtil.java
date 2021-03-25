package com.alibaba.datax.core.util.container;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.AbstractPlugin;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * 插件加载器，大体上分reader、transformer（还未实现）和writer三中插件类型，
 * reader和writer在执行时又可能出现Job和Task两种运行时（加载的类不同）
 */
public class LoadUtil {
    private static final String pluginTypeNameFormat = "plugin.%s.%s";

    private LoadUtil() {
    }

    private enum ContainerType {
        Job("Job"), Task("Task");
        private String type;

        private ContainerType(String type) {
            this.type = type;
        }

        public String value() {
            return type;
        }
    }

    /**
     * 所有插件配置放置在pluginRegisterCenter中，为区别reader、transformer和writer，还能区别
     * 具体pluginName，故使用pluginType.pluginName作为key放置在该map中
     */
    private static Configuration pluginRegisterCenter;

    /**
     * key 对应某个插件key  value对应自定义类加载器 这样的设计可以确保在插件这一层 jar包是相互隔离的
     */
    private static Map<String, JarLoader> jarLoaderCenter = new HashMap<String, JarLoader>();

    /**
     * 设置pluginConfigs，方便后面插件来获取
     *
     * @param pluginConfigs
     */
    public static void bind(Configuration pluginConfigs) {
        pluginRegisterCenter = pluginConfigs;
    }

    private static String generatePluginKey(PluginType pluginType,
                                            String pluginName) {
        return String.format(pluginTypeNameFormat, pluginType.toString(),
                pluginName);
    }

    /**
     * 获取该插件相关的配置项
     * @param pluginType
     * @param pluginName
     * @return
     */
    private static Configuration getPluginConf(PluginType pluginType,
                                               String pluginName) {
        // 插件相关的配置一开始就会存储在 pluginRegisterCenter中，当需要获取相关的配置时，只需要以插件类名作为key
        Configuration pluginConf = pluginRegisterCenter
                .getConfiguration(generatePluginKey(pluginType, pluginName));

        if (null == pluginConf) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_INSTALL_ERROR,
                    String.format("DataX不能找到插件[%s]的配置.",
                            pluginName));
        }

        return pluginConf;
    }

    /**
     * 通过反射实例化插件对象  在此之前一般已经在当前线程设置了该插件专有的ClassLoader
     * @param pluginType 定义了插件类型
     * @param pluginName 定义了插件名
     * @return
     */
    public static AbstractJobPlugin loadJobPlugin(PluginType pluginType,
                                                  String pluginName) {
        // 此时已经基于反射 获取到了插件.class对象
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(
                pluginType, pluginName, ContainerType.Job);

        try {
            // 实例化对象后 设置conf
            AbstractJobPlugin jobPlugin = (AbstractJobPlugin) clazz
                    .newInstance();
            jobPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
            return jobPlugin;
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    String.format("DataX找到plugin[%s]的Job配置.",
                            pluginName), e);
        }
    }

    /**
     * 加载taskPlugin，reader、writer都可能加载
     *
     * @param pluginType
     * @param pluginName
     * @return
     */
    public static AbstractTaskPlugin loadTaskPlugin(PluginType pluginType,
                                                    String pluginName) {
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(
                pluginType, pluginName, ContainerType.Task);

        try {
            AbstractTaskPlugin taskPlugin = (AbstractTaskPlugin) clazz
                    .newInstance();
            taskPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
            return taskPlugin;
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
                    String.format("DataX不能找plugin[%s]的Task配置.",
                            pluginName), e);
        }
    }

    /**
     * 根据插件类型、名字和执行时taskGroupId加载对应运行器
     *
     * @param pluginType
     * @param pluginName
     * @return
     */
    public static AbstractRunner loadPluginRunner(PluginType pluginType, String pluginName) {
        AbstractTaskPlugin taskPlugin = LoadUtil.loadTaskPlugin(pluginType,
                pluginName);

        switch (pluginType) {
            case READER:
                return new ReaderRunner(taskPlugin);
            case WRITER:
                return new WriterRunner(taskPlugin);
            default:
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        String.format("插件[%s]的类型必须是[reader]或[writer]!",
                                pluginName));
        }
    }

    /**
     * 反射出具体plugin实例
     *
     * @param pluginType
     * @param pluginName
     * @param pluginRunType
     * @return
     */
    @SuppressWarnings("unchecked")
    private static synchronized Class<? extends AbstractPlugin> loadPluginClass(
            PluginType pluginType, String pluginName,
            ContainerType pluginRunType) {
        Configuration pluginConf = getPluginConf(pluginType, pluginName);
        JarLoader jarLoader = LoadUtil.getJarLoader(pluginType, pluginName);
        try {
            // 可以看到这里加载的都是内部类 xxx$Job/Task
            return (Class<? extends AbstractPlugin>) jarLoader
                    .loadClass(pluginConf.getString("class") + "$"
                            + pluginRunType.value());
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

    /**
     * 根据指定的插件类型和插件名 生成类加载器
     * @param pluginType
     * @param pluginName
     * @return
     */
    public static synchronized JarLoader getJarLoader(PluginType pluginType,
                                                      String pluginName) {
        // 首先获取该插件相关的配置
        Configuration pluginConf = getPluginConf(pluginType, pluginName);

        // 每个插件需要加载的特殊jar包 通过类加载器这一层进行隔离
        JarLoader jarLoader = jarLoaderCenter.get(generatePluginKey(pluginType,
                pluginName));
        if (null == jarLoader) {
            // 这类类加载器在初始化时 已经执行了jar包目录
            String pluginPath = pluginConf.getString("path");
            if (StringUtils.isBlank(pluginPath)) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        String.format(
                                "%s插件[%s]路径非法!",
                                pluginType, pluginName));
            }
            jarLoader = new JarLoader(new String[]{pluginPath});
            jarLoaderCenter.put(generatePluginKey(pluginType, pluginName),
                    jarLoader);
        }

        return jarLoader;
    }
}
