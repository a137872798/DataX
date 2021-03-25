package com.alibaba.datax.core.container.util;

/**
 * Created by xiafei.qiuxf on 14/12/17.
 */

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.Hook;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.JarLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * 扫描给定目录的所有一级子目录，每个子目录当作一个Hook的目录。
 * 对于每个子目录，必须符合ServiceLoader的标准目录格式，见http://docs.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html。
 * 加载里头的jar，使用ServiceLoader机制调用。
 *
 * 也就是允许指定一个上级目录 下面所有目录都按照SPI规则生成 可以按照SPI的方式加载钩子类
 */
public class HookInvoker {

    private static final Logger LOG = LoggerFactory.getLogger(HookInvoker.class);
    private final Map<String, Number> msg;
    private final Configuration conf;

    /**
     * 指定的基础目录
     */
    private File baseDir;

    /**
     *
     * @param baseDirName
     * @param conf 某些钩子对象在初始化时 可能需要从configuration中读取某些配置
     * @param msg
     */
    public HookInvoker(String baseDirName, Configuration conf, Map<String, Number> msg) {
        this.baseDir = new File(baseDirName);
        this.conf = conf;
        this.msg = msg;
    }

    /**
     * 加载目录下所有的钩子
     */
    public void invokeAll() {
        if (!baseDir.exists() || baseDir.isFile()) {
            LOG.info("No hook invoked, because base dir not exists or is a file: " + baseDir.getAbsolutePath());
            return;
        }

        // 要求的格式必须是某个父级目录下的所有子级目录
        String[] subDirs = baseDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory();
            }
        });

        if (subDirs == null) {
            throw DataXException.asDataXException(FrameworkErrorCode.HOOK_LOAD_ERROR, "获取HOOK子目录返回null");
        }

        for (String subDir : subDirs) {
            // 加载每个子级目录下的插件类
            doInvoke(new File(baseDir, subDir).getAbsolutePath());
        }

    }

    /**
     * 针对单个子级目录 加载所有钩子类
     * @param path
     */
    private void doInvoke(String path) {
        // 使用临时变量存储之前的类加载器
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            // 在初始化该类加载器时 已经将对应目录下的jar包设置进去了 之后通过SPI机制加载这些类 并且不同目录下的类 通过类加载器进行隔离
            JarLoader jarLoader = new JarLoader(new String[]{path});
            Thread.currentThread().setContextClassLoader(jarLoader);

            // 记录这些class名字的还是 spi文件 上面只是将这些实现类设置到类加载器中
            Iterator<Hook> hookIt = ServiceLoader.load(Hook.class).iterator();
            if (!hookIt.hasNext()) {
                LOG.warn("No hook defined under path: " + path);
            } else {
                // 加载所有hook后 并进行调用
                Hook hook = hookIt.next();
                LOG.info("Invoke hook [{}], path: {}", hook.getName(), path);
                hook.invoke(conf, msg);
            }
        } catch (Exception e) {
            LOG.error("Exception when invoke hook", e);
            throw DataXException.asDataXException(
                    CommonErrorCode.HOOK_INTERNAL_ERROR, "Exception when invoke hook", e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    public static void main(String[] args) {
        new HookInvoker("/Users/xiafei/workspace/datax3/target/datax/datax/hook",
                null, new HashMap<String, Number>()).invokeAll();
    }
}
