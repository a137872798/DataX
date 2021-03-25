package com.alibaba.datax.core.job.scheduler.processinner;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Scheduler 作为整个Job启动的入口 并对子类开放钩子
 */
public abstract class ProcessInnerScheduler extends AbstractScheduler {

    /**
     * 通过线程池 执行任务 提高并行度
     */
    private ExecutorService taskGroupContainerExecutorService;

    /**
     *
     * @param containerCommunicator 对应job级别的沟通者
     */
    public ProcessInnerScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    /**
     * 以TG为单位 启动所有任务
     * @param configurations
     */
    @Override
    public void startAllTaskGroup(List<Configuration> configurations) {
        // 可以看到每个TG对应一条线程
        this.taskGroupContainerExecutorService = Executors
                .newFixedThreadPool(configurations.size());

        for (Configuration taskGroupConfiguration : configurations) {
            // 通过每个TG的配置项 生成执行任务组的 runner对象
            TaskGroupContainerRunner taskGroupContainerRunner = newTaskGroupContainerRunner(taskGroupConfiguration);
            this.taskGroupContainerExecutorService.execute(taskGroupContainerRunner);
        }

        // 这样可以拒绝线程池提交新的任务
        this.taskGroupContainerExecutorService.shutdown();
    }

    // 默认情况下 在scheduler检测到job被关闭 或者失败时 直接抛出异常

    @Override
    public void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable) {
        // 这里会强制关闭所有正在执行的任务 (通过线程中断机制)
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(
                FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }


    @Override
    public void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {
        //通过进程退出返回码标示状态
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(FrameworkErrorCode.KILLED_EXIT_VALUE,
                "job killed status");
    }


    /**
     * 将TG配置包装成runner对象
     * @param configuration
     * @return
     */
    private TaskGroupContainerRunner newTaskGroupContainerRunner(
            Configuration configuration) {
        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(configuration);

        return new TaskGroupContainerRunner(taskGroupContainer);
    }

}
