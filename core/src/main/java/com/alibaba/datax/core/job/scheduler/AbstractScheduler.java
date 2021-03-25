package com.alibaba.datax.core.job.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 定时器骨架 当通过该对象开启job后 会每个一段时间 通过检测协调者对象 观测此时job的运作状况 并进行一些数据统计工作
 */
public abstract class AbstractScheduler {
    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractScheduler.class);

    /**
     * 在执行任务前需要检测是否达到错误次数上限
     * 避免无意义的重试，加重系统负担
     */
    private ErrorRecordChecker errorLimit;

    /**
     * 对应 job级别的沟通者 也就是本对象是job级别的任务触发器
     */
    private AbstractContainerCommunicator containerCommunicator;

    /**
     * 该定时器针对的是哪个 job
     */
    private Long jobId;

    public Long getJobId() {
        return jobId;
    }

    /**
     * 定时器在初始化时 需要传入沟通者对象
     * @param containerCommunicator
     */
    public AbstractScheduler(AbstractContainerCommunicator containerCommunicator) {
        this.containerCommunicator = containerCommunicator;
    }

    /**
     * 核心的定时方法在这里 子类通过实现相关钩子 进行自行拓展
     * @param configurations  一组TG的配置项 他们的某些配置应当相同 比如 报告间隔/sleep间隔 他们的jobId也应该一致
     */
    public void schedule(List<Configuration> configurations) {
        Validate.notNull(configurations,
                "scheduler配置不能为空");
        int jobReportIntervalInMillSec = configurations.get(0).getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 30000);
        int jobSleepIntervalInMillSec = configurations.get(0).getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_SLEEPINTERVAL, 10000);

        this.jobId = configurations.get(0).getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);

        // 每个组件都有一个 通过configuration进行初始化的构造函数
        errorLimit = new ErrorRecordChecker(configurations.get(0));

        this.containerCommunicator.registerCommunication(configurations);

        // 每个配置项对应一个 TG 然后根据configuration.content的数量来判断有多少个task
        int totalTasks = calculateTaskCount(configurations);
        // 开始执行所有的TG
        startAllTaskGroup(configurations);

        // 上一次采集的数据 在下面的循环中 每一轮都要获取最新的数据
        Communication lastJobContainerCommunication = new Communication();

        long lastReportTimeStamp = System.currentTimeMillis();
        try {
            // 这里不断循环处理
            while (true) {
                /**
                 * step 1: collect job stat
                 * step 2: getReport info, then report it
                 * step 3: errorLimit do check
                 * step 4: dealSucceedStat();
                 * step 5: dealKillingStat();
                 * step 6: dealFailedStat();
                 * step 7: refresh last job stat, and then sleep for next while
                 *
                 * above steps, some ones should report info to DS
                 *
                 */
                // 每当执行任务时 会采集各种信息并存储到Communication中 这里更新 Communication内的数据
                Communication nowJobContainerCommunication = this.containerCommunicator.collect();
                nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
                LOG.debug(nowJobContainerCommunication.toString());

                //汇报周期
                long now = System.currentTimeMillis();
                // 避免频繁的报告数据 所以有一个时间间隔
                if (now - lastReportTimeStamp > jobReportIntervalInMillSec) {
                    Communication reportCommunication = CommunicationTool
                            .getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);

                    // 只有当超过 report的周期 才会更新lastReport对象 避免频繁报告
                    this.containerCommunicator.report(reportCommunication);
                    lastReportTimeStamp = now;
                    lastJobContainerCommunication = nowJobContainerCommunication;
                }

                // 检测此时错误记录是否超过限定值 超过的话抛出异常 终止整个处理流程
                errorLimit.checkRecordLimit(nowJobContainerCommunication);

                // 代表所有任务都已经完成
                if (nowJobContainerCommunication.getState() == State.SUCCEEDED) {
                    LOG.info("Scheduler accomplished all tasks.");
                    break;
                }

                // 检测job是否已经被关闭
                if (isJobKilling(this.getJobId())) {
                    dealKillingStat(this.containerCommunicator, totalTasks);
                } else if (nowJobContainerCommunication.getState() == State.FAILED) {
                    dealFailedStat(this.containerCommunicator, nowJobContainerCommunication.getThrowable());
                }

                Thread.sleep(jobSleepIntervalInMillSec);
            }
        } catch (InterruptedException e) {
            // 以 failed 状态退出
            LOG.error("捕获到InterruptedException异常!", e);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }

    }

    protected abstract void startAllTaskGroup(List<Configuration> configurations);

    // 感觉这个stat不像是统计的意思啊 子类实现是直接抛出异常

    /**
     * 当检测到job执行失败时 进行一些统计工作
     * @param frameworkCollector
     * @param throwable
     */
    protected abstract void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable);

    /**
     * 当检测到job被强制关闭时 进行一些统计工作
     * @param frameworkCollector
     * @param totalTasks
     */
    protected abstract void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks);

    /**
     * TG配置项 根据content.size 判断存在多少task
     * @param configurations
     * @return
     */
    private int calculateTaskCount(List<Configuration> configurations) {
        int totalTasks = 0;
        for (Configuration taskGroupConfiguration : configurations) {
            totalTasks += taskGroupConfiguration.getListConfiguration(
                    CoreConstant.DATAX_JOB_CONTENT).size();
        }
        return totalTasks;
    }

//    private boolean isJobKilling(Long jobId) {
//        Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
//        return jobInfo.getData() == State.KILLING.value();
//    }

    protected  abstract  boolean isJobKilling(Long jobId);
}
