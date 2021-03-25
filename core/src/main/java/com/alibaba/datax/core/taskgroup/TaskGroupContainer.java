package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.taskgroup.StandaloneTGContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.task.AbstractTaskPluginCollector;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.alibaba.datax.core.transport.transformer.TransformerExecution;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.TransformerUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * TG容器 就可以理解为容纳所有相关任务的 并管理他们的生命周期
 */
public class TaskGroupContainer extends AbstractContainer {
    private static final Logger LOG = LoggerFactory
            .getLogger(TaskGroupContainer.class);

    /**
     * 当前taskGroup所属jobId
     */
    private long jobId;

    /**
     * 当前taskGroupId
     */
    private int taskGroupId;

    /**
     * 使用的channel类
     */
    private String channelClazz;

    /**
     * task收集器使用的类
     */
    private String taskCollectorClass;

    private TaskMonitor taskMonitor = TaskMonitor.getInstance();

    public TaskGroupContainer(Configuration configuration) {
        super(configuration);

        // 初始化沟通对象 以及内部的collector/reporter对象
        initCommunicator(configuration);

        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        this.taskGroupId = this.configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);

        this.channelClazz = this.configuration.getString(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);
        this.taskCollectorClass = this.configuration.getString(
                CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
    }

    private void initCommunicator(Configuration configuration) {
        super.setContainerCommunicator(new StandaloneTGContainerCommunicator(configuration));

    }

    public long getJobId() {
        return jobId;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    /**
     * 当通过 scheduler执行job任务时 会根据一组TG configuration 生成 对应的一组TGContainer对象 并启动各个TG
     */
    @Override
    public void start() {
        try {
            // 代表每次循环的间隔时间
            int sleepIntervalInMillSec = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL, 100);
            /**
             * 状态汇报时间间隔，稍长，避免大量汇报  报告就是更新TG沟通者对象内的信息
             */
            long reportIntervalInMillSec = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL,
                    10000);

            // 获取channel数目
            int channelNumber = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);

            // 当任务失败时的重试次数
            int taskMaxRetryTimes = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXRETRYTIMES, 1);

            // 每次重试的时间间隔
            long taskRetryIntervalInMsec = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_RETRYINTERVALINMSEC, 10000);

            // 最大等待时间
            long taskMaxWaitInMsec = this.configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXWAITINMSEC, 60000);

            // 对应该TG下有多少个任务 这里是每个task对应的configuration
            List<Configuration> taskConfigs = this.configuration
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

            if(LOG.isDebugEnabled()) {
                LOG.debug("taskGroup[{}]'s task configs[{}]", this.taskGroupId,
                        JSON.toJSONString(taskConfigs));
            }
            
            int taskCountInThisTaskGroup = taskConfigs.size();
            LOG.info(String.format(
                    "taskGroupId=[%d] start [%d] channels for [%d] tasks.",
                    this.taskGroupId, channelNumber, taskCountInThisTaskGroup));

            // 为TG沟通对象 插入task级别的沟通对象
            this.containerCommunicator.registerCommunication(taskConfigs);

            // 将task级别的configuration 按照taskId分组
            Map<Integer, Configuration> taskConfigMap = buildTaskConfigMap(taskConfigs);
            // 待运行任务 初始状态就是全部task
            List<Configuration> taskQueue = buildRemainTasks(taskConfigs);
            // 用于存储上次失败的任务
            Map<Integer, TaskExecutor> taskFailedExecutorMap = new HashMap<Integer, TaskExecutor>();
            // 此时正在运行的任务 初始状态为空
            List<TaskExecutor> runTasks = new ArrayList<TaskExecutor>(channelNumber);
            // 存储每个任务开始的时间
            Map<Integer, Long> taskStartTimeMap = new HashMap<Integer, Long>();

            long lastReportTimeStamp = 0;
            // 记录上次运行所有任务之前TG内的统计信息
            Communication lastTaskGroupContainerCommunication = new Communication();

            // 下面的循环就是不断执行所有任务
            while (true) {
            	//1.判断task状态
            	boolean failedOrKilled = false;
            	// 获取每个task对象的统计信息
            	Map<Integer, Communication> communicationMap = containerCommunicator.getCommunicationMap();

            	// 这里在检测所有已经处于finished的任务 并判断是否重试 还是成功执行
            	for(Map.Entry<Integer, Communication> entry : communicationMap.entrySet()){
            		Integer taskId = entry.getKey();
            		Communication taskCommunication = entry.getValue();
            		// 检测某个任务是否已经完成了 如果未完成则跳过  默认情况下status 是 running
                    if(!taskCommunication.isFinished()){
                        continue;
                    }
                    // 首次调用时 task还没有生成对应的执行器 所以应该返回null
                    TaskExecutor taskExecutor = removeTask(runTasks, taskId);

                    // 在监控器中也要移除对应的对象
                    taskMonitor.removeTask(taskId);

                    // 如果此时任务已经失败 尚未启动时是 Running
            		if(taskCommunication.getState() == State.FAILED){
            		    // 将失败的任务插入到map中
                        taskFailedExecutorMap.put(taskId, taskExecutor);
                        // 检测任务本身是否支持重试 且还未超过最大重试次数
            			if(taskExecutor.supportFailOver() && taskExecutor.getAttemptCount() < taskMaxRetryTimes){
            			    // 将任务执行器关闭后 重新加入到任务队列 并会在下面重新生成对应的执行器
                            taskExecutor.shutdown(); //关闭老的executor
                            containerCommunicator.resetCommunication(taskId); //将task的状态重置
            				Configuration taskConfig = taskConfigMap.get(taskId);
            				taskQueue.add(taskConfig); //重新加入任务列表
            			}else{
            				failedOrKilled = true;
                			break;
            			}
            			// 发现任务被强制关闭 或者任务失败且不满足重试条件时 会退出循环
            		}else if(taskCommunication.getState() == State.KILLED){
            			failedOrKilled = true;
            			break;
            			// 发现任务已经完成 会遍历下一个任务
            		}else if(taskCommunication.getState() == State.SUCCEEDED){
                        Long taskStartTime = taskStartTimeMap.get(taskId);
                        if(taskStartTime != null){
                            // 计算整个任务从创建到现在的耗时时长
                            Long usedTime = System.currentTimeMillis() - taskStartTime;
                            LOG.info("taskGroup[{}] taskId[{}] is successed, used[{}]ms",
                                    this.taskGroupId, taskId, usedTime);
                            // 这个是性能记录对象  最终会存储在 PerfTrace对象  有关数据统计的逻辑就先不看了 不重要
                            PerfRecord.addPerfRecord(taskGroupId, taskId, PerfRecord.PHASE.TASK_TOTAL,taskStartTime, usedTime * 1000L * 1000L);
                            // 因为本任务已经完成了 所以相关配置就没有必要维护了
                            taskStartTimeMap.remove(taskId);
                            taskConfigMap.remove(taskId);
                        }
                    }
            	}

            	// 在上面的检测中 发现有某个任务已经失败了 那么认为整个TG执行失败
                if (failedOrKilled) {
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    // 失败则抛出异常
                    throw DataXException.asDataXException(
                            FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, lastTaskGroupContainerCommunication.getThrowable());
                }

                // 这里在遍历所有待执行的任务
                Iterator<Configuration> iterator = taskQueue.iterator();
                // 可以看到一次性只能执行channel个task 之后的任务需要先搁置  每个要执行的任务会设置到 runTasks 中
                // 每个taskExecutor在创建后是独享channel的 他的线程安全主要是用于 writer/reader的并发操作
                while(iterator.hasNext() && runTasks.size() < channelNumber){
                    Configuration taskConfig = iterator.next();
                    Integer taskId = taskConfig.getInt(CoreConstant.TASK_ID);
                    // 当任务开始执行时 默认已经尝试1次了
                    int attemptCount = 1;
                    TaskExecutor lastExecutor = taskFailedExecutorMap.get(taskId);
                    // 如果当前任务存在于 failed容器中 代表需要进行重试 之前一定已经设置了 taskExecutor
                    // 这里主要是通过检查 taskExecutor 来判断该任务是否还满足重试条件 当不满足时 抛出异常
                    if(lastExecutor!=null){
                        attemptCount = lastExecutor.getAttemptCount() + 1;
                        long now = System.currentTimeMillis();
                        long failedTime = lastExecutor.getTimeStamp();
                        if(now - failedTime < taskRetryIntervalInMsec){  //未到等待时间，继续留在队列
                            continue;
                        }
                        // 长时间未结束 直接抛出异常
                        if(!lastExecutor.isShutdown()){ //上次失败的task仍未结束
                            if(now - failedTime > taskMaxWaitInMsec){
                                markCommunicationFailed(taskId);
                                reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
                                throw DataXException.asDataXException(CommonErrorCode.WAIT_TIME_EXCEED, "task failover等待超时");
                            }else{
                                // 调用关闭任务后 等待下一轮检测
                                lastExecutor.shutdown(); //再次尝试关闭
                                continue;
                            }
                            // 正常情况 应该是走这个分支
                        }else{
                            LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] has already shutdown",
                                    this.taskGroupId, taskId, lastExecutor.getAttemptCount());
                        }
                    }
                    // 如果任务的重试次数超过1 生成一个副本config 推测是避免task在执行过程中修改config 而当重试时需要一个全新的config
                    Configuration taskConfigForRun = taskMaxRetryTimes > 1 ? taskConfig.clone() : taskConfig;
                	TaskExecutor taskExecutor = new TaskExecutor(taskConfigForRun, attemptCount);
                    taskStartTimeMap.put(taskId, System.currentTimeMillis());

                    // 开始执行任务 此时就会用到相关的transformer runner
                	taskExecutor.doStart();

                	// 启动的任务将从待执行队列中移除
                    iterator.remove();
                    runTasks.add(taskExecutor);

                    // 注册到任务监视器中
                    taskMonitor.registerTask(taskId, this.containerCommunicator.getCommunication(taskId));

                    // 这里已经重启了任务 所以可以从队列中移除
                    taskFailedExecutorMap.remove(taskId);
                    LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] is started",
                            this.taskGroupId, taskId, attemptCount);
                }

                // taskQueue为空 代表所有任务都开始执行
                // runTasks 为空 或者内部任务都标记成 已完成
                // 沟通者状态为success 代表成功完成
                if (taskQueue.isEmpty() && isAllTaskDone(runTasks) && containerCommunicator.collectState() == State.SUCCEEDED) {
                	// 成功的情况下，也需要汇报一次。否则在任务结束非常快的情况下，采集的信息将会不准确
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    LOG.info("taskGroup[{}] completed it's tasks.", this.taskGroupId);
                    break;
                }

                // 这个就是每隔一段时间进行汇报
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    lastReportTimeStamp = now;

                    //taskMonitor对于正在运行的task，每reportIntervalInMillSec进行检查
                    for(TaskExecutor taskExecutor:runTasks){
                        taskMonitor.report(taskExecutor.getTaskId(),this.containerCommunicator.getCommunication(taskExecutor.getTaskId()));
                    }

                }

                // 每隔这么长的时间 进行一次探测
                Thread.sleep(sleepIntervalInMillSec);
            }

            //6.最后还要汇报一次
            reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);


        } catch (Throwable e) {
            Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();

            if (nowTaskGroupContainerCommunication.getThrowable() == null) {
                nowTaskGroupContainerCommunication.setThrowable(e);
            }
            nowTaskGroupContainerCommunication.setState(State.FAILED);
            this.containerCommunicator.report(nowTaskGroupContainerCommunication);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }finally {
            if(!PerfTrace.getInstance().isJob()){
                //最后打印cpu的平均消耗，GC的统计
                VMInfo vmInfo = VMInfo.getVmInfo();
                if (vmInfo != null) {
                    vmInfo.getDelta(false);
                    LOG.info(vmInfo.totalString());
                }

                LOG.info(PerfTrace.getInstance().summarizeNoException());
            }
        }
    }

    /**
     * 将每个task的配置项按照taskId分组
     * @param configurations
     * @return
     */
    private Map<Integer, Configuration> buildTaskConfigMap(List<Configuration> configurations){
    	Map<Integer, Configuration> map = new HashMap<Integer, Configuration>();
    	for(Configuration taskConfig : configurations){
        	int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
        	map.put(taskId, taskConfig);
    	}
    	return map;
    }

    private List<Configuration> buildRemainTasks(List<Configuration> configurations){
    	List<Configuration> remainTasks = new LinkedList<Configuration>();
    	for(Configuration taskConfig : configurations){
    		remainTasks.add(taskConfig);
    	}
    	return remainTasks;
    }
    
    private TaskExecutor removeTask(List<TaskExecutor> taskList, int taskId){
    	Iterator<TaskExecutor> iterator = taskList.iterator();
    	while(iterator.hasNext()){
    		TaskExecutor taskExecutor = iterator.next();
    		if(taskExecutor.getTaskId() == taskId){
    			iterator.remove();
    			return taskExecutor;
    		}
    	}
    	return null;
    }
    
    private boolean isAllTaskDone(List<TaskExecutor> taskList){
    	for(TaskExecutor taskExecutor : taskList){
    		if(!taskExecutor.isTaskFinished()){
    			return false;
    		}
    	}
    	return true;
    }

    /**
     * 根据上次沟通者信息 以及本次沟通者信息 计算一些属性 并填充到沟通者对象中
     * @param lastTaskGroupContainerCommunication
     * @param taskCount
     * @return
     */
    private Communication reportTaskGroupCommunication(Communication lastTaskGroupContainerCommunication, int taskCount){
        Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
        nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());
        Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication,
                lastTaskGroupContainerCommunication, taskCount);
        this.containerCommunicator.report(reportCommunication);
        return reportCommunication;
    }

    private void markCommunicationFailed(Integer taskId){
        Communication communication = containerCommunicator.getCommunication(taskId);
        communication.setState(State.FAILED);
    }

    /**
     * 该对象包含了执行任务的逻辑模板
     */
    class TaskExecutor {
        private Configuration taskConfig;

        private int taskId;

        private int attemptCount;

        private Channel channel;

        private Thread readerThread;

        private Thread writerThread;
        
        private ReaderRunner readerRunner;
        
        private WriterRunner writerRunner;

        /**
         * 该处的taskCommunication在多处用到：
         * 1. channel
         * 2. readerRunner和writerRunner
         * 3. reader和writer的taskPluginCollector
         */
        private Communication taskCommunication;

        /**
         * 通过某个task的相关配置初始化 执行者对象
         * @param taskConf
         * @param attemptCount 本次任务此时第几次尝试 默认为1 如果任务开始重试 次数会变化
         */
        public TaskExecutor(Configuration taskConf, int attemptCount) {
            // 获取该taskExecutor的配置
            this.taskConfig = taskConf;
            Validate.isTrue(null != this.taskConfig.getConfiguration(CoreConstant.JOB_READER)
                            && null != this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER),
                    "[reader|writer]的插件参数不能为空!");

            // 得到taskId
            this.taskId = this.taskConfig.getInt(CoreConstant.TASK_ID);
            this.attemptCount = attemptCount;

            // 从TG级别的沟通者对象中获取 task相关的沟通对象信息 reader和writer可能会使用该对象记录一些信息
            this.taskCommunication = containerCommunicator
                    .getCommunication(taskId);
            Validate.notNull(this.taskCommunication,
                    String.format("taskId[%d]的Communication没有注册过", taskId));

            // 数据传输过程中使用的通道 默认是基于内存的通道 也就是读取过来的数据会暂存在内存中 底层使用一个阻塞队列实现
            this.channel = ClassUtil.instantiate(channelClazz,
                    Channel.class, configuration);
            // 将任务级别的沟通对象设置进去
            this.channel.setCommunication(this.taskCommunication);

            // 根据配置项信息 生成数据转换对象  这些转换对象负责加工读取到的record 可以先不看
            List<TransformerExecution> transformerInfoExecs = TransformerUtil.buildTransformerInfo(taskConfig);

            // 每个TG对象对应一条线程进行循环 而每个需要处理的task  需要额外的线程来进行read/write 这样的线程分配也更加合理
            writerRunner = (WriterRunner) generateRunner(PluginType.WRITER);
            this.writerThread = new Thread(writerRunner,
                    String.format("%d-%d-%d-writer",
                            jobId, taskGroupId, this.taskId));
            // 获取之前初始化该插件的类加载器 并设置到线程中
            this.writerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.WRITER, this.taskConfig.getString(
                            CoreConstant.JOB_WRITER_NAME)));

            // 同上
            readerRunner = (ReaderRunner) generateRunner(PluginType.READER,transformerInfoExecs);
            this.readerThread = new Thread(readerRunner,
                    String.format("%d-%d-%d-reader",
                            jobId, taskGroupId, this.taskId));
            this.readerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.READER, this.taskConfig.getString(
                            CoreConstant.JOB_READER_NAME)));
        }

        /**
         * 开始执行搬运任务
         */
        public void doStart() {
            this.writerThread.start();

            // reader没有起来，writer不可能结束
            if (!this.writerThread.isAlive() || this.taskCommunication.getState() == State.FAILED) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }

            this.readerThread.start();

            // 这里reader可能很快结束
            if (!this.readerThread.isAlive() && this.taskCommunication.getState() == State.FAILED) {
                // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }

        }

        /**
         * 根据插件类型加载对应的 writer/reader对象
         * @param pluginType
         * @return
         */
        private AbstractRunner generateRunner(PluginType pluginType) {
            return generateRunner(pluginType, null);
        }

        /**
         * 根据插件类型 生成对应的runner对象
         * @param pluginType
         * @param transformerInfoExecs
         * @return
         */
        private AbstractRunner generateRunner(PluginType pluginType, List<TransformerExecution> transformerInfoExecs) {
            AbstractRunner newRunner = null;
            TaskPluginCollector pluginCollector;

            switch (pluginType) {
                // 根据插件类型 以及配置中记录的className 反射创建对象
                case READER:
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_READER_NAME));
                    newRunner.setJobConf(this.taskConfig.getConfiguration(
                            CoreConstant.JOB_READER_PARAMETER));

                    pluginCollector = ClassUtil.instantiate(
                            taskCollectorClass, AbstractTaskPluginCollector.class,
                            configuration, this.taskCommunication,
                            PluginType.READER);

                    // 读取过程相交写入过程 多了一个操作的空间 就是使用transformer进行数据转换 (以column为单位)
                    RecordSender recordSender;
                    if (transformerInfoExecs != null && transformerInfoExecs.size() > 0) {
                        recordSender = new BufferedRecordTransformerExchanger(taskGroupId, this.taskId, this.channel,this.taskCommunication ,pluginCollector, transformerInfoExecs);
                    } else {
                        recordSender = new BufferedRecordExchanger(this.channel, pluginCollector);
                    }

                    ((ReaderRunner) newRunner).setRecordSender(recordSender);

                    /**
                     * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                     */
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                case WRITER:
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_WRITER_NAME));
                    newRunner.setJobConf(this.taskConfig
                            .getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));

                    // 该对象负责收集插件中产生的失败信息
                    pluginCollector = ClassUtil.instantiate(
                            taskCollectorClass, AbstractTaskPluginCollector.class,
                            configuration, this.taskCommunication,
                            PluginType.WRITER);
                    ((WriterRunner) newRunner).setRecordReceiver(new BufferedRecordExchanger(
                            this.channel, pluginCollector));
                    /**
                     * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                     */
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                default:
                    throw DataXException.asDataXException(FrameworkErrorCode.ARGUMENT_ERROR, "Cant generateRunner for:" + pluginType);
            }

            newRunner.setTaskGroupId(taskGroupId);
            newRunner.setTaskId(this.taskId);
            newRunner.setRunnerCommunication(this.taskCommunication);

            return newRunner;
        }

        // 检查任务是否结束
        private boolean isTaskFinished() {
            // 如果reader 或 writer没有完成工作，那么直接返回工作没有完成
            if (readerThread.isAlive() || writerThread.isAlive()) {
                return false;
            }

            if(taskCommunication==null || !taskCommunication.isFinished()){
        		return false;
        	}

            return true;
        }
        
        private int getTaskId(){
        	return taskId;
        }

        private long getTimeStamp(){
            return taskCommunication.getTimestamp();
        }

        private int getAttemptCount(){
            return attemptCount;
        }
        
        private boolean supportFailOver(){
        	return writerRunner.supportFailOver();
        }

        /**
         * 当尝试终止该任务时 会触发内部2个runner的shutdown
         */
        private void shutdown(){
            writerRunner.shutdown();
            readerRunner.shutdown();
            // 如果线程还处于启动状态强制中断
            if(writerThread.isAlive()){
                writerThread.interrupt();
            }
            if(readerThread.isAlive()){
                readerThread.interrupt();
            }
        }

        private boolean isShutdown(){
            return !readerThread.isAlive() && !writerThread.isAlive();
        }
    }
}
