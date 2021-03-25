package com.alibaba.datax.core.container.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * 该工具类规定了job是如何被分散到多个TG中的
 * TODO 还不清楚分配的其他几个参数是如何获得的
 */
public final class JobAssignUtil {
    private JobAssignUtil() {
    }

    /**
     * 公平的分配 task 到对应的 taskGroup 中。
     * 公平体现在：会考虑 task 中对资源负载作的 load 标识进行更均衡的作业分配操作。
     * @param configuration 这里对应一组task相关的configuration
     * @param channelNumber 用于计算会负载到多少个TG中
     */
    public static List<Configuration> assignFairly(Configuration configuration, int channelNumber, int channelsPerTaskGroup) {
        Validate.isTrue(configuration != null, "框架获得的 Job 不能为 null.");

        // 获取一组有关job.content的配置项信息
        List<Configuration> contentConfig = configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        Validate.isTrue(contentConfig.size() > 0, "框架获得的切分后的 Job 无内容.");

        Validate.isTrue(channelNumber > 0 && channelsPerTaskGroup > 0,
                "每个channel的平均task数[averTaskPerChannel]，channel数目[channelNumber]，每个taskGroup的平均channel数[channelsPerTaskGroup]都应该为正数");

        // 判断会使用到多少个TG
        int taskGroupNumber = (int) Math.ceil(1.0 * channelNumber / channelsPerTaskGroup);

        // 尝试性检测第一个配置是否设置了 read/write mark 如果没有 为每个conf设置fake值
        Configuration aTaskConfig = contentConfig.get(0);

        // 描述当前资源读写的负载情况 可以作为task分配的影响因素
        String readerResourceMark = aTaskConfig.getString(CoreConstant.JOB_READER_PARAMETER + "." +
                CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
        String writerResourceMark = aTaskConfig.getString(CoreConstant.JOB_WRITER_PARAMETER + "." +
                CommonConstant.LOAD_BALANCE_RESOURCE_MARK);

        boolean hasLoadBalanceResourceMark = StringUtils.isNotBlank(readerResourceMark) ||
                StringUtils.isNotBlank(writerResourceMark);

        if (!hasLoadBalanceResourceMark) {
            // fake 一个固定的 key 作为资源标识（在 reader 或者 writer 上均可，此处选择在 reader 上进行 fake）
            for (Configuration conf : contentConfig) {
                conf.set(CoreConstant.JOB_READER_PARAMETER + "." +
                        CommonConstant.LOAD_BALANCE_RESOURCE_MARK, "aFakeResourceMarkForLoadBalance");
            }
            // 是为了避免某些插件没有设置 资源标识 而进行了一次随机打乱操作
            Collections.shuffle(contentConfig, new Random(System.currentTimeMillis()));
        }

        LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap = parseAndGetResourceMarkAndTaskIdMap(contentConfig);
        List<Configuration> taskGroupConfig = doAssign(resourceMarkAndTaskIdMap, configuration, taskGroupNumber);

        // 调整 每个 taskGroup 对应的 Channel 个数（属于优化范畴）TODO 之后在看 先理解设计理念和使用流程
        adjustChannelNumPerTaskGroup(taskGroupConfig, channelNumber);
        return taskGroupConfig;
    }

    private static void adjustChannelNumPerTaskGroup(List<Configuration> taskGroupConfig, int channelNumber) {
        int taskGroupNumber = taskGroupConfig.size();
        int avgChannelsPerTaskGroup = channelNumber / taskGroupNumber;
        int remainderChannelCount = channelNumber % taskGroupNumber;
        // 表示有 remainderChannelCount 个 taskGroup,其对应 Channel 个数应该为：avgChannelsPerTaskGroup + 1；
        // （taskGroupNumber - remainderChannelCount）个 taskGroup,其对应 Channel 个数应该为：avgChannelsPerTaskGroup

        int i = 0;
        for (; i < remainderChannelCount; i++) {
            taskGroupConfig.get(i).set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, avgChannelsPerTaskGroup + 1);
        }

        for (int j = 0; j < taskGroupNumber - remainderChannelCount; j++) {
            taskGroupConfig.get(i + j).set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, avgChannelsPerTaskGroup);
        }
    }

    /**
     * 根据task 配置，获取到：
     * 资源名称 --> taskId(List) 的 map 映射关系
     */
    private static LinkedHashMap<String, List<Integer>> parseAndGetResourceMarkAndTaskIdMap(List<Configuration> contentConfig) {
        // key: resourceMark, value: taskId
        LinkedHashMap<String, List<Integer>> readerResourceMarkAndTaskIdMap = new LinkedHashMap<String, List<Integer>>();
        LinkedHashMap<String, List<Integer>> writerResourceMarkAndTaskIdMap = new LinkedHashMap<String, List<Integer>>();

        for (Configuration aTaskConfig : contentConfig) {
            // 通过从单个任务的配置项中 获取任务id
            int taskId = aTaskConfig.getInt(CoreConstant.TASK_ID);
            // 把 readerResourceMark 加到 readerResourceMarkAndTaskIdMap 中
            String readerResourceMark = aTaskConfig.getString(CoreConstant.JOB_READER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
            if (readerResourceMarkAndTaskIdMap.get(readerResourceMark) == null) {
                readerResourceMarkAndTaskIdMap.put(readerResourceMark, new LinkedList<Integer>());
            }
            readerResourceMarkAndTaskIdMap.get(readerResourceMark).add(taskId);

            // 把 writerResourceMark 加到 writerResourceMarkAndTaskIdMap 中
            String writerResourceMark = aTaskConfig.getString(CoreConstant.JOB_WRITER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
            if (writerResourceMarkAndTaskIdMap.get(writerResourceMark) == null) {
                writerResourceMarkAndTaskIdMap.put(writerResourceMark, new LinkedList<Integer>());
            }
            writerResourceMarkAndTaskIdMap.get(writerResourceMark).add(taskId);
        }

        if (readerResourceMarkAndTaskIdMap.size() >= writerResourceMarkAndTaskIdMap.size()) {
            // 采用 reader 对资源做的标记进行 shuffle
            return readerResourceMarkAndTaskIdMap;
        } else {
            // 采用 writer 对资源做的标记进行 shuffle
            return writerResourceMarkAndTaskIdMap;
        }
    }


    /**
     * /**
     * 需要实现的效果通过例子来说是：
     * <pre>
     * a 库上有表：0, 1, 2
     * b 库上有表：3, 4
     * c 库上有表：5, 6, 7
     *
     * 如果有 4个 taskGroup
     * 则 assign 后的结果为：
     * taskGroup-0: 0,  4,
     * taskGroup-1: 3,  6,
     * taskGroup-2: 5,  2,
     * taskGroup-3: 1,  7
     * </pre>
     *
     * 将task分配到TG上
     * @param resourceMarkAndTaskIdMap key对应资源名，value对应 taskId list
     * @param jobConfiguration job级别配置项
     * @param taskGroupNumber 本次会涉及到多少个TG
     * @return 返回TG级别的configuration
     */
    private static List<Configuration> doAssign(LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap, Configuration jobConfiguration, int taskGroupNumber) {
        List<Configuration> contentConfig = jobConfiguration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

        Configuration taskGroupTemplate = jobConfiguration.clone();
        taskGroupTemplate.remove(CoreConstant.DATAX_JOB_CONTENT);

        List<Configuration> result = new LinkedList<Configuration>();

        List<List<Configuration>> taskGroupConfigList = new ArrayList<List<Configuration>>(taskGroupNumber);
        // 为每个TG下的task初始化list
        for (int i = 0; i < taskGroupNumber; i++) {
            taskGroupConfigList.add(new LinkedList<Configuration>());
        }

        int mapValueMaxLength = -1;

        List<String> resourceMarks = new ArrayList<String>();
        for (Map.Entry<String, List<Integer>> entry : resourceMarkAndTaskIdMap.entrySet()) {
            resourceMarks.add(entry.getKey());
            // 记录资源对应的最多的task
            if (entry.getValue().size() > mapValueMaxLength) {
                mapValueMaxLength = entry.getValue().size();
            }
        }

        int taskGroupIndex = 0;
        for (int i = 0; i < mapValueMaxLength; i++) {
            for (String resourceMark : resourceMarks) {
                if (resourceMarkAndTaskIdMap.get(resourceMark).size() > 0) {
                    // 每当获取到任务后会从list中移除 所以还是相当于遍历所有task
                    int taskId = resourceMarkAndTaskIdMap.get(resourceMark).get(0);
                    // 可以看到是以轮询的方式进行插入的
                    taskGroupConfigList.get(taskGroupIndex % taskGroupNumber).add(contentConfig.get(taskId));
                    taskGroupIndex++;

                    resourceMarkAndTaskIdMap.get(resourceMark).remove(0);
                }
            }
        }

        Configuration tempTaskGroupConfig;
        for (int i = 0; i < taskGroupNumber; i++) {
            tempTaskGroupConfig = taskGroupTemplate.clone();
            // 这里已经完成了重分配
            tempTaskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupConfigList.get(i));
            tempTaskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);

            result.add(tempTaskGroupConfig);
        }

        return result;
    }

}
