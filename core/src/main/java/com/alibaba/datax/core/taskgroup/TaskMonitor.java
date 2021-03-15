package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by liqiang on 15/7/23.
 * 任务监控器 jvm级别单例
 */
public class TaskMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(TaskMonitor.class);
    private static final TaskMonitor instance = new TaskMonitor();
    private static long EXPIRED_TIME = 172800 * 1000;

    private ConcurrentHashMap<Integer, TaskCommunication> tasks = new ConcurrentHashMap<Integer, TaskCommunication>();

    private TaskMonitor() {
    }

    public static TaskMonitor getInstance() {
        return instance;
    }

    /**
     * 任务管理器负责管理当前jvm下运行的所有任务
     * @param communication 该任务对应的协调对象 描述任务状态等信息
     */
    public void registerTask(Integer taskid, Communication communication) {
        //如果task已经finish，直接返回
        if (communication.isFinished()) {
            return;
        }
        tasks.putIfAbsent(taskid, new TaskCommunication(taskid, communication));
    }

    /**
     * 当某个任务不再需要被监控时，从容器中移除
     * @param taskid
     */
    public void removeTask(Integer taskid) {
        tasks.remove(taskid);
    }

    /**
     * 报告某个task此时的信息 实际上就是更新 TaskCommunication
     * @param taskid
     * @param communication
     */
    public void report(Integer taskid, Communication communication) {
        //如果task已经finish，直接返回
        if (communication.isFinished()) {
            return;
        }
        if (!tasks.containsKey(taskid)) {
            LOG.warn("unexpected: taskid({}) missed.", taskid);
            tasks.putIfAbsent(taskid, new TaskCommunication(taskid, communication));
        } else {
            tasks.get(taskid).report(communication);
        }
    }

    /**
     * 获取某个任务此时的协调者信息
     * @param taskid
     * @return
     */
    public TaskCommunication getTaskCommunication(Integer taskid) {
        return tasks.get(taskid);
    }


    /**
     * 将任务相关的协调对象与任务id整合在一起
     */
    public static class TaskCommunication {
        private Integer taskid;
        //在初始化时对应此时已经执行成功/失败的记录数
        private long lastAllReadRecords = -1;
        //对应最近一次变更统计记录的时间戳
        private long lastUpdateComunicationTS;
        private long ttl;

        private TaskCommunication(Integer taskid, Communication communication) {
            this.taskid = taskid;
            lastAllReadRecords = CommunicationTool.getTotalReadRecords(communication);
            ttl = System.currentTimeMillis();
            lastUpdateComunicationTS = ttl;
        }

        /**
         * 此时某个task相关的协调对象已经发生了变化 需要更新内部信息
         * @param communication
         */
        public void report(Communication communication) {

            ttl = System.currentTimeMillis();
            // 确保数量发生了变化 才进行更新
            if (CommunicationTool.getTotalReadRecords(communication) > lastAllReadRecords) {
                lastAllReadRecords = CommunicationTool.getTotalReadRecords(communication);
                lastUpdateComunicationTS = ttl;
                // 数量没有发生变化  但是距离上次报告已经过了一个超时时间阈值  此时认为任务执行失败了
            } else if (isExpired(lastUpdateComunicationTS)) {
                communication.setState(State.FAILED);
                communication.setTimestamp(ttl);
                // 这里认为任务hung住了
                communication.setThrowable(DataXException.asDataXException(CommonErrorCode.TASK_HUNG_EXPIRED,
                        String.format("task(%s) hung expired [allReadRecord(%s), elased(%s)]", taskid, lastAllReadRecords, (ttl - lastUpdateComunicationTS))));
            }
            // 其余情况 数量没有发生变化 且时间差没有超过超时时间阈值 静默处理
        }

        private boolean isExpired(long lastUpdateComunicationTS) {
            return System.currentTimeMillis() - lastUpdateComunicationTS > EXPIRED_TIME;
        }

        public Integer getTaskid() {
            return taskid;
        }

        public long getLastAllReadRecords() {
            return lastAllReadRecords;
        }

        public long getLastUpdateComunicationTS() {
            return lastUpdateComunicationTS;
        }

        public long getTtl() {
            return ttl;
        }
    }
}
