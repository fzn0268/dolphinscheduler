package org.apache.dolphinscheduler.server.worker.runner;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContextCacheManager;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.server.worker.WorkerServer;
import org.apache.dolphinscheduler.server.worker.rpc.WorkerMessageSender;
import org.apache.dolphinscheduler.service.log.LogClient;
import org.apache.dolphinscheduler.service.utils.LoggerUtils;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.List;

import lombok.NonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class YarnAppIdCollectThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WorkerServer.class);

    private final LogClient logClient;

    private final WorkerManagerThread workerManager;

    private final WorkerMessageSender workerMessageSender;

    public YarnAppIdCollectThread(@NonNull LogClient logClient,
                                  @NonNull WorkerManagerThread workerManager,
                                  @NonNull WorkerMessageSender workerMessageSender) {
        this.logClient = logClient;
        this.workerManager = workerManager;
        this.workerMessageSender = workerMessageSender;
    }

    public void start() {
        logger.info("Yarn application ID collect thread starting");
        Thread thread = new Thread(this, this.getClass().getName());
        thread.setDaemon(true);
        thread.start();
        logger.info("Yarn application ID collect thread started");
    }

    @Override
    public void run() {
        Thread.currentThread().setName("YARN-App-ID-Collect-Thread");
        while (!ServerLifeCycleManager.isStopped()) {
            try {
                if (!ServerLifeCycleManager.isRunning()) {
                    Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                }
                Collection<TaskExecutionContext> taskRequests =
                        TaskExecutionContextCacheManager.getAllTaskRequestList();
                if (CollectionUtils.isEmpty(taskRequests)) {
                    continue;
                }
                logger.info("Worker begin to collect application ID of all YARN task, task size: {}",
                        taskRequests.size());
                for (TaskExecutionContext taskRequest : taskRequests) {
                    if (taskRequest.getCurrentExecutionStatus().isRunning() &&
                            ("SPARK".equals(taskRequest.getTaskType()) ||
                                    "FLINK".equals(taskRequest.getTaskType()) ||
                                    "MR".equals(taskRequest.getTaskType()))) {
                        final String strAppIds = taskRequest.getAppIds();
                        final Host host = Host.of(taskRequest.getHost());
                        final String logPath = taskRequest.getLogPath();
                        if (logPath != null && !logPath.isEmpty()) {
                            try {
                                LoggerUtils.setWorkflowAndTaskInstanceIDMDC(taskRequest.getProcessInstanceId(),
                                        taskRequest.getTaskInstanceId());
                                // collect application from log
                                logger.info("Collect application ID of task ({}){} from log path {}",
                                        taskRequest.getTaskInstanceId(), taskRequest.getTaskName(), logPath);
                                final List<String> appIds = logClient.getAppIds(host.getIp(), host.getPort(), logPath);
                                if (appIds != null) {
                                    final String strAppIdsNew = String.join(TaskConstants.COMMA, appIds);
                                    if (!strAppIdsNew.isEmpty() &&
                                            (strAppIds == null || !strAppIds.equals(strAppIdsNew))) {
                                        taskRequest.setAppIds(strAppIdsNew);
                                        final WorkerTaskExecuteRunnable taskExecuteThread =
                                                workerManager.getTaskExecuteThread(taskRequest.getTaskInstanceId());
                                        if (taskExecuteThread != null) {
                                            workerMessageSender.sendMessageWithRetry(taskRequest,
                                                    taskExecuteThread.masterAddress, CommandType.TASK_UPDATE);
                                        } else {
                                            logger.info("Execute thread of task ({}){} does not exist",
                                                    taskRequest.getTaskInstanceId(), taskRequest.getTaskName());
                                        }
                                    }
                                } else {
                                    taskRequest.setAppIds(null);
                                    logger.info("Got null app ID from log file of task ({}){}",
                                            taskRequest.getTaskInstanceId(), taskRequest.getTaskName());
                                }
                            } catch (Exception e) {
                                logger.error(
                                        "Collect yarn application ID error, host: {}, logPath: {}, executePath: {}, tenantCode: {}",
                                        host, logPath,
                                        taskRequest.getExecutePath(), taskRequest.getTenantCode(), e);
                            } finally {
                                LoggerUtils.removeWorkflowAndTaskInstanceIdMDC();
                            }
                        } else {
                            logger.info("Log file path of task ({}){} is null or empty",
                                    taskRequest.getTaskInstanceId(), taskRequest.getTaskName());
                        }
                    }
                }
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS);
            } catch (Exception e) {
                logger.error("An unexpected interrupt is happened, "
                        + "the exception will be ignored and this thread will continue to run", e);
            }
        }

    }
}
