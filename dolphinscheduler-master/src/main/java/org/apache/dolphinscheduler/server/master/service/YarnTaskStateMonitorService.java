package org.apache.dolphinscheduler.server.master.service;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.StateEventType;
import org.apache.dolphinscheduler.common.exception.BaseException;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.utils.TaskInstanceUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.server.master.cache.ProcessInstanceExecCacheManager;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.event.TaskEventHandleError;
import org.apache.dolphinscheduler.server.master.event.TaskStateEvent;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteThreadPool;
import org.apache.dolphinscheduler.service.log.LogClient;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.service.process.ProcessServiceForYarn;
import org.apache.dolphinscheduler.service.storage.impl.HadoopUtils;
import org.apache.dolphinscheduler.service.utils.LoggerUtils;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import lombok.NonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class YarnTaskStateMonitorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(YarnTaskStateMonitorService.class);

    private final MasterConfig masterConfig;

    private final ProcessService processService;

    private final ProcessServiceForYarn processServiceForYarn;

    private final WorkflowExecuteThreadPool workflowExecuteThreadPool;

    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    private final String localAddress;

    private final LogClient logClient;

    public YarnTaskStateMonitorService(@NonNull MasterConfig masterConfig,
                                       @NonNull ProcessService processService,
                                       @NonNull ProcessServiceForYarn processServiceForYarn,
                                       @NonNull WorkflowExecuteThreadPool workflowExecuteThreadPool,
                                       @NonNull ProcessInstanceExecCacheManager processInstanceExecCacheManager,
                                       @NonNull LogClient logClient) {
        this.masterConfig = masterConfig;
        this.processService = processService;
        this.processServiceForYarn = processServiceForYarn;
        this.localAddress = masterConfig.getMasterAddress();
        this.workflowExecuteThreadPool = workflowExecuteThreadPool;
        this.processInstanceExecCacheManager = processInstanceExecCacheManager;
        this.logClient = logClient;
    }

    public void checkYarnTaskState() {
        // 检查运行中的YARN任务在RM中的状态是否与DS中一致，若RM中已失败，且RetryTimes < MaxRetryTimes，
        // 则设置NEED_FAULT_TOLERANCE状态，
        // 否则设置为执行失败状态
        if (!masterConfig.isReferYarnExecuteState()) {
            return;
        }
        final List<ProcessInstance> processInstances =
                processServiceForYarn.queryRunningProcessInstancesContainYarnTask(this.localAddress);
        for (ProcessInstance processInstance : processInstances) {
            ProcessDefinition processDefinition =
                    processService.findProcessDefinition(processInstance.getProcessDefinitionCode(),
                            processInstance.getProcessDefinitionVersion());
            processInstance.setProcessDefinition(processDefinition);
            int processInstanceId = processInstance.getId();
            final WorkflowExecuteRunnable workflowExecuteRunnable =
                    this.processInstanceExecCacheManager.getByProcessInstanceId(processInstanceId);
            if (workflowExecuteRunnable == null) {
                continue;
            }
            Collection<TaskInstance> taskInstanceList = workflowExecuteRunnable.getAllTaskInstances();
            for (TaskInstance taskInstance : taskInstanceList) {
                if (!taskInstance.isSubProcess() &&
                        TaskExecutionStatus.RUNNING_EXECUTION == taskInstance.getState() &&
                        ("SPARK".equalsIgnoreCase(taskInstance.getTaskType()) &&
                                taskInstance.getTaskParams().contains("\"deployMode\":\"cluster\"") ||
                                "FLINK".equalsIgnoreCase(taskInstance.getTaskType()) &&
                                        taskInstance.getTaskParams().contains("\"deployMode\":\"application\""))) {
                    LOGGER.info("Check YARN status for task {}", taskInstance.getName());
                    taskInstance.setProcessInstance(processInstance);

                    String appIds = taskInstance.getAppLink();
                    if (appIds != null) {
                        final String[] split = appIds.split(TaskConstants.COMMA);
                        String id = split[split.length - 1];
                        LoggerUtils.setWorkflowAndTaskInstanceIDMDC(taskInstance.getProcessInstanceId(),
                                taskInstance.getId());
                        try {
                            final TaskExecutionStatus applicationStatus = getApplicationStatus(id);
                            LOGGER.info("Check YARN status for task {}, applicationID: {}, status: {}",
                                    taskInstance.getName(), id, applicationStatus);
                            if (applicationStatus != taskInstance.getState()) {
                                TaskInstance oldTaskInstance = new TaskInstance();
                                TaskInstanceUtils.copyTaskInstance(taskInstance, oldTaskInstance);
                                try {
                                    switch (applicationStatus) {
                                        case FAILURE:
                                            if (taskInstance.getRetryTimes() < taskInstance.getMaxRetryTimes()) {
                                                taskInstance.setState(TaskExecutionStatus.NEED_FAULT_TOLERANCE);
                                            } else {
                                                taskInstance.setState(applicationStatus);
                                            }
                                            taskInstance.setFlag(Flag.NO);
                                            break;
                                        case KILL:
                                        case SUCCESS:
                                            taskInstance.setState(applicationStatus);
                                            break;
                                        default:
                                            continue;
                                    }
                                    taskInstance.setEndTime(new Date());
                                    taskInstance.setAppLink(id);
                                    processService.saveTaskInstance(taskInstance);
                                } catch (Exception ex) {
                                    TaskInstanceUtils.copyTaskInstance(oldTaskInstance, taskInstance);
                                    throw new TaskEventHandleError(
                                            "Update task state error, save taskInstance to db error", ex);
                                }

                                TaskStateEvent stateEvent = TaskStateEvent.builder()
                                        .processInstanceId(processInstance.getId())
                                        .taskInstanceId(taskInstance.getId())
                                        .status(taskInstance.getState())
                                        .type(StateEventType.TASK_STATE_CHANGE)
                                        .build();
                                workflowExecuteThreadPool.submitStateEvent(stateEvent);
                            }
                        } catch (Exception e) {
                            LOGGER.error("Get yarn application app id [{}}] status failed, " +
                                    "cannot confirm status of taskInstanceId[{}]", id, taskInstance.getId(), e);
                        } finally {
                            LoggerUtils.removeWorkflowAndTaskInstanceIdMDC();
                        }
                    }
                }
            }
        }
    }

    private static TaskExecutionStatus getApplicationStatus(String id) throws BaseException {
        final int retryTimes = 2;
        ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS * 30L);
        TaskExecutionStatus applicationStatus = HadoopUtils.getInstance().getApplicationStatus(id);
        int retryCount = 0;
        while (applicationStatus.isFailure() && retryCount < retryTimes) {
            ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS * 15L);
            applicationStatus = HadoopUtils.getInstance().getApplicationStatus(id);
            retryCount++;
        }
        return applicationStatus;
    }
}
