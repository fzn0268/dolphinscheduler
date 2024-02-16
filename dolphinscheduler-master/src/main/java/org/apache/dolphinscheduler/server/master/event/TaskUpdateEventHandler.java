package org.apache.dolphinscheduler.server.master.event;

import org.apache.dolphinscheduler.common.enums.TaskEventType;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.utils.TaskInstanceUtils;
import org.apache.dolphinscheduler.remote.command.TaskUpdateAckCommand;
import org.apache.dolphinscheduler.server.master.cache.ProcessInstanceExecCacheManager;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.processor.queue.TaskEvent;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TaskUpdateEventHandler implements TaskEventHandler {

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    @Autowired
    private ProcessService processService;

    @Autowired
    private MasterConfig masterConfig;

    @Override
    public void handleTaskEvent(TaskEvent taskEvent) throws TaskEventHandleError, TaskEventHandleException {
        int taskInstanceId = taskEvent.getTaskInstanceId();
        int processInstanceId = taskEvent.getProcessInstanceId();

        WorkflowExecuteRunnable workflowExecuteRunnable = this.processInstanceExecCacheManager.getByProcessInstanceId(
                processInstanceId);
        if (workflowExecuteRunnable == null) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task update event error, cannot find related workflow instance from cache, will discard this event");
        }
        Optional<TaskInstance> taskInstanceOptional = workflowExecuteRunnable.getTaskInstance(taskInstanceId);
        if (!taskInstanceOptional.isPresent()) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task update event error, cannot find the taskInstance from cache, will discord this event");
        }
        TaskInstance taskInstance = taskInstanceOptional.get();
        if (taskInstance.getState().isFinished()) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task update event error, the task instance is already finished, will discord this event");
        }

        TaskInstance oldTaskInstance = new TaskInstance();
        TaskInstanceUtils.copyTaskInstance(taskInstance, oldTaskInstance);
        try {
            taskInstance.setPid(taskEvent.getProcessId());
            taskInstance.setAppLink(taskEvent.getAppIds());
            processService.updateTaskInstance(taskInstance);
            sendAckToWorker(taskEvent);
        } catch (Exception ex) {
            TaskInstanceUtils.copyTaskInstance(oldTaskInstance, taskInstance);
            throw new TaskEventHandleError("Handle task result event error, save taskInstance to db error", ex);
        }
    }

    public void sendAckToWorker(TaskEvent taskEvent) {
        TaskUpdateAckCommand taskUpdateAckCommand = new TaskUpdateAckCommand(
                masterConfig.getMasterAddress(),
                taskEvent.getWorkerAddress(),
                System.currentTimeMillis(),
                taskEvent.getProcessInstanceId(),
                taskEvent.getTaskInstanceId(),
                true);
        taskEvent.getChannel().writeAndFlush(taskUpdateAckCommand);
    }

    @Override
    public TaskEventType getHandleEventType() {
        return TaskEventType.UPDATE;
    }
}
