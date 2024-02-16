package org.apache.dolphinscheduler.server.worker.message;

import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.TaskUpdateCommand;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;
import org.apache.dolphinscheduler.server.worker.rpc.WorkerRpcClient;

import lombok.NonNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TaskUpdateMessageSender implements MessageSender<TaskUpdateCommand> {

    @Autowired
    private WorkerRpcClient workerRpcClient;

    @Autowired
    private WorkerConfig workerConfig;

    @Override
    public void sendMessage(TaskUpdateCommand message) throws RemotingException {
        workerRpcClient.send(Host.of(message.getMessageReceiverAddress()), message.convert2Command());
    }

    @Override
    public TaskUpdateCommand buildMessage(@NonNull TaskExecutionContext taskExecutionContext,
                                          @NonNull String messageReceiverAddress) {
        TaskUpdateCommand taskUpdateRequestMessage =
                new TaskUpdateCommand(workerConfig.getWorkerAddress(),
                        messageReceiverAddress,
                        System.currentTimeMillis());
        taskUpdateRequestMessage.setTaskInstanceId(taskExecutionContext.getTaskInstanceId());
        taskUpdateRequestMessage.setProcessInstanceId(taskExecutionContext.getProcessInstanceId());
        taskUpdateRequestMessage.setProcessId(taskExecutionContext.getProcessId());
        taskUpdateRequestMessage.setAppIds(taskExecutionContext.getAppIds());
        return taskUpdateRequestMessage;
    }

    @Override
    public CommandType getMessageType() {
        return CommandType.TASK_UPDATE;
    }
}
