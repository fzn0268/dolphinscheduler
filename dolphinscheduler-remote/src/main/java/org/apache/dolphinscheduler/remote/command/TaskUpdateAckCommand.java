package org.apache.dolphinscheduler.remote.command;

import org.apache.dolphinscheduler.common.utils.JSONUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class TaskUpdateAckCommand extends BaseCommand {

    public TaskUpdateAckCommand(String sourceServerAddress,
                                String messageReceiverAddress,
                                long messageSendTime,
                                int processInstanceId,
                                int taskInstanceId,
                                boolean success) {
        super(sourceServerAddress, messageReceiverAddress, messageSendTime);
        this.processInstanceId = processInstanceId;
        this.taskInstanceId = taskInstanceId;
        this.success = success;
    }

    /**
     * task instance id
     */
    private int taskInstanceId;

    /**
     * process instance id
     */
    private int processInstanceId;

    private boolean success;

    public Command convert2Command() {
        Command command = new Command();
        command.setType(CommandType.TASK_UPDATE_ACK);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }

}
