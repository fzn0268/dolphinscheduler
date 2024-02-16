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
public class TaskUpdateCommand extends BaseCommand {

    public TaskUpdateCommand(String messageSenderAddress,
                             String messageReceiverAddress,
                             long messageSendTime) {
        super(messageSenderAddress, messageReceiverAddress, messageSendTime);
    }

    /**
     * task instance id
     */
    private int taskInstanceId;

    /**
     * process instance id
     */
    private int processInstanceId;

    /**
     * host
     */
    private String host;

    /**
     * processId
     */
    private int processId;

    /**
     * appIds
     */
    private String appIds;

    public Command convert2Command() {
        Command command = new Command();
        command.setType(CommandType.TASK_UPDATE);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }

}
