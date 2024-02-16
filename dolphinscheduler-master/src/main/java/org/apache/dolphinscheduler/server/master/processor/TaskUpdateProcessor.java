package org.apache.dolphinscheduler.server.master.processor;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.TaskUpdateCommand;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.server.master.processor.queue.TaskEvent;
import org.apache.dolphinscheduler.server.master.processor.queue.TaskEventService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;

@Component
public class TaskUpdateProcessor implements NettyRequestProcessor {

    private final Logger logger = LoggerFactory.getLogger(TaskUpdateProcessor.class);

    @Autowired
    private TaskEventService taskEventService;

    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(CommandType.TASK_UPDATE == command.getType(),
                String.format("invalid command type : %s", command.getType()));
        TaskUpdateCommand taskUpdateMessage = JSONUtils.parseObject(command.getBody(), TaskUpdateCommand.class);
        logger.info("taskUpdateCommand: {}", taskUpdateMessage);

        TaskEvent taskEvent = TaskEvent.newUpdateEvent(taskUpdateMessage,
                channel,
                taskUpdateMessage.getMessageSenderAddress());
        taskEventService.addEvent(taskEvent);
    }

}
