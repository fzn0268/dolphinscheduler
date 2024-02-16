package org.apache.dolphinscheduler.service.process;

import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.mapper.ProcessInstanceMapper;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProcessServiceForYarnImpl implements ProcessServiceForYarn {

    @Autowired
    private ProcessInstanceMapper processInstanceMapper;

    @Override
    public List<ProcessInstance> queryRunningProcessInstancesContainYarnTask(String host) {
        final List<ProcessInstance> processInstances = processInstanceMapper.queryByHostAndStatus(host,
                new int[]{WorkflowExecutionStatus.RUNNING_EXECUTION.getCode()});
        return processInstances;
    }
}
