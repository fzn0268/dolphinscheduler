package org.apache.dolphinscheduler.service.process;

import org.apache.dolphinscheduler.dao.entity.ProcessInstance;

import java.util.List;

public interface ProcessServiceForYarn {

    List<ProcessInstance> queryRunningProcessInstancesContainYarnTask(String host);

}
