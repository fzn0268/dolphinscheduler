package org.apache.dolphinscheduler.server.master.runner;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.service.YarnTaskStateMonitorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class YarnTaskStateMonitorThread extends BaseDaemonThread {

    private static final Logger logger = LoggerFactory.getLogger(StateWheelExecuteThread.class);

    @Autowired
    private MasterConfig masterConfig;

    @Autowired
    private YarnTaskStateMonitorService yarnTaskStateMonitorService;

    protected YarnTaskStateMonitorThread() {
        super(YarnTaskStateMonitorThread.class.getSimpleName());
    }

    @Override
    public synchronized void start() {
        logger.info("YARN task state monitor thread starting");
        super.start();
        logger.info("YARN task state monitor thread started");
    }

    @Override
    public void run() {
        // when startup, wait 10s for ready
        ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS * 10);

        while (!ServerLifeCycleManager.isStopped()) {
            try {
                if (!ServerLifeCycleManager.isRunning()) {
                    continue;
                }
                yarnTaskStateMonitorService.checkYarnTaskState();
            } catch (Exception e) {
                logger.error("YARN task state monitor thread execute error", e);
            } finally {
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS * 10);
            }
        }
    }
}
