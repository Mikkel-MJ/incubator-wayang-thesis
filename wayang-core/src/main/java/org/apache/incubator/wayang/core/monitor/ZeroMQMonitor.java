package org.apache.incubator.wayang.core.monitor;

import org.apache.incubator.wayang.core.api.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO: Implement
 */

public class ZeroMQMonitor extends Monitor {
    @Override
    public void initialize(Configuration config, String runId, List<Map> initialExecutionPlan) throws IOException {

    }

    @Override
    public void updateProgress(HashMap<String, Integer> partialProgress) throws IOException {

    }
}