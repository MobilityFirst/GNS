package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.test.connecttime.StartConnectTimeExperiment;

import java.io.IOException;

/**
 * This class reads the experiment type field in workloadFile, and starts the corresponding experiment.
 *
 * Created by abhigyan on 5/14/14.
 */
public class StartExperiment {

  public void startMyTest(int nodeID, String workloadFile, String lookupTraceFile, String updateTraceFile,
                           double lookupRate, double updateRate, ClientRequestHandlerInterface handler)
          throws IOException, InterruptedException {
    GNS.getLogger().info("Workload config file: " + workloadFile);
    WorkloadParams params = null;
    if (workloadFile != null) {
      params = new WorkloadParams(workloadFile);
    }

    if (params == null || params.getExpType().equals(ExpType.TRACE)) {
      GNS.getLogger().info("Starting trace based experiment ... ");
      TraceRequestGenerator.genRequests(workloadFile, lookupTraceFile, updateTraceFile, lookupRate, updateRate,
              handler);
    }
    else if (params.getExpType().equals(ExpType.CONNECT_TIME)) {
      GNS.getLogger().info("Starting connect time experiment ... ");
      StartConnectTimeExperiment.startTest(nodeID, params);
    }

  }
}
