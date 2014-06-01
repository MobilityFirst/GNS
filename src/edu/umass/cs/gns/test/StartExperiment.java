package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.test.connecttime.StartConnectTimeExperiment;
import edu.umass.cs.gns.test.testclient.LNSSideImplementation;

import java.io.IOException;

/**
 * This class reads the experiment type field in workloadFile, and starts the corresponding experiment.
 *
 * Created by abhigyan on 5/14/14.
 */
public class StartExperiment {

  public void startMyTest(int nodeID, String workloadFile, String updateTraceFile,
                          ClientRequestHandlerInterface handler)
          throws IOException, InterruptedException {

    GNS.getLogger().info("Workload config file: " + workloadFile);
    WorkloadParams params = new WorkloadParams(workloadFile);

    if (updateTraceFile != null || params.getExpType().equals(ExpType.TRACE)) {
      GNS.getStatLogger().info("Starting trace based experiment ... ");
      new NewRequestGenerator(params, updateTraceFile, handler);
    }
    else if (params.getExpType().equals(ExpType.CONNECT_TIME)) {
      GNS.getStatLogger().info("Starting connect time experiment ... ");
      StartConnectTimeExperiment.startTest(nodeID, params);
    }
    else if (params.getExpType().equals(ExpType.BASICTEST)) {
      GNS.getStatLogger().info("Starting basic test ... ");
      LNSSideImplementation.startServer();
      // Test1Name is also a test
//      Test1Name.startTest();
    }

  }
}
