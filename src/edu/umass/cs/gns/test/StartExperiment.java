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

//        long initialExpDelayMillis = 1000;
//        Thread.sleep(initialExpDelayMillis); // Abhigyan: When multiple LNS are running on same machine, we wait for
//        // all lns's to bind to their respective listening port before sending any traffic. Otherwise, another LNS could
//        // start a new connection and bind to this LNS's listening port. We have seen this very often in cluster tests.
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }

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
