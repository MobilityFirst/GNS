package edu.umass.cs.gns.test.connecttime;

import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.test.WorkloadParams;

/**
 *
 * Created by abhigyan on 5/14/14.
 */
@SuppressWarnings("unchecked")
public class StartConnectTimeExperiment {

  public static void startTest(String nodeID, WorkloadParams workloadParams, ClientRequestHandlerInterface handler) {
    if (nodeID.equals(workloadParams.getMobileId())) {
      new Thread(new Mobile(workloadParams.getMobileUpdateInterval(), handler)).start();
    }
    if (nodeID.equals(workloadParams.getCorrespondentId())) {
      new Thread(new Correspondent(workloadParams.getMobileId(), handler)).start();
    }
  }
}
