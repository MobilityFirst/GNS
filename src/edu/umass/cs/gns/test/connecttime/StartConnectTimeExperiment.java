package edu.umass.cs.gns.test.connecttime;

import edu.umass.cs.gns.test.WorkloadParams;

/**
 *
 * Created by abhigyan on 5/14/14.
 */
public class StartConnectTimeExperiment {

  public static void startTest(int nodeID, WorkloadParams workloadParams) {
    if (nodeID == workloadParams.getMobileId()) {
      new Thread(new Mobile(workloadParams.getMobileUpdateInterval())).start();
    }
    if (nodeID == workloadParams.getCorrespondentId()) {
      new Thread(new Correspondent(workloadParams.getMobileId())).start();
    }
  }
}
