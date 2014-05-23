package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.workloads.ProbabilityDistribution;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;


public class RequestGenerator {

  public  void generateRequests(WorkloadParams workloadParams, List<TestRequest> requests,
                                ProbabilityDistribution probDistribution1, ClientRequestHandlerInterface handler) {
    if (requests == null) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Request trace is null. Scheduler returning.");
      }
      return;
    }
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Number of requests:" + requests.size());
    }
//    ExponentialDistribution exponentialDistribution = new ExponentialDistribution(requestRateMillis);

//    double expectedDurationSec = (requests.size() * probDistribution.getMean()) / 1000;

//    String msg = "SendRequestStart Expected-Duration " + expectedDurationSec
//            + " Number-Requests " + requests.size();
//    GNS.getStatLogger().fine(msg);
//    if (StartLocalNameServer.debugMode) {
//      GNS.getLogger().fine(msg);
//    }
    double delay = 0;
//            probDistribution.getMean();
    LNSPacketDemultiplexer lnsPacketDemultiplexer = new LNSPacketDemultiplexer(handler);
    GNS.getLogger().info(" Initial update delay: " + delay);

    List<Double> delays = new ArrayList<Double>();
    List<TimerTask> tasks = new ArrayList<TimerTask>();

    int count = 0;

    double ratePerSec = 100.0; // default rate is 100 req/sec

    for (TestRequest r: requests) {
      if (r.type == TestRequest.RATE) {     // delay
        ratePerSec = Double.parseDouble(r.name);
        continue;
      }
      if (r.type == TestRequest.DELAY) {     // delay
        delay += Integer.parseInt(r.name);  // the name field conceals the delay that we want to introduce
                                            // between the previous and next request.
        continue;
      }

      if (r.type == TestRequest.LOOKUP) {
        tasks.add(new GenerateLookupRequest(r.name, count, lnsPacketDemultiplexer));
      }else if (r.type == TestRequest.UPDATE) {
        tasks.add(new GenerateUpdateRequest(r.name, count, workloadParams.getObjectSizeKB(), lnsPacketDemultiplexer));
      } else if (r.type == TestRequest.ADD) {
        tasks.add(new GenerateAddRequest(r.name, count, workloadParams.getObjectSizeKB(), workloadParams.getTtl(),
                lnsPacketDemultiplexer));
      } else if (r.type == TestRequest.REMOVE) {
        tasks.add(new GenerateRemoveRequest(r.name, count, lnsPacketDemultiplexer));
      } else if (r.type == TestRequest.GROUP_CHANGE) {
        tasks.add(new GenerateGroupChangeRequest(r.name, count, (TestGroupChangeRequest) r, lnsPacketDemultiplexer));
      } else {
        GNS.getLogger().severe("Unknown packet type found: " + r.toString());
        throw new UnsupportedOperationException();
      }
      count++;
      delays.add(delay);
      delay += 1000.0/ratePerSec;
    }
    long t0 = System.currentTimeMillis();
    assert tasks.size() == delays.size();
    for (int i = 0; i < tasks.size(); i++) {
      handler.getExecutorService().schedule(tasks.get(i), (long) delays.get(i).intValue(), TimeUnit.MILLISECONDS);
    }
    long t1 = System.currentTimeMillis();
    GNS.getLogger().severe(" Time to submit all requests: " + (t1 - t0) + " Delay = " + delay);

    GNS.getLogger().info("Final delay = " + delay / 1000); //  + " Expected-duration " + expectedDurationSec

  }

  private static final String THOUSAND;
  static {
    StringBuilder sb = new StringBuilder();
    while (sb.length() < 1000) sb.append("x");
    THOUSAND = sb.toString();
  }


}
