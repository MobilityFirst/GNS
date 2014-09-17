package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.workloads.ExponentialDistribution;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 *
 * Created by abhigyan on 5/21/14.
 */
public class NewRequestGenerator {

  private static final int PERIOD = 10000;

  private final WorkloadParams workloadParams;

  private final ClientRequestHandlerInterface handler;

  private final BufferedReader traceFileReader;

  private int numRuns = 0;

  private Double delayMillis = 0D;

  private int reqCount = 0;

  private double ratePerSec = 100.0; // default rate is 100 req/sec

  private ExponentialDistribution dist;

  public NewRequestGenerator(WorkloadParams workloadParams, String traceFile, ClientRequestHandlerInterface handler)
          throws FileNotFoundException {

    this.workloadParams = workloadParams;
    this.handler = handler;
    this.traceFileReader = new BufferedReader(new FileReader(traceFile));
    this.dist = new ExponentialDistribution(1/ratePerSec);
    new Timer().schedule(new RunTask(), 0, PERIOD);
  }


  class RunTask extends TimerTask {

    public void run() {
      numRuns += 1;

      LNSPacketDemultiplexer lnsPacketDemultiplexer = new LNSPacketDemultiplexer(handler);
      long firstDelay = -1;
      long lastDelay = -1;
      try {
        // schedule for 1 period ahead
        while (delayMillis < (numRuns + 2) * PERIOD) {
          String line = traceFileReader.readLine();
          if (line == null) {
            GNS.getLogger().info("Reached end-of-trace. Completed.");
            this.cancel();
            break;
          }
          TestRequest r = TestRequest.parseLine(line);
          if (r == null) {
            continue;
          } else if (r.type == TestRequest.RATE) {     // rate of request
            ratePerSec = Double.parseDouble(r.name);
            dist = new ExponentialDistribution(1.0/ratePerSec);
            continue;
          }
          if (r.type == TestRequest.DELAY) {     // delay
            delayMillis += Integer.parseInt(r.name);  // the name field conceals the delay that we want to introduce
            // between the previous and next request.
            continue;
          }

          int objectSizeBytes = (workloadParams.getObjectSizeKB() == 0) ? 10 : workloadParams.getObjectSizeKB()*1000;

          TimerTask t;
          if (r.type == TestRequest.LOOKUP) {
            t = new GenerateLookupRequest(r.name, reqCount, lnsPacketDemultiplexer);
          }else if (r.type == TestRequest.UPDATE) {
            t = new GenerateUpdateRequest(r.name, reqCount, objectSizeBytes, lnsPacketDemultiplexer);
          } else if (r.type == TestRequest.ADD) {
            String[] tokens = line.trim().split("\\s+");
            if (tokens.length > 2) {
              // if an initial set of active replicas for this name is given in trace, then use those replicas
              Set<NodeId<String>> activeReplicas = new HashSet<NodeId<String>>();
              for (int i = 2; i < tokens.length; i++) {
                activeReplicas.add(new NodeId<String>(tokens[i]));
              }
              GNS.getLogger().fine("Name " + r.name + " Initial active replicas: " + activeReplicas);
              t = new GenerateAddRequest(r.name, reqCount, objectSizeBytes, workloadParams.getTtl(),
                      lnsPacketDemultiplexer, activeReplicas);
            } else {
              t = new GenerateAddRequest(r.name, reqCount, objectSizeBytes, workloadParams.getTtl(),
                      lnsPacketDemultiplexer);
            }

          } else if (r.type == TestRequest.REMOVE) {
            t = new GenerateRemoveRequest(r.name, reqCount, lnsPacketDemultiplexer);
          } else if (r.type == TestRequest.GROUP_CHANGE) {
            t = new GenerateGroupChangeRequest(r.name, reqCount, (TestGroupChangeRequest) r, lnsPacketDemultiplexer);
          } else {
            GNS.getLogger().severe("Unknown request type found: " + r.toString());
            throw new UnsupportedOperationException();
          }

          // 'delay' is the delay from the first run. from the current
          long delayFromNow = (delayMillis.longValue() - PERIOD*(numRuns-1));
          GNS.getLogger().fine("delay: " + delayFromNow + " count: " + reqCount);
          if (handler != null) handler.getExecutorService().schedule(t, delayFromNow, TimeUnit.MILLISECONDS);
          if (firstDelay == -1) firstDelay = delayFromNow;
          lastDelay = delayFromNow;
          delayMillis += dist.getNextArrivalDelay() * 1000;
          reqCount++;
        }
        GNS.getStatLogger().info("Scheduled requests until ... " + delayMillis /1000 + "sec. run " + numRuns + " count " + reqCount
        + " firstdelay " + firstDelay + " lastdelay " + lastDelay);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws IOException {
    String traceFile = "/Users/abhigyan/Documents/workspace/GNS/test_output/trace/8";
    NewRequestGenerator rg = new NewRequestGenerator(new WorkloadParams(null),traceFile, null);
  }
}
