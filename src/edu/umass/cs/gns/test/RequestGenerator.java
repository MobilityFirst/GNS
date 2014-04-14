package edu.umass.cs.gns.test;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.util.ConsistentHashing;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.workloads.ProbabilityDistribution;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class RequestGenerator {

  public  void generateRequests(WorkloadParams workloadParams, List<TestRequest> requests, ProbabilityDistribution probDistribution,
                                ScheduledThreadPoolExecutor executorService) {
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

    double expectedDurationSec = (requests.size() * probDistribution.getMean()) / 1000;

    String msg = "SendRequestStart Expected-Duration " + expectedDurationSec
            + " Number-Requests " + requests.size();
    GNS.getStatLogger().fine(msg);
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine(msg);
    }
    double delay = probDistribution.getMean();
    LNSPacketDemultiplexer lnsPacketDemultiplexer = new LNSPacketDemultiplexer();
    GNS.getLogger().info(" Initial update delay: " + delay);

    List<Double> delays = new ArrayList<Double>();
    List<TimerTask> tasks = new ArrayList<TimerTask>();

    int count = 0;
    for (TestRequest r: requests) {
      if (r.type == TestRequest.DELAY) {     // delay
        delay += Integer.parseInt(r.name);  // the name field conceals the delay that we want to introduce
                                            // between the previous and next request.
        continue;
      }
      count++;
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
      delays.add(delay);
      delay += probDistribution.getNextArrivalDelay();
    }
    long t0 = System.currentTimeMillis();
    assert tasks.size() == delays.size();
    for (int i = 0; i < tasks.size(); i++) {
      executorService.schedule(tasks.get(i), (long) delays.get(i).intValue(), TimeUnit.MILLISECONDS);
    }
    long t1 = System.currentTimeMillis();
    GNS.getLogger().info(" Time to submit all requests: " + (t1 - t0));
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Final delay = " + delay / 1000 + " Expected-duration " + expectedDurationSec);
    }
  }

  private static final String THOUSAND;
  static {
    StringBuilder sb = new StringBuilder();
    while (sb.length() < 1000) sb.append("x");
    THOUSAND = sb.toString();
  }

  /**
   * Returns string of length sizeKB. If sizeKB == 0, it returns a 8 character string.
   */
  private static String getRandomString(int sizeKB) {
    if (sizeKB == 0) {
      int intRange = 1000000;
      Integer x = intRange + new Random().nextInt(1000000);
      return x.toString();
    } else {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < sizeKB; i++) {
        sb.append(THOUSAND);
        sb.append("\n");
      }
      return sb.toString();
    }
  }

  class GenerateUpdateRequest extends TimerTask {

    private int updateCount;
    private String name;
    private LNSPacketDemultiplexer packetDemultiplexer;
    private int objectSizeKB;

    public GenerateUpdateRequest(String name, int updateCount, int objectSizeKB, LNSPacketDemultiplexer packetDemultiplexer) {

      this.updateCount = updateCount;
      this.name = name;
      this.packetDemultiplexer = packetDemultiplexer;
      this.objectSizeKB = objectSizeKB;
    }

    @Override
    public void run() {

      ResultValue newValue = new ResultValue();
      newValue.add(RequestGenerator.getRandomString(objectSizeKB));

      UpdatePacket updateAddressPacket = new UpdatePacket(-1,
              updateCount, updateCount,
              name, NameRecordKey.EdgeRecord, newValue, null, -1,
              UpdateOperation.REPLACE_ALL, -1, -1, GNS.DEFAULT_TTL_SECONDS,
              //ignore signature info
              null, null, null);
      try {
        packetDemultiplexer.handleJSONObject(updateAddressPacket.toJSONObject());
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
  }

  class GenerateAddRequest extends TimerTask {

    private int requestCount;
    private String name;
    private LNSPacketDemultiplexer packetDemultiplexer;
    private int objectSizeKB;
    private int ttl;

    public GenerateAddRequest(String name, int count, int objectSizeKB, int ttl, LNSPacketDemultiplexer packetDemultiplexer) {

      this.requestCount = count;
      this.name = name;
      this.packetDemultiplexer = packetDemultiplexer;
      this.objectSizeKB = objectSizeKB;
      this.ttl = ttl;
    }

    @Override
    public void run() {

      ResultValue newValue = new ResultValue();
      newValue.add(RequestGenerator.getRandomString(objectSizeKB));
      AddRecordPacket packet = new AddRecordPacket(-1, requestCount, name, NameRecordKey.EdgeRecord, newValue,
              -1, ttl);

      try {
        packetDemultiplexer.handleJSONObject(packet.toJSONObject());
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
  }

  class GenerateRemoveRequest extends TimerTask {

    private int requestCount;
    private String name;
    private LNSPacketDemultiplexer packetDemultiplexer;


    public GenerateRemoveRequest(String name, int count, LNSPacketDemultiplexer packetDemultiplexer) {

      this.requestCount = count;
      this.name = name;
      this.packetDemultiplexer = packetDemultiplexer;

    }

    @Override
    public void run() {

      RemoveRecordPacket packet = new RemoveRecordPacket(-1, requestCount, name, -1);

      try {
        packetDemultiplexer.handleJSONObject(packet.toJSONObject());
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
  }


  class GenerateGroupChangeRequest extends TimerTask {

    private int requestCount;
    private String name;
    private LNSPacketDemultiplexer packetDemultiplexer;
    private TestGroupChangeRequest groupChangeRequest;

    public GenerateGroupChangeRequest(String name, int count, TestGroupChangeRequest grpChange,
                                      LNSPacketDemultiplexer packetDemultiplexer) {
      this.requestCount = count;
      this.name = name;
      this.packetDemultiplexer = packetDemultiplexer;
      this.groupChangeRequest = grpChange;
    }

    @Override
    public void run() {

      NewActiveProposalPacket packet = new NewActiveProposalPacket(name, selectReplicaController(name),
              groupChangeRequest.replicaSet, groupChangeRequest.version);
      try {
        packetDemultiplexer.handleJSONObject(packet.toJSONObject());
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }

    private int selectReplicaController(String name) {
      Set<Integer> replicaControllers = ConsistentHashing.getReplicaControllerSet(name);
      return LocalNameServer.getGnsNodeConfig().getClosestServer(replicaControllers, null);
    }
  }


  class GenerateLookupRequest extends TimerTask {

    private int lookupCount;
    private String name;
    private LNSPacketDemultiplexer packetDemultiplexer;

    public GenerateLookupRequest(String name, int lookupCount, LNSPacketDemultiplexer packetDemultiplexer) {

      this.lookupCount = lookupCount;
      this.name = name;
      this.packetDemultiplexer = packetDemultiplexer;
    }

    @Override
    public void run() {
      DNSPacket queryRecord = new DNSPacket(-1, lookupCount, name, NameRecordKey.EdgeRecord, null, null, null);
      queryRecord.getHeader().setId(lookupCount);

      JSONObject json;
      try {
        json = queryRecord.toJSONObjectQuestion();
        packetDemultiplexer.handleJSONObject(json);
      } catch (JSONException e) {
        e.printStackTrace();

      }

    }
  }
}
