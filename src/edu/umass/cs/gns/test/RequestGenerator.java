package edu.umass.cs.gns.test;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.workloads.ProbabilityDistribution;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class RequestGenerator {

  public  void generateRequests(List<TestRequest> requests, ProbabilityDistribution probDistribution,
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
    for (TestRequest r : requests) {
      if (r.type == TestRequest.DELAY) {     // delay
        delay += Integer.parseInt(r.name);  // the name field conceals the delay that we want to introduce
                                            // between the previous and next request.
        continue;
      }
      count++;
      if (r.type == TestRequest.LOOKUP) {
        tasks.add(new GenerateLookupRequest(r.name, count, lnsPacketDemultiplexer));
      }else if (r.type == TestRequest.UPDATE) {
        tasks.add(new GenerateUpdateRequest(r.name, count, lnsPacketDemultiplexer));
      } else if (r.type == TestRequest.ADD) {
        tasks.add(new GenerateAddRequest(r.name, count, lnsPacketDemultiplexer));
      } else if (r.type == TestRequest.REMOVE) {
        tasks.add(new GenerateRemoveRequest(r.name, count, lnsPacketDemultiplexer));
      }
      delays.add(delay);
      delay += probDistribution.getNextArrivalDelay();
    }
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < requests.size(); i++) {
      executorService.schedule(tasks.get(i), (long) delays.get(i).intValue(), TimeUnit.MILLISECONDS);
    }
    long t1 = System.currentTimeMillis();
    GNS.getLogger().info(" Time to submit all requests: " + (t1 - t0));
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Final delay = " + delay / 1000 + " Expected-duration " + expectedDurationSec);
    }
  }

  /**
   * Returns a 8-character string.
   *
   * @return
   */
  private static String getRandomString() {
    Random rand = new Random();
    int intRange = 1000000;
    Integer x = intRange + rand.nextInt(1000000);
    return x.toString();
  }

  class GenerateUpdateRequest extends TimerTask {

    private int updateCount;
    private String name;
    private LNSPacketDemultiplexer packetDemultiplexer;

    public GenerateUpdateRequest(String name, int updateCount, LNSPacketDemultiplexer packetDemultiplexer) {

      this.updateCount = updateCount;
      this.name = name;
      this.packetDemultiplexer = packetDemultiplexer;
    }

    @Override
    public void run() {

      ResultValue newValue = new ResultValue();
      newValue.add(RequestGenerator.getRandomString());

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

    public GenerateAddRequest(String name, int count, LNSPacketDemultiplexer packetDemultiplexer) {

      this.requestCount = count;
      this.name = name;
      this.packetDemultiplexer = packetDemultiplexer;
    }

    @Override
    public void run() {

      ResultValue newValue = new ResultValue();
      newValue.add(RequestGenerator.getRandomString());
      AddRecordPacket packet = new AddRecordPacket(-1, requestCount, name, NameRecordKey.EdgeRecord, newValue,
              -1, GNS.DEFAULT_TTL_SECONDS);

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
