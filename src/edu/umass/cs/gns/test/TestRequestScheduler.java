package edu.umass.cs.gns.test;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.util.TestRequest;
import edu.umass.cs.gns.workloads.ExponentialDistribution;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestRequestScheduler {

  public static void schdeduleAllRequests(List<TestRequest> testRequest, double requestRateMillis,
                                          ScheduledThreadPoolExecutor executorService) {
    if (testRequest == null) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Request trace is null. Scheduler returning.");
      }
      return;
    }
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Number of requests:" + testRequest.size());
    }
    ExponentialDistribution exponentialDistribution = new ExponentialDistribution(requestRateMillis);

    double expectedDurationSec = (testRequest.size() * requestRateMillis) / 1000;

    String msg = "SendRequestStart Expected-Duration " + expectedDurationSec
            + " Number-Requests " + testRequest.size();
    GNS.getStatLogger().fine(msg);
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine(msg);
    }

    double delay = 0;

    GNS.getLogger().info(" Initial update delay: " + delay);
    List<Double> delays = new ArrayList<Double>();
    List<TimerTask> tasks = new ArrayList<TimerTask>();
    int count = 0;
    for (TestRequest u : testRequest) {
      count++;
      if (u.type == TestRequest.LOOKUP) {
        tasks.add(new SendTestLookupTask(u.name, count));
      }else if (u.type == TestRequest.UPDATE) {
        tasks.add(new SendTestUpdateTask(u.name, count));
      } else if (u.type == TestRequest.ADD) {
        tasks.add(new SendTestAddTask(u.name, count));
      } else if (u.type == TestRequest.REMOVE) {
        tasks.add(new SendTestRemoveTask(u.name, count));
      }
      delays.add(delay);
      delay += exponentialDistribution.exponential();
    }
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < testRequest.size(); i++) {
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
  public static String getRandomString() {
    Random rand = new Random();
    int intRange = 1000000;
    Integer x = intRange + rand.nextInt(1000000);
    return x.toString();
  }
//
//  public static void main(String[] args) {
//    StartLocalNameServer.updateRateRegular = 1.0;
//    updateTrace = new ArrayList<TestRequest>();
//    int requests = 300000;
//    for (int i = 0; i < requests; i++) {
//      updateTrace.add(new TestRequest("0", TestRequest.UPDATE));
//    }
//    schdeduleAllRequests();
//  }
}

class SendTestUpdateTask extends TimerTask {

  int updateCount;
  String name;

  public SendTestUpdateTask(String name, int updateCount) {

    this.updateCount = updateCount;
    this.name = name;
  }

  @Override
  public void run() {

    ResultValue newValue = new ResultValue();
    newValue.add(TestRequestScheduler.getRandomString());

    UpdateAddressPacket updateAddressPacket = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_LNS,
            updateCount, updateCount,
            name, NameRecordKey.EdgeRecord, newValue, null,
            UpdateOperation.REPLACE_ALL, -1, -1, GNS.DEFAULT_TTL_SECONDS,
            //ignore signature info
            null, null, null);
    try {
      LNSPacketDemultiplexer.demultiplexLNSPackets(updateAddressPacket.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
}

class SendTestAddTask extends TimerTask {

  int requestCount;
  String name;

  public SendTestAddTask(String name, int count) {

    this.requestCount = count;
    this.name = name;
  }

  @Override
  public void run() {

    ResultValue newValue = new ResultValue();
    newValue.add(TestRequestScheduler.getRandomString());
    AddRecordPacket packet = new AddRecordPacket(requestCount, name, NameRecordKey.EdgeRecord, newValue,
            -1, GNS.DEFAULT_TTL_SECONDS);

    try {
      LNSPacketDemultiplexer.demultiplexLNSPackets(packet.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
}

class SendTestRemoveTask extends TimerTask {

  int requestCount;
  String name;

  public SendTestRemoveTask(String name, int count) {

    this.requestCount = count;
    this.name = name;
  }

  @Override
  public void run() {

    RemoveRecordPacket packet = new RemoveRecordPacket(requestCount, name, -1);

    try {
      LNSPacketDemultiplexer.demultiplexLNSPackets(packet.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
}


class SendTestLookupTask extends TimerTask {

  int lookupCount;
  String name;

  public SendTestLookupTask(String name, int lookupCount) {

    this.lookupCount = lookupCount;
    this.name = name;
  }

  @Override
  public void run() {
    DNSPacket queryRecord = new DNSPacket(lookupCount, name, NameRecordKey.EdgeRecord, -1, null, null, null);
    queryRecord.getHeader().setId(lookupCount);

    JSONObject json;
    try {
      json = queryRecord.toJSONObjectQuestion();
      LNSPacketDemultiplexer.demultiplexLNSPackets(json);
    } catch (JSONException e) {
      e.printStackTrace();

    }

  }
}