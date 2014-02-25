package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.packet.AddRecordPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.RemoveRecordPacket;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
import edu.umass.cs.gns.util.UpdateTrace;
import edu.umass.cs.gns.workloads.ExponentialDistribution;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class SendUpdatesViaIntercessor {

  public static void schdeduleAllUpdates() {
    if (LocalNameServer.updateTrace == null) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("UpdateTrace trace is null. SendQueriesViaIntercessor thread quitting.");
      }
      return;
    }
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Send update intercessor started. Number of queries. "
              + LocalNameServer.updateTrace.size());
    }
    ExponentialDistribution exponentialDistribution = new ExponentialDistribution(StartLocalNameServer.updateRateRegular);

    double expectedDurationSec = (LocalNameServer.updateTrace.size()
            * StartLocalNameServer.updateRateRegular) / 1000;

    String msg = "SendUpdateStart Expected-Duration " + expectedDurationSec
            + " Number-Queries " + LocalNameServer.updateTrace.size();
    GNS.getStatLogger().fine(msg);
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine(msg);
    }

    double delay = 0;

    GNS.getLogger().info(" Initial update delay: " + delay);
    List<Double> delays = new ArrayList<Double>();
    List<TimerTask> tasks = new ArrayList<TimerTask>();
    int count = 0;
    for (UpdateTrace u : LocalNameServer.updateTrace) {
      count++;
      if (u.type == UpdateTrace.UPDATE) {
        tasks.add(new SendUpdateIntercessorTask(u.name, count));
      } else if (u.type == UpdateTrace.ADD) {
        tasks.add(new SendAddIntercessorTask(u.name, count));
      } else if (u.type == UpdateTrace.REMOVE) {
        tasks.add(new SendRemoveIntercessorTask(u.name, count));
      }
      delays.add(delay);
      delay += exponentialDistribution.exponential();
    }
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < LocalNameServer.updateTrace.size(); i++) {
      LocalNameServer.executorService.schedule(tasks.get(i), (long) delays.get(i).intValue(), TimeUnit.MILLISECONDS);
    }
    long t1 = System.currentTimeMillis();
    GNS.getLogger().info(" Time to submit all updates: " + (t1 - t0));
    System.out.println("Final delay = " + delay / 1000 + " Expected-duration " + expectedDurationSec);
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

  public static void main(String[] args) {
    StartLocalNameServer.updateRateRegular = 1.0;
    LocalNameServer.updateTrace = new ArrayList<UpdateTrace>();
    int requests = 300000;
    for (int i = 0; i < requests; i++) {
      LocalNameServer.updateTrace.add(new UpdateTrace("0", UpdateTrace.UPDATE));
    }
    schdeduleAllUpdates();
  }
}

class SendUpdateIntercessorTask extends TimerTask {

  int updateCount;
  String name;

  public SendUpdateIntercessorTask(String name, int updateCount) {

    this.updateCount = updateCount;
    this.name = name;
  }

  @Override
  public void run() {

    ResultValue newValue = new ResultValue();
    newValue.add(SendUpdatesViaIntercessor.getRandomString());

    UpdateAddressPacket updateAddressPacket = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_LNS,
            updateCount, updateCount,
            name, NameRecordKey.EdgeRecord, newValue, null,
            UpdateOperation.REPLACE_ALL, LocalNameServer.nodeID, -1, GNS.DEFAULT_TTL_SECONDS,
            //ignore signature info
            null, null, null);
    try {
      LNSPacketDemultiplexer.demultiplexLNSPackets(updateAddressPacket.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
}

class SendAddIntercessorTask extends TimerTask {

  int requestCount;
  String name;

  public SendAddIntercessorTask(String name, int count) {

    this.requestCount = count;
    this.name = name;
  }

  @Override
  public void run() {


    ResultValue newValue = new ResultValue();
    newValue.add(SendUpdatesViaIntercessor.getRandomString());
    AddRecordPacket packet = new AddRecordPacket(requestCount, name, NameRecordKey.EdgeRecord, newValue,
            LocalNameServer.nodeID, GNS.DEFAULT_TTL_SECONDS);

    try {
      LNSPacketDemultiplexer.demultiplexLNSPackets(packet.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
}

class SendRemoveIntercessorTask extends TimerTask {

  int requestCount;
  String name;

  public SendRemoveIntercessorTask(String name, int count) {

    this.requestCount = count;
    this.name = name;
  }

  @Override
  public void run() {

    RemoveRecordPacket packet = new RemoveRecordPacket(requestCount, name, LocalNameServer.nodeID);

    try {
      LNSPacketDemultiplexer.demultiplexLNSPackets(packet.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
}
