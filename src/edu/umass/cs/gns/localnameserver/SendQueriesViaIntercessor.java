package edu.umass.cs.gns.localnameserver;



import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.workloads.ExponentialDistribution;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;


public class SendQueriesViaIntercessor {



  public static void schdeduleAllQueries() {

    if (LocalNameServer.lookupTrace == null)
    {
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Lookup trace is null. SendQueriesViaIntercessor thread quitting.");
      return;
    }
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Send query intercessor started. Number of queries. "
            + LocalNameServer.lookupTrace.size());
    ExponentialDistribution exponentialDistribution = new ExponentialDistribution(StartLocalNameServer.lookupRate );

    long expectedDurationSec = (long) ((LocalNameServer.lookupTrace.size() *
            StartLocalNameServer.lookupRate)/1000);
    String msg = "SendQueryStart Expected-Duration " + expectedDurationSec +
            " Number-Queries " + LocalNameServer.lookupTrace.size();

    GNS.getStatLogger().fine(msg);
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine(msg);

    double delay = 0;

    GNS.getLogger().info(" Initial lookup delay: " + delay);
    List<Double> delays = new ArrayList<Double>();
    List<TimerTask> tasks = new ArrayList<TimerTask>();
    int count = 0;
		for( String name : LocalNameServer.lookupTrace) {
			count++;
      tasks.add(new SendQueryIntercessorTask(name, count));
      delays.add(delay);
			delay += exponentialDistribution.exponential();
		}
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < LocalNameServer.lookupTrace.size(); i++) {
      LocalNameServer.executorService.schedule(tasks.get(i), (long) delays.get(i).intValue(), TimeUnit.MILLISECONDS);
    }
    long t1 = System.currentTimeMillis();
    GNS.getLogger().info(" Time to submit all updates: " + (t1 - t0));
  }

}


class SendQueryIntercessorTask extends TimerTask {

  int lookupCount;
  String name;

  public SendQueryIntercessorTask(String name, int lookupCount) {

    this.lookupCount = lookupCount;
    this.name = name;
  }

  @Override
  public void run() {
    DNSPacket queryRecord = new DNSPacket(lookupCount, name, NameRecordKey.EdgeRecord, LocalNameServer.nodeID, null, null, null);
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