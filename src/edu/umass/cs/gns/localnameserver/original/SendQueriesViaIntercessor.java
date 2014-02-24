package edu.umass.cs.gns.localnameserver.original;



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


	private static ExponentialDistribution exponentialDistribution;

  public static void schdeduleAllQueries() {

    if (LocalNameServer.lookupTrace == null)
    {
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Lookup trace is null. SendQueriesViaIntercessor thread quitting.");
      return;
    }
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Send query intercessor started. Number of queries. "
            + LocalNameServer.lookupTrace.size());
		exponentialDistribution = new ExponentialDistribution(StartLocalNameServer.lookupRate );
//    double delay = LocalNameServer.lookupTrace.size();

    long expectedDurationSec = (long) ((LocalNameServer.lookupTrace.size() *
            StartLocalNameServer.lookupRate)/1000);
    String msg = "SendQueryStart Expected-Duration " + expectedDurationSec +
            " Number-Queries " + LocalNameServer.lookupTrace.size();

    GNS.getStatLogger().fine(msg);
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine(msg);
//    long delay = (long) (StartLocalNameServer.lookupRate * 1000);
//    LocalNameServer.executorService.scheduleAtFixedRate(new SendQueryIntercessorTask(), 0, (long) delay, TimeUnit.MICROSECONDS);
//    int num = 1;
//    if (StartLocalNameServer.lookupRate <1) {
//      num = (int) (1.0/StartLocalNameServer.lookupRate);
//    }

    // cache all name records
//    try {
//    HashSet<String> names = new HashSet<String>();
//    for( String name : LocalNameServer.lookupTrace) {
//      names.add(name);
//    }
//    for (String name: names) {
//      PendingTasks.addToPendingRequests(name);
//      Thread.sleep(1);
//    }
//
//
//      Thread.sleep(60000);
//    } catch (InterruptedException e) {
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    }
    int requests = 0;
    if (LocalNameServer.lookupTrace != null) requests += LocalNameServer.lookupTrace.size();
    if (LocalNameServer.updateTrace != null) requests += LocalNameServer.updateTrace.size();
    double delay = (requests)/100000 * 1000;

    GNS.getLogger().info(" Initial lookup delay: " + delay);
    List<Double> delays = new ArrayList<Double>();
    List<TimerTask> tasks = new ArrayList<TimerTask>();
    int count = 0;
		for( String name : LocalNameServer.lookupTrace) {
			count++;
      tasks.add(new SendQueryIntercessorTask(name, count));
      delays.add(delay);
			delay += StartLocalNameServer.lookupRate; //exponentialDistribution.exponential();
//			if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Send query scheduled: count " + count + " delay = " + delay);
		}
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < LocalNameServer.lookupTrace.size(); i++) {
      LocalNameServer.executorService.schedule(tasks.get(i), (long) delays.get(i).intValue(), TimeUnit.MILLISECONDS);
    }
    long t1 = System.currentTimeMillis();
    GNS.getLogger().info(" Time to submit all updates: " + (t1 - t0));
//    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Final delay = " + delay/1000 + " Expected-duration " + expectedDurationSec);
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
//    if (name.equals("5242") == false) return;
    DNSPacket queryRecord = new DNSPacket(lookupCount, name, NameRecordKey.EdgeRecord, LocalNameServer.nodeID, null, null, null);
    queryRecord.getHeader().setId(lookupCount);

    JSONObject json;
    try {
      json = queryRecord.toJSONObjectQuestion();
      LNSListener.demultiplexLNSPackets(json);
    } catch (JSONException e) {
      e.printStackTrace();

    }

  }
}