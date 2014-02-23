package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ListenerNameRecordStats;
import edu.umass.cs.gns.packet.NameRecordStatsPacket;
import edu.umass.cs.gns.statusdisplay.StatusClient;
import edu.umass.cs.gns.util.HashFunction;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ***********************************************************
 * This class implements a thread that periodically pushes read and write stats for names in its lookup table.
 *
 * @author Hardeep Uppal
 * **********************************************************
 */
public class SendNameRecordStats extends TimerTask {

  public static ConcurrentHashMap<String, int[]> allStats = new ConcurrentHashMap<String, int[]>();

  public static void incrementLookupCount(String name) {
    if (allStats.containsKey(name)) {
      allStats.get(name)[0]++;
      return;
    }
    int[] x = {1,0};
    allStats.put(name,x);
  }

  public static void incrementUpdateCount(String name) {
    if (allStats.containsKey(name)) {
      allStats.get(name)[1]++;
      return;
    }
    int[] x = {0,1};
    allStats.put(name,x);
  }

  int count = 0;

  @Override
  public void run() {
    count++;
    StatusClient.sendStatus(NameServer.nodeID, "Pushing stats: " + count);
    //Iterate through the NameRecords and push access frequency stats
    ConcurrentHashMap<String, int[]> collectedStats  = allStats;
    allStats = new ConcurrentHashMap<String, int[]>();
    for (String name: collectedStats.keySet()) {
//        try {

      int lookup = collectedStats.get(name)[0];
      int update = collectedStats.get(name)[1];
      if (lookup == 0 && update == 0) {
        if (StartNameServer.debugMode) GNS.getLogger().fine("Zero read write frequency. NO Frequency to report.");
        continue;
      }

      NameRecordStatsPacket statsPacket = new NameRecordStatsPacket(name, lookup, update, NameServer.nodeID);

      try {
        JSONObject json = statsPacket.toJSONObject();
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("PUSH_STATS: Round " + count + " Name " + name + " To primaries --> " + json);
        }
        int selectedPrimaryNS = -1;
        for (int x : HashFunction.getPrimaryReplicas(name)) {
          if (NameServer.paxosManager.isNodeUp(x)) {
            selectedPrimaryNS = x;
            break;
          }
        }
        if (selectedPrimaryNS != -1 && selectedPrimaryNS != NameServer.nodeID) {
          NameServer.tcpTransport.sendToID(selectedPrimaryNS, json);
        } else if (selectedPrimaryNS == NameServer.nodeID) {
          // if same node, then directly call the function
          ListenerNameRecordStats.handleIncomingPacket(json);
        }

      } catch (JSONException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

  }

}
