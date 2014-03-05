/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.ping;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.utils.Util;
import java.io.IOException;

/**
 *
 * @author westy
 */
public class Pinger {

  private final static int PINGDELAY = 10000;
  static int nodeId;
  static PingClient pingClient;
  private static long pingTable[];

  public static void startPinging(int nodeId) {
    Pinger.nodeId = nodeId;
    Pinger.pingClient = new PingClient();
    int hostCnt = ConfigFileInfo.getAllHostIDs().size();
    pingTable = new long[hostCnt];
    (new Thread() {
      @Override
      public void run() {
        doPinging();
      }
    }).start();
  }

  private static void doPinging() {
    GNS.getLogger().fine("Waiting for a bit before we start pinging.");
    while (true) {
      Util.sleep(PINGDELAY);
      for (int id : ConfigFileInfo.getAllHostIDs()) {
        try {
          if (id != nodeId) {
            long rtt = pingClient.sendPing(id);
            GNS.getLogger().fine("From " + nodeId + " to " + id + " RTT = " + rtt);
            pingTable[id] = rtt;
            // sure why not
            ConfigFileInfo.updatePingLatency(id, rtt);
          }
        } catch (IOException e) {
          GNS.getLogger().severe("Problem sending ping to node " + id + " : " + e);
        }
      }
      GNS.getLogger().fine("PINGER: " + tableToString());
    }
  }

  public static String tableToString() {
    int hostCnt = ConfigFileInfo.getAllHostIDs().size();
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < hostCnt; i++) {
      result.append(i);
      result.append(" = ");
      result.append(pingTable[i]);
      result.append(NEWLINE);
    }

    return result.toString();
  }

  public static void main(String args[]) throws Exception {
    String configFile = args[0];
    NameServer.setNodeID(0);
    ConfigFileInfo.readHostInfo(configFile, NameServer.getNodeID());
    Pinger.startPinging(0);
  }
  
  public final static String NEWLINE = System.getProperty("line.separator");
}
