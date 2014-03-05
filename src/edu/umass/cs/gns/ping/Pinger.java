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
    GNS.getLogger().info("Waiting for a bit before we start pinging.");
    while (true) {
      Util.sleep(PINGDELAY);
      for (int id : ConfigFileInfo.getAllHostIDs()) {
        try {
          if (id != nodeId) {
            long rtt = pingClient.sendPing(id);
            GNS.getLogger().info("From " + nodeId + " to " + id + " RTT = " + rtt);
            pingTable[id] = rtt;
            // sure why not
            ConfigFileInfo.updatePingLatency(id, rtt);
          }
        } catch (IOException e) {
          GNS.getLogger().severe("Problem sending ping to node " + id + " : " + e);
        }
      }
      GNS.getLogger().info("PINGER: " + tableToString(nodeId));
    }
  }

  public static String tableToString(int node) {
    int hostCnt = ConfigFileInfo.getAllHostIDs().size();
    StringBuilder result = new StringBuilder();
    result.append("Node   RTT     L/NS Hostname");
    result.append(NEWLINE);
    for (int i = 0; i < hostCnt; i++) {
      result.append(String.format("%2d", i));
      if (i != node) {
        result.append("  =  ");
        result.append(String.format("%3d", pingTable[i]));
        result.append("     ");
      } else {
        result.append(" {this node} ");
      }
      result.append(ConfigFileInfo.isNameServer(i) ? "NS " : "LNS");
      result.append("  ");
      result.append(ConfigFileInfo.getIPAddress(i).getHostName());
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
