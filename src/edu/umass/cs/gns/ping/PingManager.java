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
import java.net.PortUnreachableException;

/**
 * Handles the updating of ping latencies for Local and Name Servers. 
 * Uses PingClient to send out ping requests.
 * 
 * @author westy
 */
public class PingManager {

  private final static int TIME_BETWEEN_PINGS = 30000;
  private static int nodeId;
  private static PingClient pingClient;
  private final static int WINDOWSIZE = 10; // how many old samples of rtts we keep
  private static long pingTable[][]; // the place we store all the sampled rtt values

  /**
   * Starts the pinging thread for the given node.
   * 
   * @param nodeId 
   */
  public static void startPinging(int nodeId) {
    PingManager.nodeId = nodeId;
    PingManager.pingClient = new PingClient();
    int hostCnt = ConfigFileInfo.getAllHostIDs().size();
    pingTable = new long[hostCnt][WINDOWSIZE];
    // initialize the whole thing to -1 which represents a bad or unknown sample
    for (int i = 0; i < hostCnt; i++) {
      for (int j = 0; j < WINDOWSIZE; j++) {
        pingTable[i][j] = -1L;
      }
    }
    // make a thread to run the pinger
    (new Thread() {
      @Override
      public void run() {
        doPinging();
      }
    }).start();
  }

  private static void doPinging() {
    GNS.getLogger().fine("Waiting for a bit before we start pinging.");
    int windowSlot = 0;
    while (true) {
      Util.sleep(TIME_BETWEEN_PINGS);
      for (int id : ConfigFileInfo.getAllHostIDs()) {
        try {
          if (id != nodeId) {
            GNS.getLogger().fine("Send from " + nodeId + " to " + id);
            long rtt = pingClient.sendPing(id);
            GNS.getLogger().fine("From " + nodeId + " to " + id + " RTT = " + rtt);
            pingTable[id][windowSlot] = rtt;
            // update the configuration file info with the current average... the reason we're here
            ConfigFileInfo.updatePingLatency(id, nodeAverage(id));
          }
        } catch (PortUnreachableException e) {
          GNS.getLogger().severe("Problem sending ping to node " + id + " : " + e);
        } catch (IOException e) {
          GNS.getLogger().severe("Problem sending ping to node " + id + " : " + e);
        }
      }
      GNS.getLogger().fine("PINGER: " + tableToString(nodeId));
      windowSlot = (windowSlot + 1) % WINDOWSIZE;
    }
  }

  /**
   * Calculates the average ping time from the given node.
   * Returns 999L if value can't be determined.
   * 
   * @param node
   * @return 
   */
  public static long nodeAverage(int node) {
    // calculate the average ignoring bad samples
    int count = WINDOWSIZE; // tracks the number of good samples
    double total = 0;
    for (int j = 0; j < WINDOWSIZE; j++) {
      if (pingTable[node][j] != -1L) {
        total = total + pingTable[node][j];
      } else {
        count = count - 1;
      }
    }
    if (count == 0) {
      // probably should never happen but just in case
      return 999L;
    } else {
      return Math.round(total / count);
    }
  }

  /**
   * Outputs the sampled rtt values in a pretty format.
   * 
   * @param node
   * @return 
   */
  public static String tableToString(int node) {
    int hostCnt = ConfigFileInfo.getAllHostIDs().size();
    StringBuilder result = new StringBuilder();
    result.append("Node  AVG   RTT {last " + WINDOWSIZE + " samples}                    L/NS Hostname");
    result.append(NEWLINE);
    for (int i = 0; i < hostCnt; i++) {
      result.append(String.format("%2d", i));
      if (i != node) {
        result.append(" = ");
        result.append(String.format("%3d", nodeAverage(i)));
        result.append(" : ");
        // not print out all the samples... just do it in array order 
        // maybe do it in time order someday if we want to be cute
        for (int j = 0; j < WINDOWSIZE; j++) {
          if (pingTable[i][j] != -1L) {
            result.append(String.format("%3d", pingTable[i][j]));
          } else {
            result.append("  X");
          }
          result.append(" ");
        }
      } else { // i == node
        // for this node just output something useful
        result.append("  {this node }                                   ");
      }
      result.append(ConfigFileInfo.isNameServer(i) ? "  NS " : "  LNS");
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
    PingManager.startPinging(0);
  }
  public final static String NEWLINE = System.getProperty("line.separator");
}
