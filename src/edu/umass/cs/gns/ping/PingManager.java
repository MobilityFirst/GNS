/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.ping;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
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

  private final static int TIME_BETWEEN_PINGS = 3000;
  private final int nodeId;
  private PingClient pingClient;
  private final static int WINDOWSIZE = 10; // how many old samples of rtts we keep
  private SparseMatrix<Long> pingTable; // the place we store all the sampled rtt values
  private final GNSNodeConfig gnsNodeConfig;

  public PingManager(int nodeId, GNSNodeConfig gnsNodeConfig) {
    this.nodeId = nodeId;
    this.gnsNodeConfig = gnsNodeConfig;
  }

  /**
   * Starts the pinging thread for the given node.
   */
  public void startPinging() {
    pingClient = new PingClient(gnsNodeConfig);
    pingTable = new SparseMatrix(PingClient.INVALID_INTERVAL);
    // make a thread to run the pinger
    (new Thread() {
      @Override
      public void run() {
        doPinging();
      }
    }).start();
  }

  private void doPinging() {
    GNS.getLogger().fine("Waiting for a bit before we start pinging.");
    int windowSlot = 0;
    while (true) {
      Util.sleep(TIME_BETWEEN_PINGS);
      for (int id : gnsNodeConfig.getNodeIDs()) {
        try {
          if (id != nodeId) {
            GNS.getLogger().fine("Send from " + nodeId + " to " + id);
            long rtt = pingClient.sendPing(id);
            GNS.getLogger().fine("From " + nodeId + " to " + id + " RTT = " + rtt);
            pingTable.put(id, windowSlot, rtt);
            //pingTable[id][windowSlot] = rtt;
            // update the configuration file info with the current average... the reason we're here
            gnsNodeConfig.updatePingLatency(id, nodeAverage(id));
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
  public long nodeAverage(int node) {
    // calculate the average ignoring bad samples
    int count = WINDOWSIZE; // tracks the number of good samples
    double total = 0;
    for (int j = 0; j < WINDOWSIZE; j++) {
      //System.out.println("PINGTABLE: Node: " + node + " Time: " + j + " = " + pingTable.get(node, j));
      if (pingTable.get(node, j) != PingClient.INVALID_INTERVAL) {
        total = total + pingTable.get(node, j);
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
  public String tableToString(int node) {
    StringBuilder result = new StringBuilder();
    result.append("Node  AVG   RTT {last " + WINDOWSIZE + " samples}                    L/NS Hostname");
    result.append(NEWLINE);
    for (int i : gnsNodeConfig.getNodeIDs()) {
      result.append(String.format("%4d", i));
      if (i != node) {
        result.append(" = ");
        result.append(String.format("%d", nodeAverage(i)));
        result.append(" : ");
        // not print out all the samples... just do it in array order 
        // maybe do it in time order someday if we want to be cute
        for (int j = 0; j < WINDOWSIZE; j++) {
          if (pingTable.get(i, j) != PingClient.INVALID_INTERVAL) {
            result.append(String.format("%3d", pingTable.get(i, j)));
          } else {
            result.append("  X");
          }
          result.append(" ");
        }
      } else { // i == node
        // for this node just output something useful
        result.append("  {this node }                                   ");
      }
      result.append(gnsNodeConfig.isNameServer(i) ? "  NS " : "  LNS");
      result.append("  ");
      result.append(gnsNodeConfig.getNodeAddress(i).getHostName());
      result.append(NEWLINE);
    }

    return result.toString();
  }

  public static void main(String args[]) throws Exception {
    String configFile = args[0];
    int nodeID = 0;
    GNSNodeConfig gnsNodeConfig1 = new GNSNodeConfig(configFile, nodeID);
//    ConfigFileInfo.readHostInfo(configFile, NameServer.getNodeID());
    new PingManager(nodeID, gnsNodeConfig1).startPinging();
  }
  public final static String NEWLINE = System.getProperty("line.separator");
}
