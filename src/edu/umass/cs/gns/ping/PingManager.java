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
import edu.umass.cs.gns.nsdesign.Shutdownable;


import java.io.IOException;
import java.net.PortUnreachableException;



/**
 * Handles the updating of ping latencies for Local and Name Servers.
 * Uses PingClient to send out ping requests.
 *
 * @author westy
 */
public class PingManager implements Shutdownable{
  
  // Since we don't have ids for LNSs anymore this represents and the LNS in our table.
  public static final int LOCALNAMESERVERID = Integer.MAX_VALUE;

  private final static int TIME_BETWEEN_PINGS = 30000;
  private final int nodeId;
  private final PingClient pingClient;
  private final PingServer pingServer;
  private final static int WINDOWSIZE = 10; // how many old samples of rtts we keep
  private final SparseMatrix<Long> pingTable; // the place we store all the sampled rtt values
  private final GNSNodeConfig gnsNodeConfig;
  private Thread managerThread;

  private final boolean debug = false;

  public PingManager(int nodeId, GNSNodeConfig gnsNodeConfig) {
    this.nodeId = nodeId;
    this.gnsNodeConfig = gnsNodeConfig;
    this.pingClient = new PingClient(gnsNodeConfig);
    this.pingServer = new PingServer(nodeId, gnsNodeConfig);
    new Thread(pingServer).start();
    this.pingTable = new SparseMatrix(GNSNodeConfig.INVALID_PING_LATENCY);

  }

  /**
   * Starts the pinging thread for the given node.
   */
  public void startPinging() {

    // make a thread to run the pinger
    this.managerThread = new Thread() {
      @Override
      public void run() {
        try {
          doPinging();
        } catch (InterruptedException e) {
          GNS.getLogger().warning("Ping manager closing down.");
        }
      }
    };
    this.managerThread.start();
  }

  private void doPinging() throws InterruptedException {
    GNS.getLogger().fine("Waiting for a bit before we start pinging.");
    int windowSlot = 0;
    while (true) {
      Thread.sleep(TIME_BETWEEN_PINGS);
      long t0 = System.currentTimeMillis();
      // Note that we're only pinging other NameServers here, not LNSs (they don't have IDs anyway).
      for (int id : gnsNodeConfig.getNameServerIDs()) {
        try {
          if (id != nodeId) {
            if (debug) GNS.getLogger().fine("Send from " + nodeId + " to " + id);
            long rtt = pingClient.sendPing(id);
            if (debug) GNS.getLogger().fine("From " + nodeId + " to " + id + " RTT = " + rtt);
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
      long timeForAllPings = (System.currentTimeMillis() - t0)/1000;
      GNS.getStatLogger().info("\tAllPingsTime " + timeForAllPings + "\tNode\t" + nodeId + "\t");
      if (debug) GNS.getLogger().fine("PINGER: " + tableToString(nodeId));
      windowSlot = (windowSlot + 1) % WINDOWSIZE;
    }
  }

  /**
   * Calculates the average ping time from the given node.
   * Returns 9999L if value can't be determined.
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
      if (pingTable.get(node, j) != GNSNodeConfig.INVALID_PING_LATENCY) {
        total = total + pingTable.get(node, j);
      } else {
        count = count - 1;
      }
    }
    if (count == 0) {
      // probably should never happen but just in case
      return 9999L;
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
    for (int i : gnsNodeConfig.getNameServerIDs()) {
      result.append(String.format("%4d", i));
      if (i != node) {
        result.append(" = ");
        result.append(String.format("%d", nodeAverage(i)));
        result.append(" : ");
        // not print out all the samples... just do it in array order 
        // maybe do it in time order someday if we want to be cute
        for (int j = 0; j < WINDOWSIZE; j++) {
          if (pingTable.get(i, j) != GNSNodeConfig.INVALID_PING_LATENCY) {
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
    GNSNodeConfig gnsNodeConfig1 = GNSNodeConfig.CreateGNSNodeConfigFromOldStyleFile(configFile, nodeID);
    new PingManager(nodeID, gnsNodeConfig1).startPinging();
  }
  public final static String NEWLINE = System.getProperty("line.separator");

  public void shutdown() {
    GNS.getLogger().warning("Ping shutting down .. ");
    this.managerThread.interrupt();
    this.pingServer.shutdown();
    this.pingClient.shutdown();
    GNS.getLogger().warning("Ping shutdown  complete.");
  }
}
