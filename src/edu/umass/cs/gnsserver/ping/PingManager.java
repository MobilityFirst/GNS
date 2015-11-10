/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.ping;

import edu.umass.cs.gnsserver.utils.SparseMatrix;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.nodeconfig.GNSConsistentNodeConfig;
import edu.umass.cs.gnsserver.nodeconfig.GNSInterfaceNodeConfig;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.utils.Shutdownable;
import java.io.IOException;
import java.net.PortUnreachableException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles the updating of ping latencies for Local and Name Servers.
 * Uses PingClient to send out ping requests.
 *
 * @author westy
 * @param <NodeIDType>
 */
public class PingManager<NodeIDType> implements Shutdownable {

  private final static int TIME_BETWEEN_PINGS = 30000;
  // Wait a random amount before we start pinging.
  private final static int INITIAL_DELAY = 5 + new Random().nextInt(TIME_BETWEEN_PINGS);
  private final NodeIDType nodeId;
  private final PingClient<NodeIDType> pingClient;
  private final PingServer<NodeIDType> pingServer;
  private final static int WINDOWSIZE = 10; // how many old samples of rtts we keep
  private final SparseMatrix<NodeIDType, Integer, Long> pingTable; // the place we store all the sampled rtt values
  private final GNSInterfaceNodeConfig<NodeIDType> nodeConfig;
  private Thread managerThread;

  // The list of actives we should be pinging.
  private final Set<NodeIDType> activeReplicas;

  /**
   * If true extra debugging info will be printed.
   */
  public static boolean debuggingEnabled = false;

  /**
   * Starts a ping manager. Start a server and optional client.
   *
   * @param nodeId
   * @param gnsNodeConfig
   * @param noClient
   */
  public PingManager(NodeIDType nodeId, GNSInterfaceNodeConfig<NodeIDType> gnsNodeConfig, boolean noClient) {
    this.nodeId = nodeId;
    this.nodeConfig = gnsNodeConfig;
    if (!noClient) {
      this.pingClient = new PingClient<NodeIDType>(gnsNodeConfig);
    } else {
      this.pingClient = null;
    }
    if (nodeId != null) {
      this.pingServer = new PingServer<NodeIDType>(nodeId, gnsNodeConfig);
      new Thread(pingServer).start();
    } else {
      this.pingServer = null;
    }
    this.pingTable = new SparseMatrix<NodeIDType, Integer, Long>(GNSNodeConfig.INVALID_PING_LATENCY);
    this.activeReplicas = Collections.newSetFromMap(new ConcurrentHashMap<NodeIDType, Boolean>());
  }

  /**
   * Starts a ping manager with a server and client.
   *
   * @param nodeId
   * @param gnsNodeConfig
   */
  public PingManager(NodeIDType nodeId, GNSInterfaceNodeConfig<NodeIDType> gnsNodeConfig) {
    this(nodeId, gnsNodeConfig, false);
  }

  /**
   * Starts a ping manager with just a client.
   *
   * @param gnsNodeConfig
   */
  public PingManager(GNSInterfaceNodeConfig<NodeIDType> gnsNodeConfig) {
    this(null, gnsNodeConfig, false);
  }

  /**
   * Add active replicas to the list that this server sends pings to.
   * 
   * @param activeReplicas 
   */
  public void addActiveReplicas(Set<NodeIDType> activeReplicas) {
    this.activeReplicas.addAll(activeReplicas);
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
          GNS.getLogger().info("Waiting for a bit before we start pinging.");
          Thread.sleep(INITIAL_DELAY);
          doPinging();
        } catch (InterruptedException e) {
          GNS.getLogger().warning("Ping manager closing down.");
        }
      }
    };
    this.managerThread.start();
  }

  private void doPinging() throws InterruptedException {
    int windowSlot = -1;
    Queue<NodeIDType> nodes = new LinkedList<>();
    while (true) {
      //long t0 = System.currentTimeMillis();
      // refill the nodes queue if it is empty
      if (nodes.isEmpty()) {
        nodes = new LinkedList<NodeIDType>(activeReplicas);
        // update our moving average window
        windowSlot = (windowSlot + 1) % WINDOWSIZE;
      }
      if (!nodes.isEmpty()) {
        // do one id
        NodeIDType id = nodes.remove();
        try {
          if (!id.equals(nodeId)) {
            if (debuggingEnabled) {
              GNS.getLogger().info("Send from " + nodeId + " to " + id);
            }
            long rtt = pingClient.sendPing(id);
            if (debuggingEnabled) {
              GNS.getLogger().info("From " + nodeId + " to " + id + " RTT = " + rtt);
            }
            pingTable.put(id, windowSlot, rtt);
            // update the configuration file info with the current average... the reason we're here
            nodeConfig.updatePingLatency(id, nodeAverage(id));
          }
        } catch (PortUnreachableException e) {
          GNS.getLogger().severe("Problem sending ping to node " + id + " : " + e);
        } catch (IOException e) {
          GNS.getLogger().severe("Problem sending ping to node " + id + " : " + e);
        }
      }

      //long timeForAllPings = (System.currentTimeMillis() - t0) / 1000;
      //GNS.getStatLogger().info("\tAllPingsTime " + timeForAllPings + "\tNode\t" + nodeId + "\t");
      if (debuggingEnabled) {
        GNS.getLogger().info("PINGER: \n" + tableToString(nodeId));
      }
      Thread.sleep(TIME_BETWEEN_PINGS);
    }

  }

  /**
   * Calculates the average ping time from the given node.
   * Returns 9999L if value can't be determined.
   *
   * @param node
   * @return a long
   */
  public long nodeAverage(NodeIDType node) {
    // calculate the average ignoring bad samples
    int count = WINDOWSIZE; // tracks the number of good samples
    double total = 0;
    for (int j = 0; j < WINDOWSIZE; j++) {
      //System.out.println("PINGTABLE: Node: " + node + " Time: " + j + " = " + pingTable.toString(node, j));
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
   * @return a string
   */
  public String tableToString(NodeIDType node) {
    StringBuilder result = new StringBuilder();
    result.append("Node  AVG   RTT {last " + WINDOWSIZE + " samples}                    Hostname");
    result.append(NEWLINE);
    for (NodeIDType otherNode : activeReplicas) {
      result.append(String.format("%4s", otherNode));
      if (!otherNode.equals(node)) {
        result.append(" = ");
        result.append(String.format("%d", nodeAverage(otherNode)));
        result.append(" : ");
        // not print out all the samples... just do it in array order 
        // maybe do it in time order someday if we want to be cute
        for (int j = 0; j < WINDOWSIZE; j++) {
          if (pingTable.get(otherNode, j) != GNSNodeConfig.INVALID_PING_LATENCY) {
            result.append(String.format("%3d", pingTable.get(otherNode, j)));
          } else {
            result.append("  X");
          }
          result.append(" ");
        }
      } else { // i == node
        // for this node just output something useful
        result.append("  {this node }                                   ");
      }
      result.append(nodeConfig.getNodeAddress(otherNode).getHostName());
      result.append(NEWLINE);
    }
    return result.toString();
  }

  @Override
  public void shutdown() {
    GNS.getLogger().warning("Ping shutting down .. ");
    this.managerThread.interrupt();
    if (this.pingServer != null) {
      this.pingServer.shutdown();
    }
    if (this.pingClient != null) {
      this.pingClient.shutdown();
    }
    GNS.getLogger().warning("Ping shutdown  complete.");
  }

  /**
   * The main routine.  For testing only.
   * 
   * @param args
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String args[]) throws Exception {
    String configFile = args[0];
    String nodeID = "0";
    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(configFile, nodeID);
    GNSConsistentNodeConfig nodeConfig = new GNSConsistentNodeConfig(gnsNodeConfig);
    new PingManager(nodeID, nodeConfig).startPinging();
  }

  /**
   * A newline.
   */
  public final static String NEWLINE = System.getProperty("line.separator");

}
