package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.paxos.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.paxos.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.util.Stringifiable;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Random;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Deprecated
public class FailureDetection<NodeIDType> {

  /**
   * Frequency of pinging a node
   */
  private int pingIntervalMillis = 10000;

  /**
   * Interval after which a node is declared as failed is no response is received
   */
  private int timeoutIntervalMillis = 30000;

  /**
   * ID of this node.
   */
  public NodeIDType nodeID;

  /**
   * Last time a message is received from this node.
   */
  public ConcurrentHashMap<NodeIDType, Long> nodeInfo = new ConcurrentHashMap<NodeIDType, Long>();

  /**
   * Current status (up or down) of all nodes.
   */
  private ConcurrentHashMap<NodeIDType, Boolean> nodeStatus = new ConcurrentHashMap<NodeIDType, Boolean>();

  /**
   * Current status (up or down) of all nodes.
   */
  private ReentrantLock lock = new ReentrantLock();

//	public DatagramSocket socket;


  /**
   * starting time of failure detector
   */
  long startingTime;

  ScheduledThreadPoolExecutor executorService;
  PaxosManager<NodeIDType> paxosManager;
  private boolean debugMode = true;

  /**
   * initialize the failure detection module
   *
   * @param nodeIDs set of nodes to monitor
   * @param nodeID  ID of this node
   */
  public FailureDetection(Set<NodeIDType> nodeIDs, NodeIDType nodeID, ScheduledThreadPoolExecutor executorService, PaxosManager<NodeIDType> paxosManager,
                          int pingIntervalMillis, int timeoutIntervalMillis) {
    this.nodeID = nodeID; 
//     this.N =  N;
    this.startingTime = System.currentTimeMillis();
    this.executorService = executorService;
    this.paxosManager = paxosManager;
    this.pingIntervalMillis = pingIntervalMillis;
    this.timeoutIntervalMillis = timeoutIntervalMillis;
    assert this.pingIntervalMillis * 3 >= this.timeoutIntervalMillis;
    GNS.getLogger().info("Failure Detector: Ping Interval: " + this.pingIntervalMillis +
            " Timeout: " + this.timeoutIntervalMillis);
    for (NodeIDType remoteNodeID : nodeIDs) {
      if (remoteNodeID.equals(nodeID)) {
        nodeStatus.put(nodeID, true);
        nodeInfo.put(nodeID, System.currentTimeMillis());
        continue;
      }
      startNodeMonitoring(remoteNodeID);
    }
    GNS.getLogger().info("Failure detection initialized for " + nodeIDs.size() + " nodes");
  }

  /**
   * starts monitoring the node with ID = nodeID.
   * if node is already being monitored, method makes no change.
   *
   * @param monitoredNodeID
   */
  void startNodeMonitoring(NodeIDType monitoredNodeID) {
    lock.lock();
    try {

      if (nodeInfo.containsKey(monitoredNodeID)) return;

      FailureDetectionPacket<NodeIDType> fail = new FailureDetectionPacket<NodeIDType>(nodeID, monitoredNodeID, false, PaxosPacketType.FAILURE_DETECT);
      Random r = new Random();
      try {
        FailureDetectionTask<NodeIDType> failureDetectionTask = new FailureDetectionTask<NodeIDType>(monitoredNodeID, fail.toJSONObject(), this);
        long initialDelay = timeoutIntervalMillis + r.nextInt(pingIntervalMillis);
        nodeInfo.put(monitoredNodeID, System.currentTimeMillis() + initialDelay);
        nodeStatus.put(monitoredNodeID, true);
        executorService.scheduleAtFixedRate(failureDetectionTask, initialDelay,
                pingIntervalMillis, TimeUnit.MILLISECONDS);
      } catch (JSONException e) {
        GNS.getLogger().severe("JSON EXCEPTION HERE !! " + e.getMessage());
        e.printStackTrace();
      }

      if (debugMode) GNS.getLogger().fine(nodeID + " started monitoring node " + monitoredNodeID);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Stop monitoring this node.
   *
   * @param nodeID
   * @param json
   * @param port
   */
  public void stopNodeMonitoring(int nodeID, JSONObject json, int port) {
    lock.lock();
    try {
      if (nodeInfo.containsKey(nodeID)) {
        nodeInfo.remove(nodeID);
        nodeStatus.remove(nodeID);
      }
    } finally {
      lock.unlock();
    }
  }


//	/**
//	 * handle an incoming message: either a failure detection query or a response of a query sent.
//	 * @param json
//	 */
//	public  void handleIncomingMessage(JSONObject json) {
//        PaxosManager.timer.schedule(new HandleFailureDetectionPacketTask(json),0);
////        NameServer.executorService.submit();
//
//	}

  /**
   * Update node info for the fdPacket.
   * //	 * @param fdPacket
   */
  void updateNodeInfo(NodeIDType responderNodeID) {

    // this condition should never be true.
//		if (fdPacket.senderNodeID != nodeID) return;

    lock.lock();
    try {
      if (nodeInfo.containsKey(responderNodeID)) {

        nodeInfo.put(responderNodeID, System.currentTimeMillis());
        if (debugMode) GNS.getLogger().finer(nodeID + "FD received response "
                + responderNodeID);
      }

    } finally {
      lock.unlock();
    }

  }

  void resetNodeInfo(NodeIDType responderNodeID) {

    // this condition should never be true.
//		if (fdPacket.senderNodeID != nodeID) return;

    lock.lock();
    try {
      if (nodeInfo.containsKey(responderNodeID)) {
        nodeInfo.put(responderNodeID, System.currentTimeMillis());
        if (debugMode) GNS.getLogger().finer(nodeID + "FD reset " + responderNodeID);
      }

    } finally {
      lock.unlock();
    }

  }

  /**
   * returns true if node = nodeID is up, false otherwise.
   *
   * @param nodeID ID of the node.
   * @return true if node = nodeID is up, false otherwise.
   */
  boolean isNodeUp(NodeIDType nodeID) {
    return nodeStatus.get(nodeID);
  }

  /**
   * if an active node fails, or a failed node comes up again.
   * notify local replica.
   *
   * @param monitoredNode
   */
  void notifyNodeOfStatusChange(NodeIDType monitoredNode) {
    FailureDetectionPacket<NodeIDType> fdPacket = null;
    lock.lock();
    try {
      if (nodeInfo.containsKey(monitoredNode)) {
        boolean status = nodeStatus.get(monitoredNode);
        long val = nodeInfo.get(monitoredNode);
        long delay = System.currentTimeMillis() - val;

        if (delay < timeoutIntervalMillis && status == false) {
          // case 1: node is up
          nodeStatus.put(monitoredNode, true);
          fdPacket = new FailureDetectionPacket<NodeIDType>(nodeID, monitoredNode,
                  true, PaxosPacketType.NODE_STATUS);
        } else if (delay > timeoutIntervalMillis && status == true) {
          // case 2: node is down
//					GNS.getLogger().severe(nodeID + "FD Node failed " + monitoredNode + "delay = " + delay);

          nodeStatus.put(monitoredNode, false);
          fdPacket = new FailureDetectionPacket<NodeIDType>(nodeID, monitoredNode,
                  false, PaxosPacketType.NODE_STATUS);
//                    resetNodeInfo(monitoredNode);
          GNS.getLogger().severe(nodeID + "\tFDNodeFailed\t" + nodeID + "\t" + monitoredNode + "\t" + delay + "\t" +
                  (System.currentTimeMillis() - startingTime) + "\t" + (System.currentTimeMillis() - nodeInfo.get(monitoredNode)) + "\t");

        }

      }
    } finally {
      lock.unlock();
    }

    if (fdPacket != null) { // status has changed, down to up or up to down.
      paxosManager.informNodeStatus(fdPacket);
    }
  }


  public boolean isDebugMode() {
    return debugMode;
  }
}


class FailureDetectionTask<NodeIDType> extends TimerTask {

  /**
   * JSONObject  is a FailureDetectionPacket object.
   */
  JSONObject json;

  /**
   * Send failure detection packet to the destination node ID.
   */
  NodeIDType destNodeID;

  FailureDetection<NodeIDType> failureDetection;

  /**
   * Constructor
   *
   * @param destNodeID which node to monitor
   * @param json       failure detection packet to send
   */
  public FailureDetectionTask(NodeIDType destNodeID, JSONObject json, FailureDetection<NodeIDType> failureDetection) {
    this.destNodeID = destNodeID;
    this.json = json;
    this.failureDetection = failureDetection;
    if (failureDetection.isDebugMode())
      GNS.getLogger().fine(failureDetection.nodeID + " Started FailureDetectionTask for "
              + destNodeID);
  }


  @Override
  public void run() {
    try {
      // send a FD packet
      if (failureDetection.nodeInfo.containsKey(destNodeID)) {
        GNS.getLogger().finer(failureDetection.nodeID + "FD sent request " + destNodeID);
        failureDetection.paxosManager.sendMessage(destNodeID, json, null);
      } else {
        GNS.getLogger().severe("Failure Detection: Canceling Timer Task. " + this.destNodeID);
        cancel();
        return;
      }

      // check if node has failed, or come up.
      failureDetection.notifyNodeOfStatusChange(destNodeID);
    } catch (Exception e) {
      GNS.getLogger().severe("Exception in failure detection ... " + e.getMessage());
      e.printStackTrace();
    }
  }
}


class HandleFailureDetectionPacketTask<NodeIDType> extends TimerTask {

  FailureDetectionPacket<NodeIDType> fdPacket;

  FailureDetection<NodeIDType> failureDetection;
  
  

  HandleFailureDetectionPacketTask(JSONObject json, FailureDetection<NodeIDType> failureDetection, Stringifiable<NodeIDType> unstringer) {
    this.failureDetection = failureDetection;
    try {
      fdPacket = new FailureDetectionPacket<NodeIDType>(json, unstringer);
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception " + e.getMessage());
    }
  }

  @Override
  public void run() {
    try {
      if (failureDetection == null) return; // this case can happen if a packet arrives before we have
      // initialized failure detection
      if (fdPacket != null && fdPacket.packetType == PaxosPacketType.FAILURE_DETECT.getInt()) {

        FailureDetectionPacket<NodeIDType> fdResponse = fdPacket.getFailureDetectionResponse();
        failureDetection.resetNodeInfo(fdPacket.senderNodeID);
        try {
          GNS.getLogger().finer(failureDetection.nodeID + "FD sent response to " + fdPacket.senderNodeID);
          failureDetection.paxosManager.sendMessage(fdPacket.senderNodeID, fdResponse.toJSONObject(), null);
        } catch (JSONException e) {
          GNS.getLogger().severe("JSON Exception " + e.getMessage());
        }
      } else if (fdPacket != null && fdPacket.packetType == PaxosPacketType.FAILURE_RESPONSE.getInt()) {
        GNS.getLogger().finer(failureDetection.nodeID + "FD recvd response from " + fdPacket.responderNodeID);
        failureDetection.updateNodeInfo(fdPacket.responderNodeID);
      }

    } catch (Exception e) {
      GNS.getLogger().severe("Exception in failure detection packet task. " + fdPacket);
      e.printStackTrace();
    }
  }
}