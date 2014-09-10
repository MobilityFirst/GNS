package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.paxos.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.paxos.paxospacket.PaxosPacketType;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Random;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class FailureDetection {

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
  public NodeId<String> nodeID;

  /**
   * Last time a message is received from this node.
   */
  public ConcurrentHashMap<NodeId<String>, Long> nodeInfo = new ConcurrentHashMap<NodeId<String>, Long>();

  /**
   * Current status (up or down) of all nodes.
   */
  private ConcurrentHashMap<NodeId<String>, Boolean> nodeStatus = new ConcurrentHashMap<NodeId<String>, Boolean>();

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
  PaxosManager paxosManager;
  private boolean debugMode = true;

  /**
   * initialize the failure detection module
   *
   * @param nodeIDs set of nodes to monitor
   * @param nodeID  ID of this node
   */
  public FailureDetection(Set<NodeId<String>> nodeIDs, NodeId<String> nodeID, ScheduledThreadPoolExecutor executorService, PaxosManager paxosManager,
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
    for (NodeId<String> remoteNodeID : nodeIDs) {
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
  void startNodeMonitoring(NodeId<String> monitoredNodeID) {
    lock.lock();
    try {

      if (nodeInfo.containsKey(monitoredNodeID)) return;

      FailureDetectionPacket fail = new FailureDetectionPacket(nodeID, monitoredNodeID, false, PaxosPacketType.FAILURE_DETECT);
      Random r = new Random();
      try {
        FailureDetectionTask failureDetectionTask = new FailureDetectionTask(monitoredNodeID, fail.toJSONObject(), this);
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
  void updateNodeInfo(NodeId<String> responderNodeID) {

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

  void resetNodeInfo(NodeId<String> responderNodeID) {

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
  boolean isNodeUp(NodeId<String> nodeID) {
//    return true;
    return nodeStatus.get(nodeID);
  }


//	/**
//	 * Receives FailureDetectionPacket and responds to the sender.
//	 */
//	public void run() {
//		
//		while (true) {
//			
////			if (failure == false && this.nodeID == Network.N && System.currentTimeMillis() - startingTime > 30000) {
////				System.out.println(this.nodeID + " IMP: failure detection quitting.");
////				try
////				{
////					Thread.sleep(20000);
////				} catch (InterruptedException e)
////				{
////					// Auto-generated catch block
////					e.printStackTrace();
////				}
////				failure = true;
////			}
//			
//			byte[] buf = new byte[Network.MAX_PACKET_SIZE];
//			DatagramPacket packet = new DatagramPacket(buf, buf.length);
//			try {
//				socket.receive(packet);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//			byte[] payload = packet.getData();			//Packet's payload
//			FailureDetectionPacket fdPacket;
//			try {
//				JSONObject json = new JSONObject(new String(payload));
//				fdPacket = new FailureDetectionPacket(json);
//			} catch (JSONException e) {
//				// Auto-generated catch block
//				e.printStackTrace();
//				continue;
//			}
//			
//			if (fdPacket.packetType == PacketType.FAILURE_DETECT) {
//				// send response
//				FailureDetectionPacket fdResponse = fdPacket.getFailureDetectionResponse();
//				try
//				{
//					sendMessage(fdPacket.senderNodeID, fdResponse.toJSONObject());//, 
////							Network.getFailureMonitoringPortNumber(fdPacket.senderNodeID));
//				} catch (JSONException e)
//				{
//					// Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//			else if (fdPacket.packetType == PacketType.FAILURE_RESPONSE) {
//				updateNodeInfo(fdPacket);
//			}
//			
//		}
//	}


  /**
   * if an active node fails, or a failed node comes up again.
   * notify local replica.
   *
   * @param monitoredNode
   */
  void notifyNodeOfStatusChange(NodeId<String> monitoredNode) {
//		 if (monitoredNode == 0) GNS.getLogger().severe("FDEnter ... ");
    FailureDetectionPacket fdPacket = null;
    lock.lock();
    try {
      if (nodeInfo.containsKey(monitoredNode)) {
        boolean status = nodeStatus.get(monitoredNode);
        long val = nodeInfo.get(monitoredNode);
        long delay = System.currentTimeMillis() - val;
//				if (monitoredNode == 0) GNS.getLogger().fine(nodeID + " status " + status + " delay " + delay);

        if (delay < timeoutIntervalMillis && status == false) {
          // case 1: node is up
          nodeStatus.put(monitoredNode, true);
          fdPacket = new FailureDetectionPacket(nodeID, monitoredNode,
                  true, PaxosPacketType.NODE_STATUS);
        } else if (delay > timeoutIntervalMillis && status == true) {
          // case 2: node is down
//					GNS.getLogger().severe(nodeID + "FD Node failed " + monitoredNode + "delay = " + delay);

          nodeStatus.put(monitoredNode, false);
          fdPacket = new FailureDetectionPacket(nodeID, monitoredNode,
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


class FailureDetectionTask extends TimerTask {

  /**
   * JSONObject  is a FailureDetectionPacket object.
   */
  JSONObject json;

  /**
   * Send failure detection packet to the destination node ID.
   */
  NodeId<String> destNodeID;

  FailureDetection failureDetection;

  /**
   * Constructor
   *
   * @param destNodeID which node to monitor
   * @param json       failure detection packet to send
   */
  public FailureDetectionTask(NodeId<String> destNodeID, JSONObject json, FailureDetection failureDetection) {
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


class HandleFailureDetectionPacketTask extends TimerTask {

  FailureDetectionPacket fdPacket;

  FailureDetection failureDetection;

  HandleFailureDetectionPacketTask(JSONObject json, FailureDetection failureDetection) {
    this.failureDetection = failureDetection;
    try {
      fdPacket = new FailureDetectionPacket(json);
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

        FailureDetectionPacket fdResponse = fdPacket.getFailureDetectionResponse();
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