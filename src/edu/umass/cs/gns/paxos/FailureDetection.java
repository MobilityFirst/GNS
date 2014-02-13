package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

  public class FailureDetection extends Thread{
	
	/**
	 * Frequency of pinging a node.
	 */
	 int pingIntervalMillis = 10000;

  /**
   * Interval after which a node is declared as failed is no response is received
   */
	 int timeoutIntervalMillis = 31000;

	/**
	 * number of nodes.
	 */
	 int N;
	
	/**
	 * ID of this node.
	 */
	 int nodeID;

	/**
	 * Last time a message is received from this node.
	 */
	 ConcurrentHashMap<Integer,Long> nodeInfo = new ConcurrentHashMap<Integer, Long>();
	
	/**
	 * Current status (up or down) of all nodes.
	 */
	 ConcurrentHashMap<Integer,Boolean> nodeStatus = new ConcurrentHashMap<Integer, Boolean>();

	/**
	 * 
	 */
	 ReentrantLock lock = new ReentrantLock();

//	public DatagramSocket socket;

	/**
	 * Timer object to schedule failure detection messages.
	 */


	/**
	 * starting time of failure detector
	 */
	 long startingTime;

     ScheduledThreadPoolExecutor executorService;
     PaxosManager paxosManager;

	/**
	 * initialize the failure detection module
	 * @param N number of nodes
	 * @param nodeID ID of this node 
	 */
	 public FailureDetection(int N, int nodeID, ScheduledThreadPoolExecutor executorService,
                                         PaxosManager paxosManager, int pingIntervalMillis, int timeoutIntervalMillis) {
		 this.nodeID = nodeID;
     this.N =  N;
     this.startingTime = System.currentTimeMillis();
     this.executorService = executorService;
     this.paxosManager = paxosManager;
     this.pingIntervalMillis = pingIntervalMillis;
     this.timeoutIntervalMillis = timeoutIntervalMillis;
		for (int i = 0; i < N; i++) {
			if (i == nodeID) {
				nodeStatus.put(nodeID, true);
        nodeInfo.put(nodeID, System.currentTimeMillis());
				continue;
			}
			startNodeMonitoring(i);
		}
		if (StartNameServer.debugMode) GNS.getLogger().fine("Failure detection initialized for " + N + " nodes");
	}
	
	/**
	 * starts monitoring the node with ID = nodeID. 
	 * if node is already being monitored, method makes no change.
	 * @param monitoredNodeID
	 */
	 void startNodeMonitoring(int monitoredNodeID){
    lock.lock();
		try {

			if (nodeInfo.containsKey(monitoredNodeID)) return;

      FailureDetectionPacket fail = new FailureDetectionPacket(nodeID, monitoredNodeID, false, PaxosPacketType.FAILURE_DETECT);
      Random r = new Random();
      try
      {
        FailureDetectionTask failureDetectionTask = new FailureDetectionTask(monitoredNodeID, fail.toJSONObject(), this);
        long initialDelay = timeoutIntervalMillis + r.nextInt(pingIntervalMillis);
//        if (StartNameServer.experimentMode) {
//          initialDelay += 2 * NameServer.initialExpDelayMillis; // wait for all name servers to start up.
//        }
        nodeInfo.put(monitoredNodeID, System.currentTimeMillis() + initialDelay);
        nodeStatus.put(monitoredNodeID, true);
        executorService.scheduleAtFixedRate(failureDetectionTask, initialDelay,
                pingIntervalMillis, TimeUnit.MILLISECONDS);
      } catch (JSONException e)
      {
        GNS.getLogger().severe("JSON EXCEPTION HERE !! " + e.getMessage());
        e.printStackTrace();
      }
      if (StartNameServer.debugMode) GNS.getLogger().fine(nodeID + " started monitoring node " + monitoredNodeID);
    } finally {
      lock.unlock();
    }
	}

	/**
	 * Stop monitoring this node.
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
//	 * @param fdPacket
	 */
	 void updateNodeInfo(int responderNodeID) {
		
		// this condition should never be true.
//		if (fdPacket.senderNodeID != nodeID) return;
		
		lock.lock();
		try{
			if (nodeInfo.containsKey(responderNodeID)) {

				nodeInfo.put(responderNodeID, System.currentTimeMillis());
				if (StartNameServer.debugMode) GNS.getLogger().finer(nodeID + "FD received response "
                        + responderNodeID);
			}

		} finally {
			lock.unlock();
		}

	}

     void resetNodeInfo(int responderNodeID) {

        // this condition should never be true.
//		if (fdPacket.senderNodeID != nodeID) return;

        lock.lock();
        try{
            if (nodeInfo.containsKey(responderNodeID)) {
                nodeInfo.put(responderNodeID, System.currentTimeMillis());
                if (StartNameServer.debugMode) GNS.getLogger().finer(nodeID + "FD reset " + responderNodeID);
            }

        } finally {
            lock.unlock();
        }

    }

	/**
	 * returns true if node = nodeID is up, false otherwise.  
	 * @param nodeID ID of the node.
	 * @return true if node = nodeID is up, false otherwise.
	 */
	 boolean isNodeUp(int nodeID) {
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
	 * @param monitoredNode
	 */
	  void notifyNodeOfStatusChange(int monitoredNode) {
//		 if (monitoredNode == 0) GNS.getLogger().severe("FDEnter ... ");
		FailureDetectionPacket fdPacket = null;
		lock.lock();
		try{
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
				}
				else if (delay > timeoutIntervalMillis && status == true) {
					// case 2: node is down
//					GNS.getLogger().severe(nodeID + "FD Node failed " + monitoredNode + "delay = " + delay);

          nodeStatus.put(monitoredNode, false);
					fdPacket = new FailureDetectionPacket(nodeID, monitoredNode, 
							false, PaxosPacketType.NODE_STATUS);
//                    resetNodeInfo(monitoredNode);
          GNS.getLogger().severe(nodeID + "\tFDNodeFailed\t" + nodeID + "\t"  + monitoredNode + "\t" + delay + "\t" +
                  (System.currentTimeMillis() - startingTime) + "\t" + (System.currentTimeMillis() - nodeInfo.get(monitoredNode))  + "\t");

        }

			}
		} finally {
			lock.unlock();
		}

		if (fdPacket != null) { // status has changed, down to up or up to down.
      paxosManager.informNodeStatus(fdPacket);
		}
	}

	
}



class FailureDetectionTask extends TimerTask{

	/**
	 * JSONObject  is a FailureDetectionPacket object. 
	 */
	JSONObject json;
	
	/**
	 * Send failure detection packet to the destination node ID.
	 */
	int destNodeID;

  FailureDetection failureDetection;

	/**
	 * Constructor
	 * @param destNodeID which node to monitor
	 * @param json failure detection packet to send
	 */
	public FailureDetectionTask(int destNodeID, JSONObject json, FailureDetection failureDetection) {
		this.destNodeID = destNodeID;
		this.json = json;
    this.failureDetection = failureDetection;
		if (StartNameServer.debugMode) GNS.getLogger().fine(failureDetection.nodeID + " Started FailureDetectionTask for "
            + destNodeID);
	}
	
	
	@Override
	public void run() {
    try{
		// send a FD packet
		if (failureDetection.nodeInfo.containsKey(destNodeID)) {
			if (StartNameServer.debugMode && destNodeID == 0) GNS.getLogger().fine(failureDetection.nodeID
              + "FD sent request " + destNodeID);
      failureDetection.paxosManager.sendMessage(destNodeID, json, null);
		}
		else {
			GNS.getLogger().severe("Failure Detection: Canceling Timer Task. " + this.destNodeID);
			cancel();
			return;
		}
		
		// check if node has failed, or come up.
		failureDetection.notifyNodeOfStatusChange(destNodeID);
    }catch (Exception e) {
      GNS.getLogger().severe("Exception in failure detection ... " + e.getMessage());
      e.printStackTrace();
    }
	}
}


class HandleFailureDetectionPacketTask extends TimerTask{

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

        if (fdPacket!= null && fdPacket.packetType == PaxosPacketType.FAILURE_DETECT) {

            FailureDetectionPacket fdResponse = fdPacket.getFailureDetectionResponse();
            failureDetection.resetNodeInfo(fdPacket.senderNodeID);
            try
            {
                if(StartNameServer.debugMode)
                    GNS.getLogger().finer(failureDetection.nodeID + "FD sent response " + fdPacket.senderNodeID);
              failureDetection.paxosManager.sendMessage(fdPacket.senderNodeID, fdResponse.toJSONObject(), null);
            } catch (JSONException e)
            {
                GNS.getLogger().severe("JSON Exception " + e.getMessage());
            }
        }
        else if (fdPacket!= null && fdPacket.packetType == PaxosPacketType.FAILURE_RESPONSE) {
          failureDetection.updateNodeInfo(fdPacket.responderNodeID);
        }

      } catch (Exception e) {
        GNS.getLogger().severe("Exception in failure detection packet task. " + fdPacket);
        e.printStackTrace();
      }
    }
}