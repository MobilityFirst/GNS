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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class FailureDetection extends Thread{
	
	/**
	 * Frequency of pinging a node.
	 */
	static int pingInterval = 10000;

	static int timeoutInterval = 31000;

	/**
	 * number of nodes.
	 */
	static int N;
	
	/**
	 * ID of this node.
	 */
	static int nodeID;

	/**
	 * Last time a message is received from this node.
	 */
	static ConcurrentHashMap<Integer,Long> nodeInfo = new ConcurrentHashMap<Integer, Long>();
	
	/**
	 * Current status (up or down) of all nodes.
	 */
	static ConcurrentHashMap<Integer,Boolean> nodeStatus = new ConcurrentHashMap<Integer, Boolean>();

	/**
	 * 
	 */
	static ReentrantLock lock = new ReentrantLock();

//	public DatagramSocket socket;

	/**
	 * Timer object to schedule failure detection messages.
	 */


	/**
	 * starting time of failure detector
	 */
	static long startingTime;

	/**
	 * initialize the failure detection module
	 * @param N number of nodes
	 * @param nodeID ID of this node 
	 */
	static void initializeFailureDetection(int N, int nodeID) {
		FailureDetection.nodeID = nodeID;
		FailureDetection.N =  N;
		FailureDetection.startingTime = System.currentTimeMillis();
				
		for (int i = 0; i < N; i++) {
			if (i == nodeID) {
				nodeStatus.put(nodeID, true);
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
	static void startNodeMonitoring(int monitoredNodeID){
		lock.lock();
		try {
			if (nodeInfo.containsKey(monitoredNodeID)) return;
			nodeInfo.put(monitoredNodeID, System.currentTimeMillis());
			nodeStatus.put(monitoredNodeID, true);

		} finally {
			lock.unlock();
		}

		FailureDetectionPacket fail = new FailureDetectionPacket(nodeID, monitoredNodeID, false, PaxosPacketType.FAILURE_DETECT);
        Random r = new Random();
		try
		{
            FailureDetectionTask failureDetectionTask = new FailureDetectionTask(monitoredNodeID, fail.toJSONObject());
			PaxosManager.executorService.scheduleAtFixedRate(failureDetectionTask, r.nextInt(pingInterval),
                    pingInterval, TimeUnit.MILLISECONDS);
		} catch (JSONException e)
		{

			if (StartNameServer.debugMode) GNS.getLogger().severe(" JSON EXCEPTION HERE !! " + e.getMessage());
			e.printStackTrace();
		}
		if (StartNameServer.debugMode) GNS.getLogger().fine(nodeID + " started monitoring node " + monitoredNodeID);
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
//	public static void handleIncomingMessage(JSONObject json) {
//        PaxosManager.timer.schedule(new HandleFailureDetectionPacketTask(json),0);
////        NameServer.executorService.submit();
//
//	}

	/**
	 * Update node info for the fdPacket.
//	 * @param fdPacket
	 */
	static void updateNodeInfo(int responderNodeID) {
		
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

    static void resetNodeInfo(int responderNodeID) {

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
	static boolean isNodeUp(int nodeID) {
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
	 static void notifyNodeOfStatusChange(int monitoredNode) {
		
		FailureDetectionPacket fdPacket = null;
		lock.lock();
		try{
			if (nodeInfo.containsKey(monitoredNode)) {
				boolean status = nodeStatus.get(monitoredNode);
				long val = nodeInfo.get(monitoredNode);
				long delay = System.currentTimeMillis() - val;
//				if (StartNameServer.debugMode) GNRS.getLogger().finer(nodeID + " status " + status + " delay " + delay);
				
				if (delay < pingInterval && status == false) {
					// case 1: node is up
					nodeStatus.put(monitoredNode, true);
					fdPacket = new FailureDetectionPacket(nodeID, monitoredNode, 
									true, PaxosPacketType.NODE_STATUS);
				}
				else if (delay > timeoutInterval && status == true) {
					// case 2: node is down
					if (StartNameServer.debugMode) GNS.getLogger().severe(nodeID +
                            "FD Node failed " + monitoredNode + "delay = " + delay);
					nodeStatus.put(monitoredNode, false);
					fdPacket = new FailureDetectionPacket(nodeID, monitoredNode, 
							false, PaxosPacketType.NODE_STATUS);
                    resetNodeInfo(monitoredNode);

				}

			}
		} finally {
			lock.unlock();
		}

		if (fdPacket != null) { // status has changed, down to up or up to down.
			PaxosManager.informNodeStatus(fdPacket);
		}
	}


//
//	/**
//	 * send message to the node  destReplicaID 
//	 * @param destReplicaID
//	 * @param json
//	 */
//	public void sendMessage(int destReplicaID, JSONObject json,  int portNumber) {
//		try
//		{
//			byte[] buffer = json.toString().getBytes();
//			DatagramPacket packet = new DatagramPacket(buffer, buffer.length, 
//					InetAddress.getLocalHost(), portNumber);
//			socket.send(packet);
//		} catch (UnknownHostException e)
//		{
//			// Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e)
//		{
//			// Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//	
	
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

	/**
	 * Constructor
	 * @param destNodeID which node to monitor
	 * @param json failure detection packet to send
	 */
	public FailureDetectionTask(int destNodeID, JSONObject json) {
		this.destNodeID = destNodeID;
		this.json = json;
		if (StartNameServer.debugMode) GNS.getLogger().fine(FailureDetection.nodeID + " Started FailureDetectionTask for "  + destNodeID);
	}
	
	
	@Override
	public void run() {
		// send a FD packet
		if (FailureDetection.nodeInfo.containsKey(destNodeID)) {
			if (StartNameServer.debugMode) GNS.getLogger().finer(FailureDetection.nodeID + "FD sent request " + destNodeID);
			PaxosManager.sendMessage(destNodeID, json);
		}
		else {
			if (StartNameServer.debugMode) GNS.getLogger().severe("Failure Detection: Canceling Timer Task. " + this.destNodeID);
			cancel();
			return;
		}
		
		// check if node has failed, or come up.
		FailureDetection.notifyNodeOfStatusChange(destNodeID);
	}



}


class HandleFailureDetectionPacketTask extends TimerTask{

    FailureDetectionPacket fdPacket;

    HandleFailureDetectionPacketTask(JSONObject json) {

        try {
            fdPacket = new FailureDetectionPacket(json);
        } catch (JSONException e) {
            if (StartNameServer.debugMode) GNS.getLogger().severe("JSON Exception " + e.getMessage());

        }

    }

    @Override
    public void run() {


        if (fdPacket!= null && fdPacket.packetType == PaxosPacketType.FAILURE_DETECT) {

            FailureDetectionPacket fdResponse = fdPacket.getFailureDetectionResponse();
            FailureDetection.resetNodeInfo(fdPacket.senderNodeID);
            try
            {
                if(StartNameServer.debugMode)
                    GNS.getLogger().finer(FailureDetection.nodeID + "FD sent response " + fdPacket.senderNodeID);
                PaxosManager.sendMessage(fdPacket.senderNodeID, fdResponse.toJSONObject());
            } catch (JSONException e)
            {
                if (StartNameServer.debugMode) GNS.getLogger().severe("JSON Exception " + e.getMessage());
            }
        }
        else if (fdPacket!= null && fdPacket.packetType == PaxosPacketType.FAILURE_RESPONSE) {
            FailureDetection.updateNodeInfo(fdPacket.responderNodeID);
        }


    }
}