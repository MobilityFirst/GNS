package paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.GNS.PortType;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import nio.ByteStreamToJSONObjects;
import nio.NioServer2;
import nio.PacketDemultiplexer;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Manages a set of Paxos instances, each instance could be running 
 * on a different susbset of nodes but including this node.  
 * @author abhigyan
 *
 */
public class PaxosManager extends Thread{

	public static final String PAXOS_ID = "PXS";
	
	private final static String REQUEST_ID = "RQID";

//    public static final int timeoutForRequestState = 1000;

	/**
	 *  total number of nodes. node IDs = 0, 1, ..., N -1
	 */
	static int N;
	
	/**
	 * nodeID of this node. among node IDs = 0, 1, ..., (N - 1)
	 */
	static int nodeID;

    /**
     * When paxos is run independently {@code tcpTransport} is used to send messages between paxos replicas and client.
     */
    public static NioServer2 tcpTransport;

	/**
	 * a hash table of where K = paxos ID, V = Paxos replica   
	 */
	static ConcurrentHashMap<String, PaxosReplica> paxosInstances = new ConcurrentHashMap<String, PaxosReplica>();

    public static PaxosClientRequestHandler clientRequestHandler;

    public static ScheduledThreadPoolExecutor executorService;

    static int maxThreads = 5;
	/**
	 * debug = true is used to debug the paxos module,  debug = false when complete GNRS system is running.
	 */
	static boolean debug;

    /**
     * Minimum interval (in milliseconds) between two garbage state collections by this replica.
     */
    public static int GARBAGE_COLLECTION_INTERVAL = 100;

    /**
     * Paxos ID of the paxos instance created for testing/debugging.
     */
    public static String defaultPaxosID  = "0";


    public static void initalizePaxosManager(int N, int nodeID, PaxosClientRequestHandler outputHandler, ScheduledThreadPoolExecutor executorService) {

        PaxosManager.N = N;
        PaxosManager.nodeID = nodeID;
        PaxosManager.debug = false;

        // recover previous state if exists using logger
//        ConcurrentHashMap<String, PaxosReplica> paxosInstances = PaxosLogger.initializePaxosLogger();
        PaxosLogger2.initLogger();

        PaxosManager.clientRequestHandler = outputHandler;
        PaxosManager.executorService = executorService;
        FailureDetection.initializeFailureDetection(N, nodeID);

        if (paxosInstances!=null) {
            PaxosManager.paxosInstances = paxosInstances;
            for (String x: PaxosManager.paxosInstances.keySet()) {
                if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos Recovery: starting paxos replica .. " + x);
                PaxosManager.paxosInstances.get(x).startReplica();
            }
        }

        ResendPendingMessagesTask task = new ResendPendingMessagesTask();
        executorService.scheduleAtFixedRate(task,1000,1000, TimeUnit.MILLISECONDS);
        if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos manager initialization complete");

    }

    /**
     *
     */
    private static void sendPaxosInstanceListToOtherNodes(ConcurrentHashMap<String, PaxosReplica> paxosInstances) {
        for (int i = 0; i < N; i++) {
            Set<String> paxosIDsActive = new HashSet<String>();
            Set<String> paxosIDsDeleted = new HashSet<String>();

            for (String x: paxosInstances.keySet()) {
                if (!paxosInstances.get(x).getNodeIDs().contains(i)) continue;
                if (paxosInstances.get(x).isStopped()) paxosIDsDeleted.add(x);
                else paxosIDsActive.add(x);
            }
            // send paxos IDs to node i
        }

    }

    private static void prepareListOfPaxosInstances(int senderNodeID, Set<String> paxosIDsActiveAtSendingNode,
                                                    Set<String> paxosIDsStoppedAtSendingNode) {
        synchronized (paxosInstances) {
            HashMap<String,Set<Integer>> paxosInstancesAdded = new HashMap<String, Set<Integer>>();
            HashMap<String,Set<Integer>> paxosInstancesStopped = new HashMap<String,Set<Integer>>();
            HashMap<String,RequestPacket> stopRequests = new HashMap<String,RequestPacket>();

            for (String paxosID: paxosInstances.keySet()) {
                //
                if (!paxosInstances.get(paxosID).isNodeInPaxosInstance(senderNodeID)) continue;

                PaxosReplica paxosReplica = paxosInstances.get(paxosID);
                if (paxosReplica.isStopped()) {
                    if (!paxosIDsStoppedAtSendingNode.contains(paxosID)) {
                        paxosInstancesStopped.put(paxosID, paxosReplica.getNodeIDs());
                        stopRequests.put(paxosID,paxosReplica.getLastRequest());
                    }
                }
                else {
                    if (!paxosIDsActiveAtSendingNode.contains(paxosID))
                        paxosInstancesAdded.put(paxosID,paxosReplica.getNodeIDs());
                }

            }
        }

    }

    /**
     * Handle newPaxosInstancesAdded(ConcurrentHashMap<String, P)
     * @param paxosInstancesAdded
     */
    private static void handlePaxosInstanceSetAdded(ConcurrentHashMap<String, Set<Integer>> paxosInstancesAdded) {
        synchronized (paxosInstances) {
            for (String paxosID: paxosInstancesAdded.keySet()) {
                if (paxosInstances.containsKey(paxosID)) {
                    if (paxosInstances.get(paxosID).isStopped()) {
                        // paxos ID is stopped, continue
                        continue;
                    }
                    else {
                        paxosInstances.get(paxosID).startReplica();
                    }
                }
                else {
                    PaxosReplica paxosReplica = new PaxosReplica(paxosID,nodeID,paxosInstancesAdded.get(nodeID));
                    paxosInstances.put(paxosID,paxosReplica);
//                    PaxosLogger.logPaxosStart(paxosID, paxosInstancesAdded.get(nodeID));
                    if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tStarting replica");
                    paxosReplica.startReplica();
                }
            }
        }
    }

    /**
     * Handle deletion of these paxos instances
     * @param paxosInstancesStopped
     */
    private static void handlePaxosInstanceSetStopped(ConcurrentHashMap<String, Set<Integer>> paxosInstancesStopped,
                                                     ConcurrentHashMap<String, RequestPacket> paxosInstaceStopRequests) {
        synchronized (paxosInstances) {
            for (String paxosID: paxosInstancesStopped.keySet()) {
                // paxos ID is already stopped, continue
                if (paxosInstances.containsKey(paxosID) && paxosInstances.get(paxosID).isStopped())
                    continue;
                PaxosReplica paxosReplica = new PaxosReplica(paxosID,nodeID,paxosInstancesStopped.get(paxosID),true, paxosInstaceStopRequests.get(paxosID));
                paxosInstances.put(paxosID, paxosReplica);
//                PaxosLogger.logPaxosStop(paxosID, paxosInstaceStopRequests.get(paxosID));
                clientRequestHandler.forwardDecisionToClient(paxosID, paxosInstaceStopRequests.get(paxosID));
            }
        }
    }


    public static void initalizePaxosManagerDebugMode(String nodeConfig, String testConfig, int nodeID, PaxosClientRequestHandler outputHandler) {

        PaxosManager.nodeID = nodeID;

        readTestConfigFile(testConfig);

        PaxosManager.clientRequestHandler = outputHandler;
        debug = true;

//        ConcurrentHashMap<String, PaxosReplica> paxosInstances = PaxosLogger.initializePaxosLogger();
        PaxosLogger2.initLogger();

        // initialize executor service
        initExecutor();

        // initialize transport
        initTransport(nodeConfig);

        if (paxosInstances!=null) {
            PaxosManager.paxosInstances = paxosInstances;
            for (String x: PaxosManager.paxosInstances.keySet()) {
                if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos Recovery: starting paxos replica .. " + x);
                PaxosManager.paxosInstances.get(x).startReplica();
            }
        }

        FailureDetection.initializeFailureDetection(N, nodeID);
        // create a paxos instance for debugging
        createDefaultPaxosInstance();

        ResendPendingMessagesTask task = new ResendPendingMessagesTask();
        executorService.scheduleAtFixedRate(task,1000,1000, TimeUnit.MILLISECONDS);
    }

    /**
     * read config file during testing/debugging
     * @param testConfig
     */
    private static void readTestConfigFile(String testConfig) {
        File f = new File(testConfig);
        if (!f.exists()) {
            if (StartNameServer.debugMode) GNS.getLogger().fine(" testConfig file does not exist. Quit. " +
                    "Filename =  " + testConfig);
            System.exit(2);
        }

        try {
            BufferedReader br = new BufferedReader(new FileReader(f));
            while (true) {
                String line = br.readLine();
                if (line == null) break;
                String[] tokens = line.trim().split("\\s+");
                if (tokens.length != 2) continue;
                if (tokens[0].equals("NumberOfReplicas")) N = Integer.parseInt(tokens[1]);
                else if (tokens[0].equals("EnableLogging")) StartNameServer.debugMode = Boolean.parseBoolean(tokens[1]);
                else if (tokens[0].equals("MaxThreads")) maxThreads = Integer.parseInt(tokens[1]);
                else if (tokens[0].equals("GarbageCollectionInterval")) {
                    GARBAGE_COLLECTION_INTERVAL = Integer.parseInt(tokens[1]);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    /**
     * Create a paxos instance for testing/debugging.
     */
    private static void createDefaultPaxosInstance() {
        if (paxosInstances.containsKey(defaultPaxosID)) {
            if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance " + defaultPaxosID + " already exists.");
            return;
        }
        // create a default paxos instance for testing.
        Set<Integer> x = new HashSet<Integer>();
        for (int i = 0;  i < N; i++)
            x.add(i);
        createPaxosInstance(defaultPaxosID, x);


    }

    /**
     * initialize executor service during Paxos debugging/testing
     */
    private static void initExecutor() {

        executorService = new ScheduledThreadPoolExecutor(maxThreads);
    }

    /**
     * initialize transport object during Paxos debugging/testing
     * @param configFile config file containing list of node ID, IP, port
     */
    private static void initTransport(String configFile) {

        // create the worker object
        PaxosPacketDemultiplexer paxosDemux = new PaxosPacketDemultiplexer();
        ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(paxosDemux);

        // earlier worker was running as a separate thread
//        new Thread(worker).start();

        // start TCP transport thread
        try {
            tcpTransport = new NioServer2(nodeID, worker, new PaxosNodeConfig(configFile));
            if (StartNameServer.debugMode) GNS.getLogger().fine(" TRANSPORT OBJECT CREATED ... " );
            new Thread(tcpTransport).start();
        } catch (IOException e) {
            GNS.getLogger().severe(" Could not initialize TCP socket at client");
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public static void resetAll() {
        // delete paxos instances
        paxosInstances.clear();
        // clear paxos logs
        PaxosLogger.clearLogs();
        // run gc
        System.gc();
    }

	/**
	 * Adds a new Paxos instance to the set of actives.
	 */
	public static boolean createPaxosInstance(String paxosID, Set<Integer> nodeIDs) {

        if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tEnter createPaxos");
		if (nodeIDs.size() < 3) {
			if (StartNameServer.debugMode) GNS.getLogger().severe(nodeID + " less than three replicas " +
					"paxos instance cannot be created. SEVERE ERROR. EXCEPTION.");
			return false;
		}
		
		if (!nodeIDs.contains(nodeID)) {
			if (StartNameServer.debugMode) GNS.getLogger().severe(nodeID + " this node not a member of paxos instance. replica not created.");
			return false;
		}

        PaxosReplica r;

        synchronized (paxosInstances)
		{
            if (paxosInstances.containsKey(paxosID)) {
                if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance already exists. Paxos ID = " + paxosID);
                return false;
            }
            if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tBefore creating replica.");

            r = new PaxosReplica(paxosID, nodeID, nodeIDs);

//            paxosOccupancy.put(paxosID, 1);

            // paxosInstance object can be concurrently modified.
            paxosInstances.put(paxosID, r);
            if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance created. Paxos ID = " + paxosID);
		}
        PaxosLogger.logPaxosStart(paxosID, nodeIDs);
        if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tStarting replica");
        if (r!= null) r.startReplica();

        if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tExit create Paxos");
        return true;
	}

//	/**
//	 * Delete state corresponding to a Paxos instance.
//	 */
//	public static void deletePaxosInstance(String paxosID) {
//
//        PaxosReplica r = paxosInstances.remove(paxosID);
//
////        synchronized (paxosMessages) {
////            paxosMessages.remove(paxosID);
////            paxosOccupancy.remove(paxosID);
////
////        }
//		if (r != null) {
//			r.deleteState();
//			if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS INSTANCE DELETED. PaxosID = " + paxosID);
//		}
//		else {
//			if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS INSTANCE DOES NOT EXIST, DELETED ALREADY. PaxosID = " + paxosID);
//		}
//	}

    public static boolean doesPaxosInstanceExist(String paxosID) {
        return paxosInstances.containsKey(paxosID);
    }

    public static String getPaxosState(String paxosID) {
        return null;
//        return NameServer.getPaxosState(paxosID);
    }

    public static void updatePaxosState(String paxosID, String state) {
//        NameServer.updatePaxosState(paxosID, state);
    }

	/**
	 * Check if the given proposed value is a stop command.
	 * @param value command
	 * @return true if command is a stop command, else false.
	 */
	public static boolean isStopCommand(String value) {
		// not a NO-OP
		if (PaxosReplica.NO_OP.equals(value)) return false;
		// is value == STOP, means stop command
		if (PaxosReplica.STOP.equals(value)) return true;
		// this code for l

		if (!PaxosManager.debug) { // running as a part of GNS
			try
			{
				JSONObject json = new JSONObject(value);
				if (Packet.getPacketType(json).equals(Packet.PacketType.ACTIVE_PAXOS_STOP) ||
						Packet.getPacketType(json).equals(Packet.PacketType.PRIMARY_PAXOS_STOP))
					return true;
			} catch (JSONException e)
			{
				if (StartNameServer.debugMode) GNS.getLogger().fine("ERROR: JSON Exception Here: " + e.getMessage());
				e.printStackTrace();
			}
		}
		return false;
	}

	/**
	 * Propose requestPacket in the paxos instance with paxosID.
	 * ReqeustPacket.clientID is used to distinguish which method proposed this value.
	 * @param paxosID
	 * @param requestPacket
	 */
	public static void propose(String paxosID, RequestPacket requestPacket) {
		
		try
		{
			JSONObject json = requestPacket.toJSONObject();
//			if (StartNameServer.debugMode) GNRS.getLogger().fine("PAXOS PROPOSE:" + json.toString());
			// put paxos ID for identification
			json.put(PAXOS_ID, paxosID);
//			if (StartNameServer.debugMode) GNRS.getLogger().fine("PAXOS PROPOSE:" + json.toString());
			handleIncomingPacket(json);
			
		} catch (JSONException e)
		{
			if (StartNameServer.debugMode) GNS.getLogger().fine(" JSON Exception" + e.getMessage());
			e.printStackTrace();
		}
		
	}

//    static int decisionCount = 0;
//    static final  Object decisionLock = new ReentrantLock();
    /**
     *
     * @param paxosID
     * @param req
     * @param stop
     */
	public static void handleDecision(String paxosID, RequestPacket req, boolean stop)
	{
        if (PaxosManager.debug) {return;}

        if (paxosInstances.containsKey(paxosID)) {
            clientRequestHandler.forwardDecisionToClient(paxosID, req);
        }
        else {
            if (StartNameServer.debugMode) GNS.getLogger().severe(nodeID + " Paxos ID not found: " + paxosID);
        }
//        PaxosReplica paxosReplica = paxosInstances.get(paxosID);
        if (stop) paxosInstances.remove(paxosID);
            //paxosInstances.put(paxosID, new PaxosReplica(paxosID,nodeID, paxosReplica.getNodeIDs(),true, req));
	}

	/**
	 * If a node fails, or comes up again, the respective Paxos instances are informed.
	 * Some of them may elect a new co-ordinator.
	 */
	public static void informNodeStatus(FailureDetectionPacket fdPacket) {
		for (String x: paxosInstances.keySet()) {
            PaxosReplica r = paxosInstances.get(x);

            if (r.isNodeInPaxosInstance(fdPacket.responderNodeID)) {
				try
				{
                    JSONObject json = fdPacket.toJSONObject();
                    json.put(PAXOS_ID, x);
                    processMessage(new HandlePaxosMessageTask(json,fdPacket.packetType));
				} catch (JSONException e)
				{
					if (StartNameServer.debugMode) GNS.getLogger().fine(" JSON Exception");
					e.printStackTrace();
				}
			}
		}
        // inform output handler of node failure
        clientRequestHandler.handleFailureMessage(fdPacket);
	}

	/**
	 * Handle incoming message, incoming message could be of any Paxos instance.
	 * @param json
	 */
	public static void handleIncomingPacket(JSONObject json) {

        int incomingPacketType;
        try {
            GNS.getLogger().finest("here " + json.toString());
            incomingPacketType = json.getInt(PaxosPacketType.ptype);
        } catch (JSONException e) {
            e.printStackTrace();
            return;
        }
        switch (incomingPacketType){
            case PaxosPacketType.DECISION:
            case PaxosPacketType.ACCEPT:
                PaxosLogger2.logMessage(json);
                break;
            case PaxosPacketType.FAILURE_DETECT:
            case PaxosPacketType.FAILURE_RESPONSE:
                processMessage(new HandleFailureDetectionPacketTask(json));
                break;
            default:
                processMessage(new HandlePaxosMessageTask(json,incomingPacketType));
        }

//        if (json.has(PAXOS_ID)) {
////            if (StartNameServer.debugMode) GNS.getLogger().fine(paxosid + "\tPAXOS RECVD MSG: " + json);
//
//
//            if (incomingPacketType == PaxosPacketType.DECISION || incomingPacketType == PaxosPacketType.ACCEPT) {
//
//
//            } else {
//
//            }
//        }
//        else { // Failure detection packet
//
//        }

	}



    public static void processMessage(Runnable runnable) {
        executorService.submit(runnable);
    }

	/**
	 * all paxos instances use this method to exchange messages
	 * @param destID
	 * @param json
	 */
	public static void sendMessage(int destID, JSONObject json) {
        try
        {
            if (debug) {
                tcpTransport.sendToID(destID, json);
            }
            else {
                // Add paxos packet type field so that packet can be routed to a paxos manager.
                Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);
                NameServer.tcpTransport.sendToID(json, destID, PortType.STATS_PORT);
            }
        } catch (IOException e)
        {
            if (StartNameServer.debugMode) GNS.getLogger().severe("Paxos: IO Exception in sending to ID. " + destID);
        } catch (JSONException e)
        {
            if (StartNameServer.debugMode) GNS.getLogger().severe("JSON Exception in sending to ID. " + destID);
        }

	}

	/**
	 * main funtion to test the paxos manager code.
	 * @param args
	 */
	public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("QUIT. Incorrect arguments.\nUsage: PaxosManager <NumberOfPaoxsNodes> <ReplicaID> <NodeConfigFile>");
            System.exit(2);
        }
		// node IDs (for paxos replicas) = 0, 1, ..., N - 1
        String nodeConfigFile = args[0];
        String testConfig = args[1];
        String paxosLogFolder = args[2];
		int myID = Integer.parseInt(args[3]);

        PaxosLogger2.logFolder = paxosLogFolder + "/paxoslog_" + myID;
		initalizePaxosManagerDebugMode(nodeConfigFile, testConfig, myID, new DefaultPaxosClientRequestHandler());

	}

}


class PaxosPacketDemultiplexer extends PacketDemultiplexer {

    @Override
    public void handleJSONObjects(ArrayList jsonObjects) {
        for (Object j: jsonObjects) {
//            try {
                JSONObject json = (JSONObject)j;
                PaxosManager.handleIncomingPacket(json);
//                int incomingPacketType = json.getInt(PaxosPacketType.ptype);
//                if (incomingPacketType == PaxosPacketType.REQUEST) {
//                    String paxosID = json.getString(PaxosManager.PAXOS_ID);
//                    RequestPacket req = new RequestPacket(json);
//                    PaxosManager.clientRequestHandler.handleRequestFromClient(paxosID, req);
//                }
//                else {
//                    PaxosManager.handleIncomingPacket(json);
//                }
//            } catch (JSONException e) {
//                if (StartNameServer.debugMode) GNS.getLogger().fine("JSON Exception: PaxosPacketType not found");
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                continue;
//            }

        }

    }
}



class HandlePaxosMessageTask extends TimerTask {
    JSONObject json;
    int packetType;
    HandlePaxosMessageTask(JSONObject json, int packetType){
        this.json = json;
        this.packetType = packetType;
    }

    @Override
    public void run() {
        try {
            String paxosID;
            try {
                paxosID = json.getString(PaxosManager.PAXOS_ID);
            } catch (JSONException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return;
            }
            PaxosReplica replica = PaxosManager.paxosInstances.get(paxosID);
            if (replica != null) {
                if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tPAXOS PROCESS START " + paxosID + "\t" +  json);
                replica.handleIncomingMessage(json,packetType);
                if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tPAXOS MSG DONE " + paxosID + "\t" +  json);
            }
            else {
                if (StartNameServer.debugMode) GNS.getLogger().fine("ERROR: Paxos Instances does not contain ID = " + paxosID);
            }
        } catch (Exception e) {
            if (StartNameServer.debugMode) GNS.getLogger().severe(" PAXOS EXCEPTION!!.");
            e.printStackTrace();
        }

    }
}

class ResendPendingMessagesTask extends TimerTask{

    @Override
    public  void run() {
        for (String x:PaxosManager.paxosInstances.keySet()) {
            PaxosReplica paxosReplica = PaxosManager.paxosInstances.get(x);
            if (paxosReplica !=null) {
                paxosReplica.resendPendingAccepts();
                paxosReplica.checkIfReplicasUptoDate();
            }
        }
    }
}


