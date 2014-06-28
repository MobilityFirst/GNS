package edu.umass.cs.gns.reconfigurator;

import edu.umass.cs.gns.database.AbstractRecordCursor;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.MongoRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicaController.ReconfiguratorInterface;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkInterface;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.util.UniqueIDHashMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * Based on code created by abhigyan on 2/26/14.
 */
public class ReplicaController implements Replicable, ReconfiguratorInterface {

	public static final int RC_TIMEOUT_MILLIS = 3000;
	private static final long UNDOCUMENTED_DELAY_PARAMETER = 9000L;

	private final int myID; 
	private final InterfaceJSONNIOTransport niot; // used for all transport including helper classes
	private final JSONMessenger messenger;
	private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5);
	private final BasicRecordMap replicaControllerDB; 
	private final GNSNodeConfig gnsNodeConfig;
	private final ReplicationFrameworkInterface replicationFrameworkInterface; // replication algorithm
	private final UniqueIDHashMap ongoingStopActiveRequests = new UniqueIDHashMap(); 
	private final UniqueIDHashMap ongoingStartActiveRequests = new UniqueIDHashMap();
	private final ConcurrentHashMap<Integer, Double> nsRequestRates = new ConcurrentHashMap<>();

	private Logger log = Logger.getLogger(getClass().getName()); //GNS.getLogger();

	public ReplicaController(int nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig,
			InterfaceJSONNIOTransport nioServer, MongoRecords mongoRecords) {
		Config.initialize(configParameters);
		this.myID = nodeID;
		this.gnsNodeConfig = gnsNodeConfig;
		this.niot = nioServer;
		this.messenger = new JSONMessenger(this.niot);
		this.replicaControllerDB = new MongoRecordMap(mongoRecords, MongoRecords.DBREPLICACONTROLLER);
		this.replicationFrameworkInterface = ReplicationFrameworkType.instantiateReplicationFramework(Config.replicationFrameworkType, gnsNodeConfig);
	}

	@Override public ConcurrentHashMap<Integer, Double> getNsRequestRates() {return nsRequestRates;} // FIXME: make protected or remove
	@Override public GNSNodeConfig getGnsNodeConfig() {return gnsNodeConfig;} // FIXME: make protected or remove

	protected int getNodeID() {return myID;}
	protected BasicRecordMap getDB() {return replicaControllerDB;}
	protected InterfaceJSONNIOTransport getNioServer() {return niot;}
	protected UniqueIDHashMap getOngoingStopActiveRequests() {return ongoingStopActiveRequests;}
	protected UniqueIDHashMap getOngoingStartActiveRequests() {return ongoingStartActiveRequests;}
	protected ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {return executor;} // FIXME: This should be unnecessary.
	protected ReplicationFrameworkInterface getReplicationFrameworkInterface() {return replicationFrameworkInterface;}

	protected static int getNewActiveVersion(int activeVersion) {return activeVersion + 1;}


	/********************** Start of Replicable interface methods ***************************/
	@Override
	public String getState(String name) {
		AbstractRecordCursor iterator = replicaControllerDB.getAllRowsIterator();
		StringBuilder sb = new StringBuilder();
		int recordCount = 0;
		while (iterator.hasNext()) {
			try {
				JSONObject jsonObject = iterator.next();
				sb.append(jsonObject.toString());
				sb.append("\n");
				recordCount += 1;
			} catch (Exception e) {
				log.severe("Problem creating ReplicaControllerRecord from JSON" + e);
			}
		}
		log.info("Number of records whose state is read from DB: " + recordCount);
		return sb.toString();
	}

	/**
	 * ReplicaControllerCoordinator calls this method to locally execute a decision.
	 * Depending on packet type, it call other methods in ReplicaController package to execute request.
	 */

	/* FIXME: This method needs more documentation and helper methods. It assumes single-line JSON.
	 * It also seems to ignore state with just one JSONObject with no newline.
	 * The parsing of JSON objects should be done systematically by a helper method.
	 */
	@Override
	public boolean updateState(String paxosID, String state) {
		if (state==null || state.length() == 0) return true;
		int recordCount=0, startIndex=0;
		log.info("Update state: " + paxosID);
		try {
			while (true) {
				int endIndex = state.indexOf('\n', startIndex); // FIXME: What if data contains \n?
				if (endIndex == -1) {
					break;  // FIXME: If state is just a single-line, why ignore it?
				}
				String line = state.substring(startIndex, endIndex);
				if (line.length() > 0) {
					recordCount += 1;
					JSONObject json = new JSONObject(line); // FIXME: Assumes single-line JSON, why okay?
					ReplicaControllerRecord rcr = new ReplicaControllerRecord(replicaControllerDB, json);

					log.fine("Inserting rcr into DB ....: " + rcr + "\tjson = " + json);
					try {
						ReplicaControllerRecord.addNameRecordPrimary(replicaControllerDB, rcr);
					} catch (FailedUpdateException e) {
						ReplicaControllerRecord.updateNameRecordPrimary(replicaControllerDB, rcr);
					}
					startIndex = endIndex;
				} else {
					startIndex += 1;
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (FailedUpdateException e) {
			log.severe("Failed update exception: " + e.getMessage());
			e.printStackTrace();
		} catch (RecordExistsException e) {
			log.severe("Record exists exception: " + e.getMessage());
			e.printStackTrace();
		}
		log.info("Number of rc records updated in DB: " + recordCount);
		return true;
	}

	/* Arun: Changed helper methods to take only what they need. It is a bad idea
	 * to pass ReplicaController and make most of its methods protected or public
	 * just for the convenience of code organization.
	 */
	@Override
	public boolean handleDecision(String name, String value, boolean recovery) {
		try {
			try {
				JSONObject json = new JSONObject(value);
				Packet.PacketType packetType = Packet.getPacketType(json);
				MessagingTask[] mtasks = null;
				RCProtocolTask[] protocolTasks = new RCProtocolTask[1];
				switch (packetType) {

				// add name to GNS
				case ADD_RECORD:
					mtasks = Add.executeAddRecord(new AddRecordPacket(json), getDB(), getNodeID(), recovery);
					break;
				case ACTIVE_ADD_CONFIRM:
					mtasks = Add.executeAddActiveConfirm(new AddRecordPacket(json));
					break;

					// lookup actives for name
				case REQUEST_ACTIVES:
					mtasks = LookupActives.executeLookupActives(new RequestActivesPacket(json), getDB(), getNodeID(), recovery);
					break;

					// remove
				case REMOVE_RECORD:
					mtasks = Remove.executeMarkRecordForRemoval(new RemoveRecordPacket(json), getDB(), getNodeID(), recovery, protocolTasks);
					break;
				case ACTIVE_REMOVE_CONFIRM:  // confirmation received from active replica that name is removed
					Remove.handleActiveRemoveRecord(new OldActiveSetStopPacket(json), getNodeID(), getOngoingStopActiveRequests(), recovery);
					break;
				case RC_REMOVE:
					Remove.executeRemoveRecord(new RemoveRecordPacket(json), getDB(), getNodeID(), recovery);
					break;

					// group change
				case NEW_ACTIVE_PROPOSE:
					GroupChange.executeNewActivesProposed(new NewActiveProposalPacket(json), getDB(), getNodeID(), recovery, protocolTasks);
					break;
				case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY: // confirmation from active replica that old actives have stopped
					GroupChange.handleOldActiveStop(new OldActiveSetStopPacket(json), getDB(), getNodeID(), 
							getOngoingStopActiveRequests(), protocolTasks);
					break;
				case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:  // confirmation from active replica that new actives have started
					GroupChange.handleNewActiveStartConfirmMessage(new NewActiveSetStartupPacket(json), getNodeID(), 
							getOngoingStartActiveRequests(), protocolTasks);
					break;
				case GROUP_CHANGE_COMPLETE:
					GroupChange.executeActiveNameServersRunning(new GroupChangeCompletePacket(json), getDB(), getNodeID(), recovery);
					break;
				case NAMESERVER_SELECTION:
					NameStats.handleLNSVotesPacket(json, getDB());
					break;
				case NAME_RECORD_STATS_RESPONSE:
					// FIXME: todo this packets related to stats reporting are not implemented yet.
					throw new UnsupportedOperationException();
				case NAME_SERVER_LOAD:
					updateNSLoad(json);
				default:
					break;
				}

				// FIXME: todo after enabling group change, ensure that messages are not send on GROUP_CHANGE_COMPLETE and NEW_ACTIVE_PROPOSE.
				send(mtasks);
				schedule(protocolTasks);

			} catch (JSONException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			log.severe("Exception in handling decisions: " + e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	/********************** End of Replicable interface methods ***************************/

	private void schedule(RCProtocolTask[] protocolTasks) {
		for(RCProtocolTask task : protocolTasks) {
			task.setReplicaController(this);
			this.executor.scheduleAtFixedRate(task, 0, ReplicaController.RC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
		}
	}
	// FIXME: Method makes little sense and has hardcoded values. 
	protected boolean isSmallestNodeRunning(String name, Set<Integer> nameServers) {
		Random r = new Random(name.hashCode());
		ArrayList<Integer> copy = new ArrayList<Integer>(nameServers);
		Collections.sort(copy);
		Collections.shuffle(copy, r);
		for (int id : copy) {
			if (gnsNodeConfig.getPingLatency(id) < UNDOCUMENTED_DELAY_PARAMETER) // FIXME: What is this???
				return id == myID;
		}
		return false;
	}

	private void updateNSLoad(JSONObject json) throws JSONException {
		NameServerLoadPacket nsLoad  = new NameServerLoadPacket(json);
		if (Config.debugMode) log.fine("Updated NS Load. Node: " + nsLoad.getReportingNodeID() +
				"\tPrevLoad: " + nsRequestRates.get(nsLoad.getReportingNodeID()) +
				"\tNewNoad: " + nsLoad.getLoadValue() + "\t");
		nsRequestRates.put(nsLoad.getReportingNodeID(), nsLoad.getLoadValue());
	}

	protected void send(MessagingTask mtask) throws JSONException, IOException {
		this.messenger.send(mtask);
	}
	protected void send(MessagingTask[] mtasks) throws JSONException, IOException {
		for(MessagingTask mtask : mtasks) this.send(mtask);
	}

	public static void main(String[] args) {
		int id = 100;
		HashMap<String,String> configParameters = new HashMap<String,String>();
		GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(Config.ARUN_GNS_DIR_PATH+"/conf/testCodeResources/name-server-info", 100);
		System.out.println(gnsNodeConfig.getNodePort(id));
		try {
			JSONNIOTransport niot = new JSONNIOTransport(id, gnsNodeConfig, new JSONMessageExtractor(new PacketDemultiplexerDefault())); 
			MongoRecords mongoRecords = new MongoRecords(id, Config.mongoPort);
			ReplicaController rc = new ReplicaController(id, configParameters, gnsNodeConfig, niot, mongoRecords);
			System.out.println("SUCCESS: ReplicaController " + rc.getNodeID() + " started without exceptions." +
					"\nNothing else except startup has been tested yet.");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
