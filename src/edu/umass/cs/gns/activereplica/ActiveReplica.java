package edu.umass.cs.gns.activereplica;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.NSNodeConfig;
import edu.umass.cs.gns.nsdesign.Reconfigurable;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.DefaultGnsCoordinator;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.DummyGnsCoordinatorUnreplicated;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsCoordinatorEventual;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsCoordinatorPaxos;
import edu.umass.cs.gns.nsdesign.packet.NameServerLoadPacket;
import edu.umass.cs.gns.nsdesign.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.reconfigurator.Add;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.util.Reportable;
import edu.umass.cs.gns.util.ReportingTask;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class ActiveReplica<AppType extends Reconfigurable & Replicable> implements Replicable, Reportable {

	private static final double STATS_MESSAGING_RATE = 1.0; 

	private final AppType app; // don't create a getActiveReplicaApp() like method to just expose this outside

	private final ActiveReplicaCoordinator coordinator; // don't create a getCoordinator() like method to just expose this outside

	private final int myID;

	private final GNSNodeConfig gnsNodeConfig;

	private final JSONMessenger messenger;

	private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5);

	private final ActiveReplicaStats myStats = new ActiveReplicaStats();

	private final ScheduledFuture<?> reportingFuture;

	private final HashMap<Integer,NewActiveSetStartupPacket> ongoingStateTransferRequests = new HashMap<Integer,NewActiveSetStartupPacket>();

	private final HashMap<Integer,NewActiveStartInfo> activeStartupInProgress = new HashMap<Integer,NewActiveStartInfo>();

	private static Logger log = NIOTransport.LOCAL_LOGGER ? Logger.getLogger(Add.class.getName()) : GNS.getLogger();

	public ActiveReplica(int id, GNSNodeConfig nc, JSONNIOTransport niot, AppType app) {
		this.myID = id;
		this.gnsNodeConfig = nc;
		this.app = app;
		this.coordinator = setReplicaCoordinator();
		this.messenger = new JSONMessenger(niot);
		this.reportingFuture = startStatsReporter();
	}

	private ScheduledFuture<?> startStatsReporter() {
		ReportingTask<ActiveReplica<AppType>> reporter = new ReportingTask<ActiveReplica<AppType>>(this, 
				STATS_MESSAGING_RATE);
		return reporter.start();
	}
	protected void cancelStatsReporter(boolean interrupt) {this.reportingFuture.cancel(interrupt);}

	public void handleIncomingPacket(JSONObject json) {
		try {
			Packet.PacketType type = Packet.getPacketType(json);
			MessagingTask mtask = null;
			ARProtocolTask[] protocolTasks = new ARProtocolTask[1];
			switch (type) {
			// replica controller to active replica
			case NEW_ACTIVE_START:
				mtask = GroupChange.handleNewActiveStart(new NewActiveSetStartupPacket(json), getNodeID(), 
						getActiveStartupInProgress(), protocolTasks/*return value*/);
				break;
			case NEW_ACTIVE_START_FORWARD:
				GroupChange.handleNewActiveStartForward(new NewActiveSetStartupPacket(json), protocolTasks);
				break;
			case NEW_ACTIVE_START_RESPONSE:
				mtask = GroupChange.handleNewActiveStartResponse(new NewActiveSetStartupPacket(json), 
						getActiveStartupInProgress());
				break;
			case NEW_ACTIVE_START_PREV_VALUE_REQUEST:
				NewActiveSetStartupPacket request = new NewActiveSetStartupPacket(json);
				mtask = GroupChange.handlePrevValueRequest(request, this.app.getFinalState(request.getName(), 
						request.getOldActiveVersion()), getNodeID());
				break;
			case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:
				GroupChange.handlePrevValueResponse(processNewActiveStartPrevValueResponse(json), getNodeID());
				break;
			case OLD_ACTIVE_STOP:
				getCoordinator().coordinateRequest(json); 
				break;
			case DELETE_OLD_ACTIVE_STATE:
				OldActiveSetStopPacket deleteOld = new OldActiveSetStopPacket(json);
				this.app.deleteFinalState(deleteOld.getName(), deleteOld.getVersion());
				break;
			}

			this.send(mtask);
			this.schedule(protocolTasks[0]);

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/* Received a response containing the final state from the previous version */
	private NewActiveSetStartupPacket processNewActiveStartPrevValueResponse(JSONObject msg) throws JSONException {
		NewActiveSetStartupPacket response = new NewActiveSetStartupPacket(msg);
		if (Config.debugMode) log.info(" Received NEW_ACTIVE_START_PREV_VALUE_RESPONSE at node " + getNodeID());
		if (response.getPreviousValueCorrect()) {
			this.app.putInitialState(response.getName(), response.getNewActiveVersion(), response.getPreviousValue());
			getCoordinator().coordinateRequest(response.toJSONObject());
			return (NewActiveSetStartupPacket) getOngoingStateTransferRequests().remove(response.getUniqueID());
		} else if (Config.debugMode) log.info("Node "+myID+" Old Active did not return previous value.");
		return null;
	}

	@Override
	public boolean handleDecision(String name, String value, boolean recovery) {
		if (!recovery) this.myStats.registerRequest();
		boolean executed = false;
		try {
			JSONObject json = new JSONObject(value);
			if (Packet.getPacketType(json).equals(Packet.PacketType.OLD_ACTIVE_STOP)) {
				OldActiveSetStopPacket stopPkt = new OldActiveSetStopPacket(json);
				if (Config.debugMode) log.fine("Node "+myID+" executing stop request: " + value);
				boolean noCoordinationState = json.has(Config.NO_COORDINATOR_STATE_MARKER);
				if (noCoordinationState) {
					// probably stop has already been executed, so send confirmation to replica controller
					log.warning("No coordinator state found for stop request: " + value);
					executed = true;
				} else {
					executed = this.app.stopVersion(stopPkt.getName(), (short) -1);
					if (!executed) {
						log.severe("Stop not executed: name="+stopPkt.getName()+"; request="+value);
					}
				}
				if (executed) {
					try {
						stopProcessed(new OldActiveSetStopPacket(new JSONObject(value)));
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
			else executed = this.app.handleDecision(name, value, recovery);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return executed;
	}
	@Override
	public String getState(String name) {
		return this.app.getState(name);
	}

	@Override
	public boolean updateState(String name, String state) {
		return this.app.updateState(name, state);
	}

	/**
	 * The app will call this method after it has executed stop decision
	 */
	public void stopProcessed(OldActiveSetStopPacket stopPacket) {
		GroupChange.handleStopProcessed(stopPacket, this);
	}

	/* FIXME: Create the coordinator based on the specification in Config.
	 * PaxosConfig should take a Config object if you simply need to 
	 * supply it values from Config. Otherwise the code is needlessly 
	 * verbose here.
	 */
	private ActiveReplicaCoordinator setReplicaCoordinator() {
		if (Config.singleNS && Config.dummyGNS) {  // coordinator for testing only
			return new DummyGnsCoordinatorUnreplicated(getNodeID(), this.app);
		} else if (Config.singleNS) {  // coordinator for testing only
			return new DefaultGnsCoordinator(getNodeID(), this.app);
		} else if(Config.eventualConsistency) {  // coordinator for testing only
			return new GnsCoordinatorEventual(getNodeID(), this.messenger, new NSNodeConfig(gnsNodeConfig),
					this.app, new PaxosConfig(), Config.readCoordination);
		} else { // this is the actual coordinator
			return new GnsCoordinatorPaxos(getNodeID(), this.messenger, new NSNodeConfig(gnsNodeConfig),
					this.app, new PaxosConfig(), Config.readCoordination);
		}
	}

	public int getNodeID() {return this.myID;}
	public GNSNodeConfig getGnsNodeConfig() {return this.gnsNodeConfig;}
	public int[] getNameServerIDs() {return Util.setToIntArray(this.gnsNodeConfig.getNameServerIDs());}

	public void send(MessagingTask mtask) {
		try {
			this.messenger.send(mtask);
		} catch(IOException ioe) {ioe.printStackTrace();}
		catch(JSONException je) {je.printStackTrace();}
	}
	public void schedule(ARProtocolTask protocolTask) {
		protocolTask.setActiveReplica(this);
		this.executor.scheduleAtFixedRate(protocolTask, 0, Config.NS_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
	}

	public JSONObject getStats() {
		JSONObject stats = null;
		try {
			NameServerLoadPacket nsStats = new NameServerLoadPacket(getNodeID(), getNodeID(), 
					this.myStats.getRequestRate());
			stats = nsStats.toJSONObject();
		} catch(JSONException je) {je.printStackTrace();}
		return stats;
	}
	public Set<Integer> getRecipients() {return this.gnsNodeConfig.getNameServerIDs();}

	/* Value of a specific subset of stats. We may need
	 * to send different stats to different sets of nodes. 
	 */
	public JSONObject getStats(String statID) {
		assert false : "Method not implemented yet";
	throw new RuntimeException("Method not implemented yet");
	}
	/* Recipients for a specific subset of stats. */
	public Set<Integer> getRecipients(String statID) {
		assert false : "Method not implemented yet";
	throw new RuntimeException("Method not implemented yet");
	}

	public JSONMessenger getJSONMessenger() {
		return this.messenger;
	}

	public ActiveReplicaCoordinator getCoordinator() {
		return this.coordinator;
	}


	private HashMap<Integer,NewActiveStartInfo> getActiveStartupInProgress() {
		return this.activeStartupInProgress;
	}

	public HashMap<Integer,NewActiveSetStartupPacket> getOngoingStateTransferRequests() {
		return this.ongoingStateTransferRequests;
	}

	// FIXME: Write a main method to test this module independently
	public static void main(String[] args) {
		
	}
}
