package edu.umass.cs.gigapaxos;

import edu.umass.cs.gigapaxos.paxospackets.FailureDetectionPacket;
import edu.umass.cs.gigapaxos.paxospackets.FindReplicaGroupPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.MessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.Messenger;
import edu.umass.cs.gigapaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gigapaxos.paxosutil.StringContainer;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.gigapaxos.testing.TESTPaxosReplicable;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.InterfaceNIOTransport;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.MultiArrayMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            <p>
 *            PaxosManager is the primary interface to create and use paxos by
 *            creating a paxos instance.
 * 
 *            PaxosManager manages all paxos instances at a node. There is
 *            typically one paxos manager per machine. This class could be
 *            static, but it is not so that we can test emulations involving
 *            multiple "machines" within a JVM.
 * 
 *            PaxosManager has four functions at a machine that are useful
 *            across paxos instances of all applications on the machine: (1)
 *            logging, (2) failure detection, (3) messaging, and (4) paxos
 *            instance mapping. The fourth is key to allowing the manager to
 *            demultiplex incoming messages to the appropriate application paxos
 *            instance.
 */
public class PaxosManager<NodeIDType> {

	private static final int PINSTANCES_CAPACITY = 2000000; // 2M
	private static final long MORGUE_DELAY = 30000;
	private static final boolean HIBERNATE_OPTION = false;
	private static final boolean PAUSE_OPTION = true;
	private static final long DEACTIVATION_PERIOD = 30000; // 30s default

	/**
	 * Refer to documentation in AbstractReconfiguratorDB.
	 */
	public static final long MAX_FINAL_STATE_AGE = 3600 * 1000;
	/**
	 * Whether request batching is enabled.
	 */
	protected static final boolean BATCHING_ENABLED = true;

	/*
	 * FIXME: Unclear what a good default is for good liveness. It doesn't
	 * really matter for safety of reconfiguration.
	 */
	protected static final long CAN_CREATE_TIMEOUT = 5000;
	private static final long WAIT_TO_GET_CREATED_TIMEOUT = 2000; // needed?

	private static final boolean EMULATE_UNREPLICATED = false;

	/*
	 * ONE_PASS_RECOVERY should just be true. False will try to individually
	 * roll forward each paxos instance during recovery and will be slow because
	 * it has to pull each instances log messages from the DB individually. The
	 * option exists only for historical reasons and is deprecated.
	 */
	@Deprecated
	private static final boolean ONE_PASS_RECOVERY = true;

	// final
	private final AbstractPaxosLogger paxosLogger; // logging
	private final FailureDetection<NodeIDType> FD; // failure detection
	private final Messenger<NodeIDType> messenger; // messaging
	private final int myID;
	private final InterfaceReplicable myApp; // default app for all paxosIDs

	// background deactivation/cremation tasks, all else event-driven
	private final ScheduledExecutorService executor;
	// paxos instance mapping
	private final MultiArrayMap<String, PaxosInstanceStateMachine> pinstances;
	// stopped paxos instances about to be incinerated
	private final HashMap<String, PaxosInstanceStateMachine> corpses;
	private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
	private final Stringifiable<NodeIDType> unstringer;
	private final RequestBatcher batched;

	private int outOfOrderLimit = PaxosInstanceStateMachine.SYNC_THRESHOLD; // default
	private int interCheckpointInterval = PaxosInstanceStateMachine.INTER_CHECKPOINT_INTERVAL;
	private int maxSyncDecisionsGap = PaxosInstanceStateMachine.MAX_SYNC_DECISIONS_GAP;
	private final boolean nullCheckpointsEnabled;

	// non-final
	private boolean hasRecovered = false;

	// need this to be static so DB can be closed gracefully
	private static boolean closed = false;

	// need this to be static so DB can be closed gracefully
	private static int processing = 0;

	/*
	 * Note: PaxosManager itself maintains no NIO transport instance as it
	 * delegates all communication related activities to other objects.
	 * PaxosManager is only responsible for managing state for and
	 * demultiplexing incoming packets to a number of paxos instances at this
	 * node.
	 */

	private static Logger log = Logger.getLogger(PaxosManager.class.getName());

	/**
	 * @param id
	 *            My node ID.
	 * @param unstringer
	 *            An instance of Stringifiable that can convert String to
	 *            NodeIDType.
	 * @param niot
	 *            InterfaceNIOTransport or InterfaceMessenger object used for
	 *            messaging.
	 * @param pi
	 *            InterfaceReplicable application controlled by gigapaxos.
	 *            Currently, all paxos instances must correspond to a single
	 *            umbrella application even though each createPaxosInstance
	 *            method explicitly specifies the app and this information is
	 *            stored explicitly inside a paxos instance. The reason for the
	 *            single umbrella app restriction is that we won't have a
	 *            pointer to the appropriate app upon recovery otherwise.
	 * @param paxosLogFolder
	 *            Paxos logging folder.
	 * @param enableNullCheckpoints
	 *            Whether null checkpoints are enabled. We need this flag to be
	 *            enabled if we intend to reconfigure paxos groups managed by
	 *            this PaxosManager. Otherwise, we can not distinguish between a
	 *            null checkpoint and no checkpoint, so the next epoch members
	 *            may be waiting forever for the previous epoch's final state
	 *            (that happens to be null). This flag needs to be set at
	 *            construction time and can not be changed thereafter.
	 */
	public PaxosManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
			InterfaceNIOTransport<NodeIDType, JSONObject> niot,
			InterfaceReplicable pi, String paxosLogFolder,
			boolean enableNullCheckpoints) {
		this.myID = this.integerMap.put(id);// id.hashCode();
		this.executor = Executors.newScheduledThreadPool(1,
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread thread = Executors.defaultThreadFactory()
								.newThread(r);
						thread.setName(PaxosManager.class.getSimpleName()
								+ myID);
						return thread;
					}
				});
		this.unstringer = unstringer;
		this.myApp = pi;
		this.FD = new FailureDetection<NodeIDType>(id, niot, paxosLogFolder);
		this.pinstances = new MultiArrayMap<String, PaxosInstanceStateMachine>(
				PINSTANCES_CAPACITY);
		this.corpses = new HashMap<String, PaxosInstanceStateMachine>();
		// this.activePaxii = new HashMap<String, ActivePaxosState>();
		this.messenger = new Messenger<NodeIDType>(niot, this.integerMap);
		this.paxosLogger = new SQLPaxosLogger(this.myID, id.toString(),
				paxosLogFolder, this.messenger);
		this.nullCheckpointsEnabled = enableNullCheckpoints;
		// periodically remove active state for idle paxii
		executor.scheduleWithFixedDelay(new Deactivator(), 0,
				DEACTIVATION_PERIOD, TimeUnit.MILLISECONDS);
		(this.batched = new RequestBatcher(this)).start();
		testingInitialization();
		// needed to unclose when testing multiple runs of open and close
		open();
		// so paxos packets will come to me
		niot.addPacketDemultiplexer(new PaxosPacketDemultiplexer(this));
		initiateRecovery();
	}

	/**
	 * Refer
	 * {@link #PaxosManager(Object, Stringifiable, InterfaceNIOTransport, InterfaceReplicable, String)}
	 * .
	 * 
	 * @param id
	 * @param nc
	 * @param niot
	 * @param app
	 * @param paxosLogFolder
	 */
	public PaxosManager(NodeIDType id, Stringifiable<NodeIDType> nc,
			InterfaceNIOTransport<NodeIDType, JSONObject> niot,
			InterfaceReplicable app, String paxosLogFolder) {
		this(id, nc, niot, (app), paxosLogFolder,
				PaxosInstanceStateMachine.ENABLE_NULL_CHECKPOINT_STATE);
	}

	/*
	 * We need to be careful with pause/unpause and createPaxosInstance as there
	 * are potential cyclic dependencies. The call chain is as below, where
	 * "info" is the information needed to create a paxos instance. The
	 * notation"->?" means the call may or may not happen, which is why the
	 * recursion breaks after at most one step.
	 * 
	 * On recovery: initiateRecovery() -> recover(info) ->
	 * createPaxosInstance(info) -> getInstance(paxosID, tryHotRestore) ->?
	 * unpause(paxosID) ->? createPaxosInstance(info, hri). An inifinite
	 * recursion is prevented because createPaxosInstance will not again call
	 * getInstance with the tryHotRestore option if hri!=null.
	 * 
	 * Upon createPaxosInstance any time after recovery, the same chain as above
	 * is followed.
	 * 
	 * On deactivation: deactivate(paxosID) ->? pause(paxosID) // may or may not
	 * be successful
	 * 
	 * On incoming packet: handleIncomingPacket() -> getInstance(paxosID) ->?
	 * unpause(paxosID) ->? createPaxosInstance(info)
	 */

	/**
	 * Returns members of this paxos instance if it is active, and null
	 * otherwise. Note that this means that we return null even if the paxos
	 * instance is being created (but has not yet completed recovery). A
	 * non-null answer to this method necessarily means that the paxos instance
	 * was ready to take more requests at the time of the query.
	 * 
	 * @param paxosID
	 *            The name of the object or service being managed as a
	 *            replicated state machine.
	 * @param version
	 *            The reconfiguration version.
	 * @return Set of members in the paxos instance paxosID:version.
	 */
	public Set<NodeIDType> getReplicaGroup(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version && pism.isActive())
			return this.integerMap.getIntArrayAsNodeSet(pism.getMembers());
		return null;
	}

	/**
	 * @param paxosID
	 * @return Set of members in the paxos instance named paxosID. There can
	 *         only be one version of a paxos instance at a node, so we don't
	 *         really have to specify a version unless we want to explicitly get
	 *         the members for a specific version, in which case,
	 *         {@link #getReplicaGroup(String, int)} should be used.
	 */
	public Set<NodeIDType> getReplicaGroup(String paxosID) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.isActive())
			return this.integerMap.getIntArrayAsNodeSet(pism.getMembers());
		return null;
	}

	/**
	 * FIXME: method unused.
	 * 
	 * @param paxosID
	 * @param version
	 * @return Whether paxos instance exists and is active.
	 */
	protected boolean isActive(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.isActive() && pism.getVersion() == version)
			return true;
		return false;
	}

	/**
	 * 
	 * FIXME: method unused.
	 * 
	 * @param paxosID
	 * @return Returns true if this paxos instance exists and is stopped.
	 */
	protected boolean isStopped(String paxosID) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.isStopped())
			return true;
		return false;
	}

	private void synchronizedNoop(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version)
			pism.synchronizedNoop();
	}

	/**
	 * A method that specifies the minimum number of necessary arguments to
	 * create a paxos instance. The version is assumed to be 0 here. The use of
	 * this method is encouraged only if reconfiguration is not desired. The
	 * initialState argument can be {@code null} if
	 * {@link #isNullCheckpointStateEnabled()} is true.
	 * 
	 * @param paxosID
	 * @param gms
	 * @param initialState
	 * @return Returns true if the paxos instance paxosID:version or one with a
	 *         higher version number was successfully created.
	 */

	public boolean createPaxosInstance(String paxosID, Set<NodeIDType> gms,
			String initialState) {
		return this.createPaxosInstance(paxosID, 0, gms, myApp, null, null,
				true);
	}

	/**
	 * Paxos instance creation with an initial state specified.
	 * 
	 * @param paxosID
	 *            Paxos group name.
	 * @param version
	 *            Paxos group version (or epoch number).
	 * @param gms
	 *            Group members.
	 * @param app
	 *            Application controlled by paxos.
	 * @param initialState
	 *            Initial application state.
	 * @return Whether this paxos instance or higher got created.
	 */
	public boolean createPaxosInstance(String paxosID, int version,
			Set<NodeIDType> gms, InterfaceReplicable app, String initialState) {
		return this.createPaxosInstance(paxosID, version, gms, app,
				initialState, null, true);
	}

	private synchronized boolean createPaxosInstance(String paxosID,
			int version, Set<NodeIDType> gms, InterfaceReplicable app,
			String initialState, HotRestoreInfo hri, boolean tryRestore) {
		return this.createPaxosInstance(paxosID, version, gms, app,
				initialState, hri, tryRestore, false);
	}

	/*
	 * Synchronized in order to prevent duplicate instance creation under
	 * concurrency. This is the only method that can actually create a paxos
	 * instance. All other methods just call this method eventually.
	 * 
	 * private because it ensures that initialState!=null and missedBirthing are
	 * not both true.
	 */
	private synchronized boolean createPaxosInstance(String paxosID,
			int version, Set<NodeIDType> gms, InterfaceReplicable app,
			String initialState, HotRestoreInfo hri, boolean tryRestore,
			boolean missedBirthing) {

		if (this.isClosed())
			return false;

		boolean tryHotRestore = (hasRecovered() && hri == null);
		PaxosInstanceStateMachine pism = this.getInstance(paxosID,
				tryHotRestore, tryRestore);
		// if equal or higher version exists, return false
		if ((pism != null) && (pism.getVersion() - version >= 0)) {
			log.log(Level.FINE,
					"{0} paxos instance {1}:{2} or higher version currently exists",
					new Object[] { this, paxosID, version });
			return false;
		}

		// if lower version exists, return false
		if (pism != null && (pism.getVersion() - version < 0)) {
			log.log(Level.INFO,
					"{0} has pre-existing paxos instance {1} when asked to create version {2}",
					new Object[] { this, pism.getPaxosIDVersion(), version });
			// pism must be explicitly stopped first
			return false; // initialState will also be ignored here
		}
		// if equal or higher version stopped on disk, return false
		if (pism == null && equalOrHigherVersionStopped(paxosID, version)) {
			log.log(Level.INFO,
					"{0} paxos instance {1}:{2} can not be created as equal or higher "
							+ "version {3}:{4} was previously created and stopped",
					new Object[] { this, paxosID, version, paxosID,
							this.getVersion(paxosID) });
			return false;
		}

		// else try to create (could still run into exception)
		pism = new PaxosInstanceStateMachine(paxosID, version, myID,
				this.integerMap.put(gms), app != null ? app : this.myApp,
				initialState, this, hri, missedBirthing);

		pinstances.put(paxosID, pism);
		this.notifyUponCreation();
		assert (this.getInstance(paxosID, false, false) != null);
		log.log(Level.FINE,
				"{0} successfully {1} paxos instance {2}",
				new Object[] { this, hri != null ? "unpaused" : "created",
						pism.getPaxosIDVersion() });
		/*
		 * Note: rollForward can not be done inside the instance as we first
		 * need to update the instance map here so that networking--even
		 * trivially sending message to self--works.
		 */
		assert (hri == null || hasRecovered());
		if (hri == null) // not hot restore
			rollForward(paxosID);
		// to sync decisions initially if needed or if missed birthing
		this.syncPaxosInstance(pism, missedBirthing);

		// keepalives only if needed
		this.FD.sendKeepAlive(gms);
		return true;
	}

	private void syncPaxosInstance(PaxosInstanceStateMachine pism,
			boolean forceSync) {
		if (pism != null)
			pism.poke(forceSync);
	}

	/**
	 * When a node is being permanently deleted.
	 * 
	 * @param id
	 * @return True if {@code id} was being monitored.
	 */
	public boolean stopFailureMonitoring(NodeIDType id) {
		return this.FD.dontSendKeepAlive(id);
	}

	private boolean canCreateOrExistsOrHigher(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		return (pism == null || (pism.getVersion() - version >= 0));
	}

	private boolean instanceExistsOrHigher(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		return (pism != null && (pism.getVersion() - version >= 0));
	}

	class PaxosPacketDemultiplexer extends AbstractJSONPacketDemultiplexer {

		private final PaxosManager<NodeIDType> paxosManager;

		public PaxosPacketDemultiplexer(PaxosManager<NodeIDType> pm) {
			paxosManager = pm;
			this.register(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
			this.setThreadName("" + myID);
		}

		public boolean handleMessage(JSONObject jsonMsg) {
			boolean isPacketTypeFound = false;
			try {

				PaxosPacket.PaxosPacketType type = PaxosPacket.PaxosPacketType
						.getPaxosPacketType(JSONPacket.getPacketType(jsonMsg));
				if (type == null
						|| !type.equals(PaxosPacket.PaxosPacketType.PAXOS_PACKET))
					return false;

				PaxosManager.getLogger().log(Level.FINEST,
						"{0} packet demultiplexer received {1}",
						new Object[] { PaxosManager.this, jsonMsg });
				paxosManager.handleIncomingPacket(jsonMsg);
				return true;
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return isPacketTypeFound;
		}
	}

	// Called by external entities, so we need to fix node IDs
	private void handleIncomingPacket(JSONObject jsonMsg) {
		try {
			if (BATCHING_ENABLED)
				this.enqueueRequest(fixNodeStringToInt(jsonMsg));
			else
				this.handlePaxosPacket(fixNodeStringToInt(jsonMsg));
		} catch (JSONException e) {
			log.severe(this + " unable to fix node ID string to integer");
			e.printStackTrace();
		}
	}

	/*
	 * If RequestPacket, hand over to batcher that will then call
	 * handleIncomingPacketInternal on batched requests.
	 */
	private void enqueueRequest(JSONObject jsonMsg) {
		try {
			if (PaxosPacket.getPaxosPacketType(jsonMsg).equals(
					PaxosPacket.PaxosPacketType.REQUEST))
				this.batched.enqueue(new RequestPacket(jsonMsg));
			else
				this.handlePaxosPacket(jsonMsg);
		} catch (JSONException e) {
			log.severe(this + " unable to process JSONObject " + jsonMsg);
			e.printStackTrace();
		}
	}

	/**
	 * Called by internal entities like rollForward and works with integer node
	 * IDs.
	 * 
	 * @param jsonMsg
	 */
	protected void handlePaxosPacket(JSONObject jsonMsg) {
		if (this.isClosed())
			return;
		else if (emulateUnreplicated(jsonMsg)) // testing
			return;
		else
			setProcessing(true);

		PaxosPacketType paxosPacketType;
		try {
			RequestPacket.addDebugInfo(jsonMsg, ("i" + myID));
			// will throw exception if no PAXOS_PACKET_TYPE
			paxosPacketType = PaxosPacket.getPaxosPacketType(jsonMsg);
			switch (paxosPacketType) {
			case FAILURE_DETECT:
				FD.receive(new FailureDetectionPacket<NodeIDType>(jsonMsg,
						unstringer));
				break;
			case FIND_REPLICA_GROUP:
				processFindReplicaGroup(new FindReplicaGroupPacket(jsonMsg));
				break;
			default: // paxos protocol messages
				assert (jsonMsg.has(PaxosPacket.PAXOS_ID)) : jsonMsg;
				String paxosID = jsonMsg.getString(PaxosPacket.PAXOS_ID);
				PaxosInstanceStateMachine pism = this.getInstance(paxosID);
				log.log(Level.FINEST,
						"{0} received paxos message for retrieved instance {1} : {2}",
						new Object[] { this, pism, jsonMsg });
				if (pism != null)
					// version checked internally
					pism.handlePaxosMessage(jsonMsg);
				else
					// for recovering group created while crashed
					this.findPaxosInstance(jsonMsg, paxosID);
				break;
			}
		} catch (JSONException je) {
			log.severe("Node" + this.myID + " received bad JSON message: "
					+ jsonMsg);
			je.printStackTrace();
		}
		setProcessing(false);
	}

	private String propose(String paxosID, RequestPacket requestPacket)
			throws JSONException {
		if (this.isClosed())
			return null;
		boolean matched = false;
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null) {
			matched = true;
			log.log(Level.FINE, "{0} proposing to {1}: {2}", new Object[] {
					this, pism.getPaxosIDVersion(), requestPacket });
			requestPacket.putPaxosID(paxosID, pism.getVersion());
			this.handleIncomingPacket(requestPacket.toJSONObject());
		} else
			log.log(Level.INFO,
					"{0} could not find paxos instance {1} for request {2}; this can happen if a"
							+ " request arrives before (after) a paxos instance is created (stopped)"
							+ "; last known version was [{3}]",
					new Object[] {
							this,
							paxosID,
							Util.truncate(requestPacket.getRequestValues()[0],
									64), this.getVersion(paxosID) });
		return matched ? pism.getPaxosIDVersion() : null;
	}

	// used (only) by RequestBatcher for already batched RequestPackets
	protected void proposeBatched(RequestPacket requestPacket) {
		try {
			this.handlePaxosPacket(requestPacket.toJSONObject());
		} catch (JSONException e) {
			log.severe(this + " " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Propose a request to the paxos group with name paxosID.
	 * 
	 * @param paxosID
	 * @param requestPacket
	 * @return The paxosID:version represented as a String to which the request
	 *         got proposed; null if no paxos group named paxosID exists
	 *         locally.
	 */
	public String propose(String paxosID, String requestPacket) {
		RequestPacket request = (new RequestPacket(-1, requestPacket, false))
				.setReturnRequestValue();
		String retval = null;
		try {
			retval = propose(paxosID, request);
		} catch (JSONException je) {
			log.severe("Could not parse proposed request string as paxospackets.RequestPacket");
			je.printStackTrace();
		}
		return retval;
	}

	/**
	 * Proposes a request to stop a specific version of paxosID. There is no way
	 * to stop a paxos replicated state machine other than by issuing a stop
	 * request. The only way to know for sure that the RSM is stopped is by the
	 * application receiving the stop request.
	 * 
	 * @param paxosID
	 * @param value
	 * @param version
	 * @return The paxosID:version represented as a String to which this request
	 *         was proposed.
	 */
	public String proposeStop(String paxosID, String value, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version) {
			RequestPacket request = (new RequestPacket(-1, value, true))
					.setReturnRequestValue();
			request.putPaxosID(paxosID, version);
			try {
				return this.propose(paxosID, request);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		} else
			log.log(Level.INFO,
					"{0} failed to propose stop request for {1}:{2}; {3}",
					new Object[] {
							this,
							paxosID,
							version,
							(pism == null ? " no paxos instance found " : pism
									.getPaxosIDVersion() + " pre-exists") });
		return null;
	}

	/**
	 * Stop the paxos instance named paxosID on this machine irrespective of the
	 * version. Note that there can be at most one paxos version with a given
	 * name on a machine.
	 * 
	 * @param paxosID
	 * @param value
	 * @return The paxosID:version string to which the request was proposed.
	 */
	public String proposeStop(String paxosID, String value) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism == null)
			return null;
		else
			return this.proposeStop(paxosID, value, pism.getVersion());
	}

	/**
	 * @param paxosID
	 * @param version
	 * @return The final state wrapped in a StringContainer. We use
	 *         StringContainer to distinguish between null state and no state
	 *         when null checkpoint state is enabled.
	 */
	public StringContainer getFinalState(String paxosID, int version) {
		/*
		 * The wait below is a hack to force a little wait through
		 * synchronization. If a stop request is being executed concurrently
		 * while a getFinalState request arrives, it is better for the stop
		 * request to finish and the final checkpoint to be created before we
		 * issue getEpochFinalCheckpoint. Otherwise, we would return a null
		 * response for getEpochFinalCheckpointState here and the requester of
		 * the checkpoint is forced to time out and resend the request after a
		 * coarse-grained timeout. This wait is just an optimization and is not
		 * necessary for safety.
		 */
		this.synchronizedNoop(paxosID, version);
		return this.paxosLogger.getEpochFinalCheckpointState(paxosID, version);
	}

	/**
	 * We are only allowed to delete a stopped paxos instance. There is no
	 * public method to force-kill a paxos instance other than by successfully
	 * creating a paxos instance with the same name and higher version. The only
	 * way to stop a paxos instance is to get a stop request committed.
	 * 
	 * @param paxosID
	 * @param version
	 * @return Returns true if the paxos instance {@code paxosID:version} exists
	 *         and is stopped. If so, the instance will be deleted.
	 */
	public boolean deleteStoppedPaxosInstance(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version)
			// nothing to do here as stopped paxos instances are auto-killed
			if (pism.synchronizedNoop() && pism.isStopped())
				return true;
		return false;
	}

	/**
	 * @param paxosID
	 * @param version
	 * @return Returns true if the final state was successfully deleted or there
	 *         was nothing to delete.
	 */
	public boolean deleteFinalState(String paxosID, int version) {
		// might as well force-kill the instance at this point
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version)
			this.kill(pism);

		return this.paxosLogger.deleteEpochFinalCheckpointState(paxosID,
				version);
	}

	/**
	 * The Integer return value as opposed to int is convenient to say that
	 * there is no epoch.
	 * 
	 * @param paxosID
	 * @return Integer version of paxos instance named {@code paxosID}.
	 */
	public Integer getVersion(String paxosID) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null)
			return (int) pism.getVersion();
		else {
			return this.paxosLogger.getEpochFinalCheckpointVersion(paxosID);
		}
	}

	/**
	 * Forces a checkpoint, but not guaranteed to happen immediately.
	 * 
	 * @param paxosID
	 */
	public void forceCheckpoint(String paxosID) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null)
			pism.forceCheckpoint();
	}

	/**
	 * Specifies the level of reordering of decisions that prompts a
	 * sync-decisions request.
	 * 
	 * @param limit
	 */
	public void setOutOfOrderLimit(int limit) {
		this.outOfOrderLimit = Math.max(limit, 1);
	}

	protected int getOutOfOrderLimit() {
		return this.outOfOrderLimit;
	}

	/**
	 * @param interval
	 */
	public void setInterCheckpointInterval(int interval) {
		this.interCheckpointInterval = interval;
	}

	/**
	 * @return Inter-checkpoint interval.
	 */
	public int getInterCheckpointInterval() {
		return this.interCheckpointInterval;
	}

	/**
	 * @param maxGap
	 */
	public void setMaxSyncDecisionsGap(int maxGap) {
		this.maxSyncDecisionsGap = maxGap;
	}

	/**
	 * @return Maximum reordering among received decisions that when exceeded
	 *         prompts a checkpoint transfer as opposed to a sync-decisions
	 *         operation.
	 */
	public int getMaxSyncDecisionsGap() {
		return this.maxSyncDecisionsGap;
	}

	/**
	 * @return Returns true if null checkpoints are enabled.
	 */
	public boolean isNullCheckpointStateEnabled() {
		return this.nullCheckpointsEnabled;
	}

	/**
	 * Removes all persistent state. Just avoid using this deprecated method.
	 */
	@Deprecated
	protected synchronized void resetAll() {
		this.pinstances.clear();
		this.corpses.clear();
		this.paxosLogger.removeAll();
	}

	private static synchronized void open() {
		{
			closed = false;
		}
	}

	private static synchronized void closeAll() {
		{
			closed = true;
		}
	}

	private static synchronized boolean allClosed() {
		{
			return closed;
		}
	}

	private boolean isClosed() {
		return allClosed();
	}

	/**
	 * Gracefully closes PaxosManager.
	 */
	public void close() {
		/*
		 * The static method closeAll sets the closed flag so as to prevent any
		 * further new packet processing across all instances of PaxosManager.
		 */
		closeAll();

		/*
		 * The static method waitToFinishAll waits until the static method
		 * getProcessing returns true, i.e., there is some PaxosManager that has
		 * started processing a packet (via handlePaxosMessage) but not finished
		 * processing it. Once closeAll returns and then waitToFinishAll
		 * returns, there can be no ongoing or future packet processing by any
		 * instance of PaxosManager in this JVM.
		 */
		waitToFinishAll();

		/* Close logger, FD, messenger, request batcher, executor */
		this.batched.stop();
		this.paxosLogger.close();
		this.FD.close();
		this.messenger.stop();
		this.batched.stop();
		this.executor.shutdownNow();
	}

	/**
	 * @return Idle period after which paxos instances are paused.
	 */
	public static long getDeactivationPeriod() {
		return DEACTIVATION_PERIOD;
	}

	/********************* End of public methods ***********************/

	private final boolean emulateUnreplicated(JSONObject jsonMsg) {
		if (!EMULATE_UNREPLICATED)
			return false;
		try {
			if (!PaxosPacket.getPaxosPacketType(jsonMsg).equals(
					PaxosPacket.PaxosPacketType.REQUEST))
				return false;

			PaxosInstanceStateMachine.execute(null, myApp, (new RequestPacket(
					jsonMsg)).getRequestValues(), false);
		} catch (JSONException e) {
			log.severe(this + " unable to parse " + jsonMsg);
			e.printStackTrace();
		}
		return true;
	}

	protected Set<NodeIDType> getNodesFromStringSet(Set<String> strNodes) {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (String strNode : strNodes) {
			nodes.add(this.unstringer.valueOf(strNode));
		}
		return nodes;
	}

	/* Separate method only for instrumentation */
	private synchronized PaxosInstanceStateMachine getInstance(String paxosID,
			boolean tryHotRestore, boolean tryRestore) {
		// long methodEntryTime = System.currentTimeMillis();
		PaxosInstanceStateMachine pism = pinstances.get(paxosID);
		if (pism == null
				&& ((tryHotRestore && this.unpause(paxosID)) || (tryRestore && this
						.restore(paxosID))))
			pism = pinstances.get(paxosID);
		// DelayProfiler.updateDelay("getInstance", methodEntryTime);
		return pism;
	}

	private synchronized PaxosInstanceStateMachine getInstance(String paxosID) {
		return this.getInstance(paxosID, true, true);
	}

	private boolean isPauseEnabled() {
		return PAUSE_OPTION;
	}

	private boolean isHibernateable() {
		return HIBERNATE_OPTION;
	}

	/*
	 * For each paxosID in the logs, this method creates the corresponding paxos
	 * instance and rolls it forward from the last checkpointed state.
	 * 
	 * Synchronized because this method invokes an incremental read on the
	 * database that currently does not support parallelism. But the
	 * "synchronized" qualifier here is not necessary for correctness.
	 */
	private synchronized void initiateRecovery() {
		boolean found = false;
		int groupCount = 0, freq = 1;
		log.log(Level.INFO, "{0} beginning to recover checkpoints",
				new Object[] { this });
		while (this.paxosLogger.initiateReadCheckpoints(true))
			; // acquires lock
		RecoveryInfo pri = null;
		while ((pri = this.paxosLogger.readNextCheckpoint(true)) != null) {
			found = true;
			assert (pri.getPaxosID() != null);
			// start paxos instance, restore app state from checkpoint if any
			// and roll forward
			this.recover(pri.getPaxosID(), pri.getVersion(), this.myID,
					getNodesFromStringSet(pri.getMembers()), myApp,
					pri.getState());
			if ((++groupCount) % freq == 0) {
				freq *= 2;
			}
		}
		this.paxosLogger.closeReadAll(); // releases lock
		log.log(Level.INFO,
				"{0} has recovered checkpoints for {1} paxos groups",
				new Object[] { this, groupCount });
		if (!found) {
			log.warning("No checkpoint state found for node "
					+ this.myID
					+ ". This can only happen if\n"
					+ "(1) the node is newly joining the system, or\n(2) the node previously crashed before "
					+ "completing even a single checkpoint, or\n(3) the node's checkpoint was manually deleted.");
		}
		int logCount = 0;
		freq = 1;
		// roll forward all logged messages in a single pass
		if (ONE_PASS_RECOVERY) {
			log.log(Level.INFO,
					"{0} beginning to roll forward logged messages",
					new Object[] { this });
			while (this.paxosLogger.initiateReadMessages())
				; // acquires lock
			PaxosPacket packet = null;
			while ((packet = this.paxosLogger.readNextMessage()) != null) {
				try {
					log.log(Level.FINEST,
							"{0} rolling forward logged message {1}",
							new Object[] { this, packet });
					this.handlePaxosPacket(PaxosPacket.markRecovered(packet)
							.toJSONObject());
					if ((++logCount) % freq == 0) {
						freq *= 2;
					}
				} catch (JSONException je) {
					je.printStackTrace();
				}
			}
			this.paxosLogger.closeReadAll(); // releases lock
			log.log(Level.INFO,
					"{0} rolled forward {1} messages total across {2} paxos groups",
					new Object[] { this, logCount, groupCount });
		}

		this.hasRecovered = true;
		this.notifyRecovered();
		log.log(Level.INFO,
				"------------------{0} recovery complete-------------------",
				new Object[] { this });
	}

	protected boolean hasRecovered() {
		return this.hasRecovered;
	}

	protected boolean hasRecovered(PaxosInstanceStateMachine pism) {
		if (ONE_PASS_RECOVERY)
			return this.hasRecovered();
		else
			// !ONE_PASS_RECOVERY
			return (pism != null && pism.isActive());
	}

	/*
	 * All messaging is done using PaxosMessenger and MessagingTask. This method
	 */
	protected void send(MessagingTask mtask) throws JSONException, IOException {
		if (mtask == null)
			return;
		if (mtask instanceof LogMessagingTask) {
			AbstractPaxosLogger.logAndMessage(this.paxosLogger,
					(LogMessagingTask) mtask, this.messenger);
		} else {
			messenger.send(mtask);
		}
	}

	/*
	 * A clean kill completely removes all trace of the paxos instance (unlike
	 * pause or hibernate or an unclean kill). The copy of the instance kept in
	 * corpses will not be revived; in fact, is there temporarily only to make
	 * sure that the instance does not get re-created as a "missed birthing"
	 * instance creation.
	 * 
	 * synchronized because any changes to pinstances must be synchronized as
	 * createPaxosInstance and other methods also use it. Also, we want to move
	 * the paxos instance atomically from pinstances to corpses. If not atomic,
	 * it can result in the corpse (to-be) getting resurrected if a packet for
	 * the instance arrives in between.
	 * 
	 * Note: pause or hibernate or crashAndRecover like options just invoke
	 * softCrash and not kill that is more fatal. None of them do any of the
	 * following: (1) wipe out checkpoint state, (2) nullify app state, or (3)
	 * move pism to corpses, all of which kill does.
	 */
	protected synchronized void kill(PaxosInstanceStateMachine pism,
			boolean clean) {
		if (pism == null || this.isClosed())
			return;

		/*
		 * Do nothing if existing version not same as kill target. Note that if
		 * existing is null, we still kill pism. It is unlikely but possible for
		 * existing to be null, say, because the instance pism just got paused,
		 * but for pism or this paxos manager to issue a kill immediately after.
		 */
		PaxosInstanceStateMachine existing = this
				.getInstance(pism.getPaxosID());
		if (existing != null && pism.getVersion() != existing.getVersion()) {
			log.log(Level.INFO,
					"{0} not killing {1} because {2} already exists in the map",
					new Object[] { this, pism, existing });
			return;
		}

		// else got murder work to do (even if existing==null)
		while (!pism.kill(clean))
			log.severe("Problem stopping paxos instance " + pism.getPaxosID()
					+ ":" + pism.getVersion());
		this.softCrash(pism);
		this.corpses.put(pism.getPaxosID(), pism);
		executor.schedule(new Cremator(pism.getPaxosID(), this.corpses),
				MORGUE_DELAY, TimeUnit.MILLISECONDS);
		this.notifyUponKill();
	}

	/*
	 * Default kill is clean. Unclean kill is used by PaxosInstanceStateMachine
	 * when it is unable to execute an app request successfully. An unclean kill
	 * does not wipe out checkpoint state or app state, but does move the paxos
	 * instance to corpses because it is as good as dead at this point.
	 * 
	 * FIXME: Why not just do a softCrash instead of corpsing out in the case of
	 * an app execution error? The instance might get re-created as a "missed
	 * birthing" case, but that would still be safe and, maybe, the roll forward
	 * this time around might succeed.
	 */
	private void kill(PaxosInstanceStateMachine pism) {
		this.kill(pism, true);
	}

	// soft crash means stopping and "forgetting" the instance
	private synchronized void softCrash(PaxosInstanceStateMachine pism) {
		assert (pism != null);
		pism.forceStop();
		this.pinstances.remove(pism.getPaxosID());
	}

	/*
	 * For testing. Similar to hibernate but without forcing a checkpoint and
	 * followed immediately by a restore from the most recent (but possibly
	 * still quite old) checkpoint.
	 */
	protected void crashAndRecover(PaxosInstanceStateMachine pism) {
		if (pism == null || this.isClosed()
				|| !this.pinstances.containsValue(pism))
			return;
		log.warning(this + " crash-and-recovering paxos instance " + pism);
		this.softCrash(pism);
		this.recover(pism.getPaxosID(), pism.getVersion(), pism.getMyID(),
				this.integerMap.get(Util.arrayToIntSet(pism.getMembers())),
				pism.getApp()); // state will be auto-recovered
	}

	// Checkpoint and go to sleep on disk. Currently not used.
	protected synchronized boolean hibernate(PaxosInstanceStateMachine pism) {
		if (pism == null || this.isClosed()
				|| !this.pinstances.containsValue(pism)
				|| !this.isHibernateable())
			return false;

		// else hibernate
		boolean hibernated = false;
		log.log(Level.INFO, "{0} trying to hibernate {1}", new Object[] { this,
				pism });
		boolean stopped = pism.tryForcedCheckpointAndStop();

		if (stopped) {
			this.softCrash(pism);
			hibernated = true;
			log.log(Level.INFO, "{0} sucessfully hibernated {1}", new Object[] {
					this, pism });
		}
		return hibernated;
	}

	// Undo hibernate. Will rollback, so not very efficient.
	private synchronized boolean restore(String paxosID) {
		if (this.isClosed() || !this.isHibernateable())
			return false;
		PaxosInstanceStateMachine pism = null;
		if ((pism = this.pinstances.get(paxosID)) != null)
			return true;
		log.log(Level.INFO, "{0} trying to restore instance {1}", new Object[] {
				this, paxosID });
		RecoveryInfo pri = this.paxosLogger.getRecoveryInfo(paxosID);
		if (pri != null)
			pism = this.recover(paxosID, pri.getVersion(), this.myID,
					this.getNodesFromStringSet(pri.getMembers()), this.myApp,
					pri.getState());
		if (pism != null)
			log.log(Level.INFO,
					"{0} successfully restored hibernated instance {1}",
					new Object[] { this, pism });
		else
			log.log(Level.WARNING, "{0} unable to restore paxos instance {1}",
					new Object[] { this, paxosID });
		return pism != null;
	}

	// Like hibernate but without checkpointing or subsequent recovery
	protected synchronized boolean pause(PaxosInstanceStateMachine pism) {
		if (pism == null || this.isClosed()
				|| !this.pinstances.containsValue(pism) || !isPauseEnabled())
			return false;
		boolean paused = false;
		log.log(Level.FINE, "{0} trying to pause {1}", new Object[] { this,
				pism.getPaxosIDVersion() });

		// else try to pause
		long pauseInitTime = System.currentTimeMillis();
		paused = pism.tryPause();
		if (paused) {
			/*
			 * crash means the same as removing from pinstances as well as
			 * activePaxii for an already stopped paxos instance.
			 */
			assert (pism.isStopped());
			this.softCrash(pism);
			DelayProfiler.updateDelay("pause", pauseInitTime);
			log.log(Level.FINE, "{0} successfully paused {1}", new Object[] {
					this, pism.getPaxosIDVersion() });
		} else
			log.log(Level.FINE, "{0} failed to pause {1}", new Object[] { this,
					pism });
		return paused;
	}

	// Hot restores from disk, i.e., restores quickly without need for rollback
	private synchronized boolean unpause(String paxosID) {
		if (this.isClosed() || !this.hasRecovered() || !this.isPauseEnabled()
				|| this.pinstances.containsKey(paxosID))
			return false;
		boolean restored = false;

		long unpauseInitTime = System.currentTimeMillis();
		HotRestoreInfo hri = this.paxosLogger.unpause(paxosID);
		if (hri != null) {
			log.log(Level.FINE,
					"{0} successfully unpaused paused instance {1}",
					new Object[] { this, paxosID });
			restored = this.createPaxosInstance(hri.paxosID, hri.version,
					this.integerMap.get(Util.arrayToIntSet(hri.members)),
					this.myApp, null, hri, false);
		} else
			log.log(Level.FINE, "{0} unable to unpause instance {1}",
					new Object[] { this, paxosID });
		DelayProfiler.updateDelay("unpause", unpauseInitTime);
		return restored;
	}

	/*
	 * Create paxos instance restoring app state from checkpoint if any and roll
	 * forward.
	 */
	private PaxosInstanceStateMachine recover(String paxosID, int version,
			int id, Set<NodeIDType> members, InterfaceReplicable app,
			String state) {
		log.log(Level.FINE, "{0} {1}:{2} recovering", new Object[] { this,
				paxosID, version });
		this.createPaxosInstance(paxosID, version, (members), app, null, null,
				false);
		PaxosInstanceStateMachine pism = (this
				.getInstance(paxosID, true, false));
		return pism;
	}

	private PaxosInstanceStateMachine recover(String paxosID, int version,
			int id, Set<NodeIDType> members, InterfaceReplicable app) {
		// state will be auto-recovered if it exists
		return this.recover(paxosID, version, id, members, app, null);
	}

	/*
	 * After rollForward, recovery is complete. In particular, we don't have to
	 * wait for any more processing of messages, e.g., out of order decisions to
	 * "settle", because the only important thing is to replay and process
	 * ACCEPTs and PREPAREs so as to bring the acceptor state up to speed, which
	 * is a purely local and non-blocking sequence of operations. Coordinator
	 * state in general is not recoverable; the easiest way to recover it is to
	 * simply call checkRunForCoordinator, which will happen automatically upon
	 * the receipt of any external packet.
	 */
	private void rollForward(String paxosID) {
		if (!ONE_PASS_RECOVERY || this.hasRecovered()) {
			log.log(Level.FINE, "{0} {1} about to roll forward", new Object[] {
					this, paxosID });
			AbstractPaxosLogger.rollForward(paxosLogger, paxosID, messenger);
			PaxosInstanceStateMachine pism = (this.getInstance(paxosID, true,
					false));
			pism.setActive();
			assert (this.getInstance(paxosID, false, false) != null);
			// if (pism != null) pism.poke();
		}
		TESTPaxosConfig.setRecovered(this.myID, paxosID, true); // testing
	}

	private void findPaxosInstance(JSONObject jsonMsg, String paxosID)
			throws JSONException {
		if (!this.hasRecovered())
			return;
		/*
		 * If it is possible for there to be no initial state checkpoint, under
		 * missed birthing, an acceptor may incorrectly report its gcSlot as -1,
		 * and if a majority do so (because that majority consists all of missed
		 * birthers), a coordinator may propose a proposal for slot 0 even
		 * though an initial state does exist, which would end up overwriting
		 * the initial state. So we can not support ambiguity in whether there
		 * is initial state or not. If we force initial state checkpoints (even
		 * null state checkpoints) to always exist, so that missed birthers can
		 * always set the initial gcSlot to 0.
		 */
		if (!this.isNullCheckpointStateEnabled())
			return;
		PaxosInstanceStateMachine zombie = this.corpses.get(paxosID);
		if (jsonMsg.has(PaxosPacket.PAXOS_VERSION)) {
			int version = jsonMsg.getInt(PaxosPacket.PAXOS_VERSION);
			if (zombie == null || (zombie.getVersion() - version) < 0)
				findReplicaGroup(jsonMsg, paxosID, version);
		}
	}

	/*
	 * The two methods, heardFrom and isNodeUp, below are the only ones that
	 * invoke nodeMap.get(int). They are only invoked after the corresponding
	 * NodeIDType is already inserted in the map.
	 */
	protected void heardFrom(int id) {
		this.FD.heardFrom(this.integerMap.get(id));
	}

	protected boolean isNodeUp(int id) {
		return (FD != null ? FD.isNodeUp(this.integerMap.get(id)) : false);
	}

	protected boolean lastCoordinatorLongDead(int id) {
		return (FD != null ? FD
				.lastCoordinatorLongDead(this.integerMap.get(id)) : true);
	}

	protected long getDeadTime(int id) {
		return (FD != null ? FD.getDeadTime(this.integerMap.get(id)) : System
				.currentTimeMillis());
	}
	
	protected AbstractPaxosLogger getPaxosLogger() {
		return paxosLogger;
	}

	protected Messenger<NodeIDType> getMessenger() {
		return this.messenger;
	}

	/****************** Start of methods to gracefully finish processing **************/
	private static synchronized void setProcessing(boolean b) {
		if (b)
			processing++;
		else
			processing--;
		if (processing == 0)
			PaxosManager.class.notify();
	}

	private static synchronized boolean getProcessing() {
		return processing > 0;
	}

	protected static synchronized void waitToFinishAll() {
		try {
			while (getProcessing()) {
				PaxosManager.class.wait();
			}
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}

	private synchronized void timedWaitCanCreateOrExistsOrHigher(
			String paxosID, int version, long timeout) {
		try {
			if (!this.canCreateOrExistsOrHigher(paxosID, version))
				wait(timeout);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}

	// wait until timeout for creation
	private synchronized void timedWaitForExistence(String paxosID,
			int version, long timeout) {
		try {
			long waitStartTime = System.currentTimeMillis();
			while (!this.equalOrHigherVersionExists(paxosID, version)
					&& System.currentTimeMillis() - waitStartTime < timeout)
				wait(timeout);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}

	protected synchronized void waitCanCreateOrExistsOrHigher(String paxosID,
			int version) {
		try {
			while (!this.canCreateOrExistsOrHigher(paxosID, version))
				wait();
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}

	}

	protected synchronized void notifyUponKill() {
		notifyAll();
	}

	private final Object createLock = new Object();

	protected synchronized void notifyUponCreation() {
		synchronized (this.createLock) {
			notifyAll();
		}
	}

	/**
	 * This is a common default create paxos instance creation method, i.e., we
	 * wait for lower versions to be killed if necessary but, after a timeout,
	 * create the new instance forcibly anyway. This method won't create the
	 * instance if the same or higher version already exists. One concern with
	 * this method is that force killing lower versions in order to start higher
	 * versions can cause the epoch final state to be unavailable at *any* node
	 * in immediately lower version. It is okay to kill even an immediately
	 * preceding instance only if we already have the necessary state
	 * (presumably obtained from a stopped immediately preceding instance at
	 * some other group member). In the reconfiguration protocol, we must ensure
	 * this by starting a new epoch only after receiving a successful
	 * acknowledgment for stopping the previous epoch. However, if say only one
	 * node has executed the previous epoch's stop and then starts the new epoch
	 * and causes other new epoch replicas to force-create the new epoch, and
	 * then crashes, the new epoch is stuck.
	 * 
	 * @param paxosID
	 * @param version
	 * @param gms
	 * @param app
	 * @param state
	 * @param timeout
	 * @return Returns true if this paxos instance or one with a higher version
	 *         number was successfully created.
	 */
	public boolean createPaxosInstanceForcibly(String paxosID, int version,
			Set<NodeIDType> gms, InterfaceReplicable app, String state,
			long timeout) {
		if (timeout < CAN_CREATE_TIMEOUT)
			timeout = CAN_CREATE_TIMEOUT;
		// still do a timed wait
		this.timedWaitCanCreateOrExistsOrHigher(paxosID, version, timeout);
		if (this.instanceExistsOrHigher(paxosID, version))
			return true;
		// else
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);

		if (pism != null && pism.getVersion() - version < 0) {
			log.log(Level.INFO,
					"{0} forcibly killing {1} in order to create {2}:{3}",
					new Object[] { this, pism.getPaxosIDVersion(), paxosID,
							version });
			this.kill(pism); // force kill
		}
		boolean created = this.createPaxosInstance(paxosID, version, gms, app,
				state);
		return created;
	}

	/**
	 * @param paxosID
	 * @param version
	 * @return Returns true if the paxos instance {@code paxosID:version} exists
	 *         or a higher version exists.
	 */
	public boolean equalOrHigherVersionExists(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && (pism.getVersion() - version >= 0))
			return true;
		return false;
	}

	/**
	 * Unlike the method {@link #equalOrHigherVersionExists(String, int)}, this
	 * method checks for whether an equal or higher version was previously
	 * stopped. We need this check to prevent going back in time, i.e., starting
	 * an epoch after it has been stopped.
	 * 
	 * @param paxosID
	 * @param version
	 * @return
	 */
	private boolean equalOrHigherVersionStopped(String paxosID, int version) {
		// lastVersion may be active or recently stopped
		Integer lastStoppedVersion = this.paxosLogger
				.getEpochFinalCheckpointVersion(paxosID);
		if (lastStoppedVersion != null && lastStoppedVersion - version >= 0)
			return true;
		return false;
	}

	/****************** End of methods to gracefully finish processing **************/

	private String printLog(String paxosID) {
		return ("State for " + paxosID + ": Checkpoint: " + this.paxosLogger
				.getStatePacket(paxosID));
	}

	// send a request asking for your group
	private void findReplicaGroup(JSONObject msg, String paxosID, int version)
			throws JSONException {
		FindReplicaGroupPacket findGroup = new FindReplicaGroupPacket(
				this.myID, msg); // paxosID and version should be within
		int nodeID = FindReplicaGroupPacket.getNodeID(msg);
		if (nodeID >= 0) {
			try {
				log.log(Level.INFO,
						"{0} received paxos message for non-existent instance {1}; contacting {2} for help",
						new Object[] { this, paxosID, nodeID });
				this.send(new MessagingTask(nodeID, findGroup));
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		} else
			log.log(Level.FINE, "{0} can't find group member in {1}:{2}: {3}",
					new Object[] { this, paxosID, version, msg });
	}

	// process a request or send an answer
	private void processFindReplicaGroup(FindReplicaGroupPacket findGroup)
			throws JSONException {
		MessagingTask mtask = null;
		if (findGroup.group == null && findGroup.nodeID != this.myID) {
			// process a request
			PaxosInstanceStateMachine pism = this.getInstance(findGroup
					.getPaxosID());
			if (pism != null && pism.getVersion() == findGroup.getVersion()) {
				FindReplicaGroupPacket frgReply = new FindReplicaGroupPacket(
						pism.getMembers(), findGroup);
				mtask = new MessagingTask(findGroup.nodeID, frgReply);
			}
		} else if (findGroup.group != null && findGroup.nodeID == this.myID) {
			// process an answer
			PaxosInstanceStateMachine pism = this.getInstance(findGroup
					.getPaxosID());
			if (pism == null
					|| (pism.getVersion() - findGroup.getVersion()) < 0) {
				// wait to see if it gets created anyway; FIXME: needed?
				this.timedWaitForExistence(findGroup.getPaxosID(),
						findGroup.getVersion(), WAIT_TO_GET_CREATED_TIMEOUT);

				// wait and kill lower versions if any
				if (pism != null
						&& (pism.getVersion() - findGroup.getVersion() < 0)) {
					// wait to see if lower versions go away anyway
					this.timedWaitCanCreateOrExistsOrHigher(
							findGroup.getPaxosID(), findGroup.getVersion(),
							CAN_CREATE_TIMEOUT);
					this.kill(pism);
				}

				// create out of "nothing"
				boolean created = this.createPaxosInstance(findGroup
						.getPaxosID(), findGroup.getVersion(), this.integerMap
						.get(Util.arrayToIntSet(findGroup.group)), myApp, null,
						null, false, true);
				if (created)
					log.log(Level.INFO,
							"{0} created paxos instance {1}:{2} from nothing because it apparently missed its birthing",
							new Object[] { this, findGroup.getPaxosID(),
									findGroup.getVersion() });
			}
		}
		try {
			if (mtask != null)
				this.send(mtask);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/*************************** Start of activePaxii related methods **********************/

	private void deactivate() {
		if (isClosed() || this.pinstances.size() == 0)
			return;

		long t0 = System.currentTimeMillis();
		log.log(Level.FINE,
				"{0} initiating deactivation attempt, |activePaxii| = {1}",
				new Object[] { this, this.pinstances.size() });
		ArrayList<String> paused = new ArrayList<String>();

		// cuckoo hashmap now supports an efficient iterator
		for (Iterator<PaxosInstanceStateMachine> pismIter = this.pinstances
				.concurrentIterator(); pismIter.hasNext();) {
			PaxosInstanceStateMachine pism = pismIter.next();

			String paxosID = pism.getPaxosID();

			if (pism.isLongIdle()) {
				log.log(Level.FINER, "{0} trying to pause {1} [{2}]",
						new Object[] { this, paxosID, pism });
				/*
				 * The sync below ensures that, at least once every deactivation
				 * period, we sync decisions for an active paxos instance. This
				 * is handy when a paxos instance is not caught up but doesn't
				 * get any new messages either, say, because nothing new is
				 * happening, then it has no reason to do anything but will
				 * remain unnecessarily active; the sync here allows it to
				 * potentially catch up and possibly be paused in the next
				 * deactivation round if there is still no action by then.
				 * 
				 * The sync is also useful irrespective of whether or not the
				 * instance is caught up for pausability as it simply allows all
				 * active paxos instances to infrequently check for missing
				 * decisions even if no new decisions are being generated.
				 * 
				 * Overhead: This sync imposes a message overhead of up to A
				 * messages A is the number of active paxos instances. For
				 * example, with 10K active instances, this method could send
				 * 10K messages, which is high. However, for each instance, the
				 * sync message will get sent only if it has not recently sent a
				 * sync message *and* it is out of sync or it has just started
				 * up and has a very low outOfOrder limit. Consequently, we
				 * should avoid having a large number of paxos instances with a
				 * very low outOfOrderLimit, especially if all they plan to do
				 * is to start up and do nothing, otherwise, they will cause a
				 * one-time deluge of sync messages before being paused.
				 * 
				 * If active instances are generally busy but out of sync, we
				 * could impose a bandwidth overhead of A/D where D is the
				 * deactivation thread's period, e.g., A=10K, D=30secs => an
				 * overhead of 333 messages/sec, which although seems high is
				 * possible only if none of those paxos instances sent a sync
				 * reauest in the last S seconds, where S is the minimum
				 * inter-sync interval for each instance (default 1 second). In
				 * expectation, a high overhead relative to the inevitable paxos
				 * commit overhead (of 3 messages per replica per decision) is
				 * unlikely unless the workload and network behave
				 * adversarially, i.e., in every D period, each active paxos
				 * instance executes just enough decisions for it to be possible
				 * for its outOfOrder threshold to be triggered and the network
				 * reorders some of those decisions. If the instance commits
				 * many more decisions than the outOfOrder threshold, then the
				 * sync message adds only a small relative overhead, e.g., if
				 * the outOfOrder threshold is 10, roughly 33 messages (accept,
				 * acceptReply, decision) would be required to commit at least
				 * 11 decisions that would then trigger just one sync message.
				 * If the outOfOrder threshold is 1, then the sync message could
				 * add one message to every 6 expected messages (for 2 paxos
				 * commits) at this replica, a ~15% overhead. But with such a
				 * low outOfOrder threshold, we should not be having a large
				 * number of paxos instances in the first place.
				 */
				this.syncPaxosInstance(pism, false);
				if (pause(pism))
					paused.add(paxosID);
			}
		}
		DelayProfiler.updateDelay("deactivator", t0);
		log.log(paused.size() > 0 ? Level.INFO : Level.FINE,
				"{0} deactivated {1} idle instances {2}; has {3} active instances; average_pause_delay = {4};"
						+ " average_deactivation_delay = {5}",
				new Object[] { this, paused.size(),
						Util.truncatedLog(paused, PRINT_LOG_SIZE),
						this.pinstances.size(),
						Util.mu(DelayProfiler.get("pause")),
						Util.mu(DelayProfiler.get("deactivationLoop")) });
	}

	private static final int PRINT_LOG_SIZE = 16;

	/*************************** End of activePaxii related methods **********************/

	private class Cremator implements Runnable {
		String id = null;
		HashMap<String, PaxosInstanceStateMachine> map = null;

		Cremator(String paxosID,
				HashMap<String, PaxosInstanceStateMachine> zombies) {
			this.id = paxosID;
			this.map = zombies;
		}

		public void run() {
			synchronized (map) {
				map.remove(id);
			}
		}
	}

	/*
	 * Both deactivates, i.e., removes temporary active paxos state, and pauses,
	 * i.e., swaps safety-critical paxos state to disk.
	 */
	private class Deactivator implements Runnable {
		public void run() {
			try {
				deactivate();
			} catch (Exception e) {
				// must continue running despite any exceptions
				e.printStackTrace();
			}
		}
	}

	protected int getMyID() {
		return this.myID;
	}

	private synchronized void waitToRecover() {
		try {
			this.wait();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private synchronized void notifyRecovered() {
		this.notifyAll();
	}

	// convert string -> NodeIDType -> int (can *NOT* convert string directly to
	// int)
	private JSONObject fixNodeStringToInt(JSONObject json) throws JSONException {
		// FailureDetectionPacket already has generic NodeIDType
		if (PaxosPacket.getPaxosPacketType(json) == PaxosPacket.PaxosPacketType.FAILURE_DETECT)
			return json;

		if (json.has(PaxosPacket.NodeIDKeys.BALLOT.toString())) {
			// fix ballot string
			String ballotString = json.getString(PaxosPacket.NodeIDKeys.BALLOT
					.toString());
			Integer coordInt = this.integerMap.put(this.unstringer
					.valueOf(Ballot.getBallotCoordString(ballotString)));
			assert (coordInt != null);
			Ballot ballot = new Ballot(Ballot.getBallotNumString(ballotString),
					coordInt);
			json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(),
					ballot.toString());
		} else if (json.has(PaxosPacket.NodeIDKeys.GROUP.toString())) {
			// fix group string (JSONArray)
			JSONArray jsonArray = json
					.getJSONArray(PaxosPacket.NodeIDKeys.GROUP.toString());
			for (int i = 0; i < jsonArray.length(); i++) {
				String memberString = jsonArray.getString(i);
				int memberInt = this.integerMap.put(this.unstringer
						.valueOf(memberString));
				jsonArray.put(i, memberInt);
			}
		} else
			for (PaxosPacket.NodeIDKeys key : PaxosPacket.NodeIDKeys.values()) {
				if (json.has(key.toString())) {
					// fix default node string
					String nodeString = json.getString(key.toString());
					int nodeInt = this.integerMap.put(this.unstringer
							.valueOf(nodeString));
					json.put(key.toString(), nodeInt);
				}
			}
		return json;
	}

	public String toString() {
		return this.getClass().getSimpleName() + this.integerMap.get(myID);
	}

	/**
	 * This option is currently available only as a static option that applies
	 * to all (possibly > 1) instances of PaxosManager running on a machine.
	 * This option is primarily for testing. If apps go back and forth with
	 * enabling and disabling logging, it will result in safety violations
	 * 
	 * @return Disables persistent logging.
	 */
	public static boolean disableLogging() {
		return SQLPaxosLogger.disableLogging();
	}

	protected String intToString(int id) {
		return this.integerMap.get(id).toString();
	}

	/* ********************** Testing methods below ********************* */
	/**
	 * @return Logger used by PaxosManager.
	 */
	public static Logger getLogger() {
		return Logger.getLogger(PaxosManager.class.getName().replace(
				"PaxosManager", ""));
	}

	private void testingInitialization() {
		if (TESTPaxosConfig.getCleanDB())
			while (!this.paxosLogger.removeAll())
				;
	}

	/**
	 * This test method is deprecated and will either be removed or
	 * significantly revamped. Use TESTPaxosMain instead to run a single machine
	 * test with multiple virtual nodes.
	 * 
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws JSONException
	 */
	@Deprecated
	static void test(String[] args) throws InterruptedException, IOException,
			JSONException {
		int[] members = TESTPaxosConfig.getDefaultGroup();
		int numNodes = members.length;

		SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>(2000);
		snc.localSetup(Util.arrayToIntSet(members));

		@SuppressWarnings("unchecked")
		PaxosManager<Integer>[] pms = new PaxosManager[numNodes];
		TESTPaxosReplicable[] apps = new TESTPaxosReplicable[numNodes];

		/*
		 * We always test with the first member crashed. This also ensures that
		 * the system is fault-tolerant to the failure of the default
		 * coordinator, which in our policy is the first (or lowest numbered)
		 * node.
		 */
		TESTPaxosConfig.crash(members[0]);
		/*
		 * We disable sending replies to client in PaxosManager's unit-test. To
		 * test with clients, we rely on other tests in TESTPaxosMain
		 * (single-machine) or on TESTPaxosNode and TESTPaxosClient for
		 * distributed testing.
		 */
		TESTPaxosConfig.setSendReplyToClient(false);

		/*
		 * This setting is "guilty until proven innocent", i.e., each node will
		 * start out assuming that all other nodes are dead. This is probably
		 * too pessimistic as it will cause every node to run for coordinator
		 * when it starts up but is good for testing.
		 */
		FailureDetection.setParanoid();

		// Set up paxos managers and apps with nio
		for (int i = 0; i < numNodes; i++) {
			System.out.println("Initiating PaxosManager at node " + members[i]);
			JSONNIOTransport<Integer> niot = new JSONNIOTransport<Integer>(
					members[i], snc, new PacketDemultiplexerDefault(), true);
			apps[i] = new TESTPaxosReplicable(niot); // app, PM reuse nio
			pms[i] = new PaxosManager<Integer>(members[i], snc, niot, apps[i],
					null);
		}

		System.out.println("Initiated all " + numNodes
				+ " paxos managers with failure detectors..\n");

		/*
		 * We don't rigorously test with multiple groups as they are
		 * independent, but this is useful for memory testing.
		 */
		int numPaxosGroups = 2;
		String[] names = new String[numPaxosGroups];
		for (int i = 0; i < names.length; i++)
			names[i] = "paxos" + i;

		System.out.println("Creating " + numPaxosGroups
				+ " paxos groups each with " + numNodes
				+ " members each, one each at each of the " + numNodes
				+ " nodes");
		for (int node = 0; node < numNodes; node++) {
			int k = 1;
			for (int group = 0; group < numPaxosGroups; group++) {
				// creating a paxos instance may induce recovery from disk
				pms[node].createPaxosInstance(names[group], 0,
						Util.arrayToIntSet(members), apps[node], null, null,
						false);
				if (numPaxosGroups > 1000
						&& ((group % k == 0 && ((k *= 2) > 0)) || group % 100000 == 0)) {
					System.out.print(group + " ");
				}
			}
			System.out.println("..node" + members[node] + " done");
		}
		Thread.sleep(1000);

		/*
		 * Wait for all paxos managers to finish recovery. Recovery is finished
		 * when initiateRecovery() is complete. At this point, all the paxos
		 * groups at that node would have also rolled forward.
		 */
		int maxRecoverySlot = -1;
		int maxRecoveredNode = -1;
		for (int i = 0; i < numNodes; i++) {
			while (!TESTPaxosConfig.isCrashed(members[i])
					&& !TESTPaxosConfig.getRecovered(members[i], names[0])) {
				log.info("Waiting for node " + members[i] + " to recover ");
				pms[i].waitToRecover();
			}
			log.info("Node" + members[i]
					+ " finished recovery including rollback;\n" + names[0]
					+ " recovered at slot " + apps[i].getNumCommitted(names[0]));
			// need max recovery slot for names[0] below
			maxRecoverySlot = Math.max(maxRecoverySlot,
					apps[i].getNumCommitted(names[0]));
			maxRecoveredNode = i;
		}

		System.out.println("all nodes done creating groups.");

		/*********** Finished creating paxos instances for testing *****************/

		/************* Begin ClientRequestTask **************************/
		ScheduledExecutorService execpool = Executors.newScheduledThreadPool(5);
		class ClientRequestTask implements Runnable {
			private final RequestPacket request;
			private final PaxosManager<Integer> paxosManager;

			ClientRequestTask(RequestPacket req, PaxosManager<Integer> pm) {
				request = req;
				paxosManager = pm;
			}

			public void run() {
				try {
					JSONObject reqJson = request.toJSONObject();
					JSONPacket.putPacketType(reqJson,
							PaxosPacketType.PAXOS_PACKET.getInt());
					paxosManager.propose(request.getPaxosID(), request);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		}
		/************* End ClientRequestTask **************************/

		/*
		 * Create and schedule requests. All requests are scheduled immediately
		 * to test concurrency
		 */
		int numRequests = 1000;
		RequestPacket[] reqs = new RequestPacket[numRequests];
		ScheduledFuture<?>[] futures = new ScheduledFuture[numRequests];
		int numExceptions = 0;
		double scheduledDelay = 0;
		for (int i = 0; i < numRequests; i++) {
			reqs[i] = new RequestPacket(0, i,
					"[ Sample write request numbered " + i + " ]", false);
			reqs[i].putPaxosID(names[0], 0);
			JSONObject reqJson = reqs[i].toJSONObject();
			JSONPacket.putPacketType(reqJson,
					PaxosPacketType.PAXOS_PACKET.getInt());
			try {
				ClientRequestTask crtask = new ClientRequestTask(reqs[i],
						pms[1]);
				futures[i] = (ScheduledFuture<?>) execpool.schedule(crtask,
						(long) scheduledDelay, TimeUnit.MILLISECONDS);
				scheduledDelay += 0;
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
		/*
		 * Any exceptions below could occur because of exceptions inside paxos.
		 * Scheduling a request will invoke PaxosManager.propose() that will
		 * cause it to send the request to the corresponding
		 * PaxosInstanceStateMachine.
		 */
		log.info("Waiting for request scheduling to complete.");
		for (int i = 0; i < numRequests; i++) {
			try {
				futures[i].get();
			} catch (Exception e) {
				e.printStackTrace();
				numExceptions++;
			}
		}
		log.info("Request scheduling complete; numExceptions=" + numExceptions);
		Thread.sleep(1000);

		/*
		 * Wait for scheduled requests to finish being processed by paxos. We
		 * check for this by checking that at least one node has executed up to
		 * the slot number maxRecoverySlot + numRequests.
		 */
		while (apps[maxRecoveredNode].getNumCommitted(names[0]) < maxRecoverySlot
				+ numRequests) {
			apps[maxRecoveredNode].waitToFinish();
			;
		}
		log.info("Node" + maxRecoveredNode + " has executed up to slot "
				+ (maxRecoverySlot + numRequests));

		/*
		 * The code below waits for all uncrashed replicas to finish executing
		 * up to the same slot and will then assert the SMR invariant, i.e.,
		 * they all made the same state transitions up to that slot.
		 */
		int numCommitted = 0;
		for (int i = 0; i < numNodes; i++) {
			for (int j = i + 1; j < numNodes; j++) {
				if (TESTPaxosConfig.isCrashed(members[i])
						|| TESTPaxosConfig.isCrashed(members[j]))
					continue; // ignore crashed nodes

				int committed1 = apps[i].getNumCommitted(names[0]);
				int committed2 = apps[j].getNumCommitted(names[0]);
				// Wait for the other node to catch up
				while (committed1 != committed2) {
					if (committed1 > committed2)
						apps[j].waitToFinish(names[0], committed1);
					else if (committed1 < committed2)
						apps[i].waitToFinish(names[0], committed2);
					log.info("Waiting : (slot1,hash1)=(" + committed1 + ","
							+ apps[i].getHash(names[0]) + "(; (slot2,hash2="
							+ committed2 + "," + apps[j].getHash(names[0])
							+ ")");
					Thread.sleep(1000);
					committed1 = apps[i].getNumCommitted(names[0]);
					committed2 = apps[j].getNumCommitted(names[0]);
				}
				// Both nodes caught up to the same slot
				assert (committed1 == committed2) : "numCommitted@" + i + "="
						+ committed1 + ", numCommitted@" + j + "=" + committed2;
				// Assert state machine replication invariant
				numCommitted = apps[i].getNumCommitted(names[0]);
				assert (apps[i].getHash(names[0]) == apps[j].getHash(names[0])) : ("Waiting : (slot1,hash1)=("
						+ committed1
						+ ","
						+ apps[i].getHash(names[0])
						+ "(; (slot2,hash2="
						+ committed2
						+ ","
						+ apps[j].getHash(names[0]) + ")");
				; // end of SMR invariant
			}
		}

		/*
		 * Print preempted requests if any. These could happen during
		 * coordinator changes. Preempted requests are converted to no-ops and
		 * forwarded to the current presumed coordinator by paxos.
		 */
		String preemptedReqs = "[ ";
		int numPreempted = 0;
		for (int i = 0; i < numRequests; i++) {
			if (!TESTPaxosConfig.isCommitted(reqs[i].requestID)) {
				preemptedReqs += (i + " ");
				numPreempted++;
			}
		}
		preemptedReqs += "]";

		System.out
				.println("\n\nTest completed. Executed "
						+ numCommitted
						+ " requests consistently including "
						+ (numRequests - numPreempted)
						+ " of "
						+ numRequests
						+ " received requests;\nPreempted requests = "
						+ preemptedReqs
						+ "; numExceptions="
						+ numExceptions
						+ "; average message log time="
						+ Util.df(DelayProfiler.get("logDelay"))
						+ "ms.\n"
						+ "\nNote that it is possible for the test to be successful even if the number of consistently\n"
						+ "executed requests is less than the number of received requests as paxos only guarantees\n"
						+ "consistency, i.e., that all replicas executed requests in the same order, not that all requests\n"
						+ "issued will get executed. The latter property can be achieved by clients reissuing requests\n"
						+ "until successfully executed. With reissuals, clients do need to worry about double execution,\n"
						+ "so they should be careful. A client is not guaranteed to get a failure message if the request fails,\n"
						+ "e.g., if the replica receiving a request dies immediately. If the client uses a timeout to detect\n"
						+ "failure and thereupon reissue its request, it is possible that both the original and re-issued\n"
						+ "requests are executed. Clients can get around this problem by using sequence numbers within\n"
						+ "their app, reading the current sequence number, and then trying to commit their write provided the\n"
						+ "sequence number has not changed in the meantime. There are other alternatives, but all of these\n"
						+ "are application-specific; they are not paxos's problem\n");
		for (int i = 0; i < numNodes; i++) {
			System.out.println(pms[i].printLog(names[0]));
		}
		execpool.shutdownNow();
		for (PaxosManager<Integer> pm : pms)
			pm.close();
	}
}
