package edu.umass.cs.gigapaxos.testing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.ProposalPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.RequestInstrumenter;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.InterfaceNIOTransport;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         The default no-op application for gigapaxos testing. It simply echoes
 *         back the request to the client. But it does a number of other
 *         instrumentations and asserts for testing.
 */
@SuppressWarnings("javadoc")
public class TESTPaxosReplicable implements InterfaceReplicable {
	public static final int MAX_STORED_REQUESTS = 1000;
	private MessageDigest md = null;
	private InterfaceNIOTransport<Integer, JSONObject> niot = null;

	private HashMap<String, PaxosState> allState = new HashMap<String, PaxosState>();

	private class PaxosState {
		private int seqnum = -1;
		private String value = null;// "Initial state";
		private int numExecuted = 0;
		private HashMap<Integer, String> committed = new HashMap<Integer, String>();
	}

	private static Logger log = Logger.getLogger(TESTPaxosReplicable.class
			.getName());

	// PaxosManager.getLogger();

	public TESTPaxosReplicable(JSONNIOTransport<Integer> nio) {
		this();
		try {
			/*
			 * app uses nio only to send, not receive, so it doesn't care to set
			 * a PacketDemultiplexer
			 */
			setNIOTransport(nio);
			AllApps.addApp(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// private because nio is necessary for testing
	private TESTPaxosReplicable() {
		// app uses nio only to send, not receive, so no PacketDemultiplexer
		try {
			md = MessageDigest.getInstance("SHA");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	/*
	 * This is the main execution method. The app is supposed to be agnostic to
	 * slot numbers, but this method takes as input a ProposalPacket as opposed
	 * to a RequestPacket only for tetsing purposes.
	 */
	public synchronized boolean handleDecision(ProposalPacket requestPacket,
			boolean doNotReplyToClient) {
		boolean executed = false;
		try {
			String paxosID = requestPacket.getPaxosID();
			PaxosState state = this.allState.get(paxosID);
			if (state == null)
				state = new PaxosState();

			/*
			 * Initialize seqnum upon first decision. We know it is the first
			 * decision if seqnum==-1 or if putState is true, i.e., checkpoint
			 * recovery has just happened and no other request has been
			 * executed.
			 */
			if (state.seqnum == -1)
				state.seqnum = requestPacket.slot;

			log.log(Level.FINE,
					"Node{0} executing {1}; seqnum={2}, prev_state={3}",
					new Object[] { (this.niot != null ? getMyID() : "[?]"),
							requestPacket.getSummary(), state.seqnum,
							Util.truncate(state.value, 16, 16)});

			/*
			 * Set state to current request value concatenated with the hash of
			 * the previous state. This allows us to easily compare using just
			 * the current state value that two RSMs executed the exact same set
			 * of state transitions to arrive at that state.
			 */
			state.value = requestPacket.requestValue + (digest(state.value));

			/*
			 * Assert that the next slot is always the next expected seqnum.
			 * This ensures that paxos is making the app execute requests in the
			 * correct slot number order. Note that state.seqnum is set exactly
			 * once at initialization to the arriving request's slot above and
			 * is then just incremented by one below for every executed
			 * decision.
			 */
			assert (state.seqnum == requestPacket.slot) : "state.seqnum = "
					+ state.seqnum + " , requestPacket.slot = "
					+ requestPacket.slot;
			/*
			 * increment seqnum (to the next expected seqnum and
			 * requestPacket.slot)
			 */
			state.committed.put(state.seqnum++, state.value);
			allState.put(paxosID, state); // needed if we initialized state
											// above
			executed = true;
			state.numExecuted++;

			if (TESTPaxosConfig.shouldAssertRSMInvariant())
				assert (RSMInvariant(requestPacket.getPaxosID(),
						requestPacket.slot)) : requestPacket;
			state.committed.remove(state.seqnum - MAX_STORED_REQUESTS); // GC

			// testing and logging below
			this.notify(); // needed for paxos manager's unit-test
			assert (requestPacket.requestID >= 0) : requestPacket.toString();
			if (!doNotReplyToClient && niot != null
					&& TESTPaxosConfig.getSendReplyToClient()) {
				this.sendResponseToClient(requestPacket);
			} else
				log.log(Level.FINE, "Node{0} not sending reply {1} to client",
						new Object[] { getMyID(), requestPacket.getSummary() });
		} catch (JSONException je) {
			je.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return executed;
	}

	private void sendResponseToClient(ProposalPacket requestPacket)
			throws JSONException, IOException {
		log.log(Level.FINE, "App {0} sending response to client {0}",
				new Object[] { getMyID(), requestPacket.getSummary() });
		RequestInstrumenter.remove(requestPacket.requestID);
		/*
		 * Entry replica check must be done here as requests with different
		 * entry replicas can get batched when forwarded between replicas.
		 */
		if (requestPacket.getEntryReplica() == this.getMyID())
			niot.sendToAddress(
					new InetSocketAddress(requestPacket.getClientAddress(),
							requestPacket.getClientPort()), requestPacket
							.toJSONObject());
		// send responses for batched requests as well
		if (requestPacket.getBatched() != null)
			for (RequestPacket batchedReq : requestPacket.getBatched())
				// single-level recursion as batches can not be deeper
				this.sendResponseToClient(new ProposalPacket(
						requestPacket.slot, batchedReq));
	}

	@Override
	public synchronized String getState(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if (state != null)
			return state.value;
		return null;
	}

	@Override
	public synchronized boolean updateState(String paxosID, String value) {
		PaxosState state = this.allState.get(paxosID);
		if (state == null)
			state = new PaxosState();
		state.value = value;
		state.seqnum = -1; // reset seqnum upon state transfer
		allState.put(paxosID, state);
		return true;
	}

	public int digest(String s) {
		// null check needed if null checkpoints are enabled
		if (s == null)
			return 0;
		md.update(s.getBytes());
		byte[] digest = md.digest();
		int dig = 0;
		for (int i = 0; i < digest.length; i++) {
			dig += (int) digest[i];
		}
		return dig;
	}

	public void shutdown() {
		this.allState.clear();
	}

	private InterfaceNIOTransport<Integer, JSONObject> setNIOTransport(
			InterfaceNIOTransport<Integer, JSONObject> nio) {
		niot = nio;
		return nio;
	}

	private int getMyID() {
		return (this.niot != null ? this.niot.getMyID() : -1);
	}

	/*
	 * Testing methods below.
	 */
	public synchronized int getNumCommitted(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if (state != null)
			return state.seqnum;
		return 0;
	}

	public synchronized int getNumExecuted(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if (state != null)
			return state.numExecuted;
		return 0;
	}

	public synchronized String getRequest(String paxosID, int reqnum) {
		PaxosState state = this.allState.get(paxosID);
		if (state != null)
			return state.committed.get(reqnum);
		return null;
	}

	public synchronized int getHash(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if (state != null)
			return state.value.hashCode();
		return 0;
	}

	public synchronized void waitToFinish() throws InterruptedException {
		this.wait();
	}

	public synchronized void waitToFinish(String paxosID, int slot)
			throws InterruptedException {
		PaxosState state = null;
		while ((state = this.allState.get(paxosID)) == null
				|| state.seqnum < slot)
			this.wait();
	}

	public synchronized String toString(String paxosID) {
		String s = "";
		PaxosState state = this.allState.get(paxosID);
		s += "[App" + this.niot.getMyID() + ":" + paxosID + ": " + "seqnum="
				+ (state != null ? state.seqnum : -1) + "; state="
				+ (state != null ? state.value : "null") + ";]";
		return s;
	}

	public boolean RSMInvariant(TESTPaxosReplicable app1,
			TESTPaxosReplicable app2, String paxosID, int seqnum) {
		// invariant makes sense only when not recovery
		if (!TESTPaxosConfig.getCleanDB())
			return true;
		String state1 = null, state2 = null;
		if (app1.allState.containsKey(paxosID))
			state1 = app1.allState.get(paxosID).committed.get(seqnum);
		if (app2.allState.containsKey(paxosID))
			state2 = app2.allState.get(paxosID).committed.get(seqnum);
		assert (state1 == null || state2 == null || state1.equals(state2) || hasHoles(
				app1, app2, paxosID, seqnum)) : (getTrace(app1, app2, paxosID,
				seqnum));
		return (state1 == null || state2 == null || state1.equals(state2));
	}

	// legitimate holes can be caused because of checkpoint transfers
	private boolean hasHoles(TESTPaxosReplicable app1,
			TESTPaxosReplicable app2, String paxosID, int seqnum) {
		for (int i = seqnum; i >= 0; i--)
			if (app1.allState.get(paxosID).committed.get(i) == null
					&& app2.allState.get(paxosID).committed.get(i) != null
					|| app2.allState.get(paxosID).committed.get(i) == null
					&& app1.allState.get(paxosID).committed.get(i) != null)
				return true;
		return false;
	}

	private String getTrace(TESTPaxosReplicable app1, TESTPaxosReplicable app2,
			String paxosID, int seqnum) {
		String s = "";
		for (int i = seqnum; i >= 0; i--) {
			s += app1.getMyID() + ":" + paxosID + ":" + i + ": "
					+ app1.allState.get(paxosID).committed.get(i) + " != "
					+ app2.getMyID() + ":" + paxosID + ":" + i + ": "
					+ app2.allState.get(paxosID).committed.get(i) + "\n";
		}
		return s;
	}

	// check invariant at seqnum
	public boolean RSMInvariant(String paxosID, int seqnum) {
		boolean invariant = true;
		TESTPaxosReplicable[] replicas = AllApps.getReplicas(paxosID).toArray(
				new TESTPaxosReplicable[0]);
		for (int i = 0; i < replicas.length; i++) {
			for (int j = i + 1; j < replicas.length; j++) {
				invariant = invariant
						&& RSMInvariant(replicas[i], replicas[j], paxosID,
								seqnum);
			}
		}
		return invariant;
	}

	// check invariant at current frontier
	public boolean RSMInvariant(String paxosID) {
		boolean invariant = true;
		TESTPaxosReplicable[] replicas = AllApps.getReplicas(paxosID).toArray(
				new TESTPaxosReplicable[0]);
		for (int i = 0; i < replicas.length; i++) {
			for (int j = i + 1; j < replicas.length; j++) {
				invariant = invariant
						&& RSMInvariant(replicas[i], replicas[j], paxosID,
								replicas[i].allState.get(paxosID).seqnum);
			}
		}
		return invariant;
	}

	// AllApps below is for testing
	public static class AllApps {
		// private static Set<TESTPaxosReplicable> appMap = new
		// HashSet<TESTPaxosReplicable>();
		private static HashMap<Integer, TESTPaxosReplicable> appMap = new HashMap<Integer, TESTPaxosReplicable>();

		private synchronized static void addApp(TESTPaxosReplicable app) {
			// appMap.add(app);
			appMap.put(app.getMyID(), app);
		}

		private synchronized static Set<TESTPaxosReplicable> getReplicas(
				String paxosID) {
			Set<TESTPaxosReplicable> replicas = new HashSet<TESTPaxosReplicable>();
			for (TESTPaxosReplicable app : appMap.values()) {
				if (app.allState.containsKey(paxosID))
					replicas.add(app);
			}
			return replicas;
		}

		public synchronized static String toString(String paxosID) {
			String s = "";
			for (TESTPaxosReplicable app : getReplicas(paxosID)) {
				s += "\n" + app.toString(paxosID);
			}
			return s;
		}
	}

	@Override
	public boolean handleRequest(InterfaceRequest request) {
		return handleRequest(request, false);
	}

	@Override
	public InterfaceRequest getRequest(String stringified)
			throws RequestParseException {
		try {
			JSONObject json = new JSONObject(stringified);
			if (PaxosPacket.getPaxosPacketType(json).equals(
					PaxosPacket.PaxosPacketType.REQUEST))
				return new RequestPacket(json);
			else
				return new ProposalPacket(json);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		throw new RequestParseException(new RuntimeException(
				"Can not parse request: " + stringified));
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		throw new RuntimeException("Method not yet implemented");
	}

	@Override
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient) {
		// no need to again stringify and unstringify
		if (request instanceof ProposalPacket)
			return this.handleDecision((ProposalPacket) request,
					doNotReplyToClient);
		else if (request instanceof RequestPacket)
			return this.sendEchoResponse((RequestPacket) request);
		else
			throw new RuntimeException(
					"TESTPaxosReplicable received non-RequestPacket type request : "
							+ request);
	}

	public boolean sendEchoResponse(RequestPacket request) {
		try {
			// arbitrary slot number of 0
			ProposalPacket proposal = new ProposalPacket(0, request);
			this.sendResponseToClient(proposal);
		} catch (JSONException | IOException e) {
			log.severe("Node" + getMyID()
					+ "encountered JSONException while decoding REQUEST: "
					+ request);
			e.printStackTrace();
		}
		return true;
	}
}
