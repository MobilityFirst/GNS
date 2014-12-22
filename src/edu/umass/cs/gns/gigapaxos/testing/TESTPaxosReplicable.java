package edu.umass.cs.gns.gigapaxos.testing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.gigapaxos.PaxosManager;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.gigapaxos.paxosutil.RequestInstrumenter;

/**
 * @author V. Arun
 */
public class TESTPaxosReplicable implements Replicable {
	private static final boolean DEBUG = PaxosManager.DEBUG;
	public static final int MAX_STORED_REQUESTS = 1000;
	private MessageDigest md = null;
	private InterfaceJSONNIOTransport<Integer> niot = null;

	private HashMap<String, PaxosState> allState = new HashMap<String, PaxosState>();

	private class PaxosState {
		private int seqnum = -1;
		private String value = "Initial state";
		private int numExecuted = 0;
		private HashMap<Integer, String> committed = new HashMap<Integer, String>();
	}

	private static Logger log = PaxosManager.getLogger();// Logger.getLogger(TESTPaxosReplicable.class.getName());

	public TESTPaxosReplicable(JSONNIOTransport<Integer> nio) {
		this();
		try {
			/*
			 * app uses nio only to send, not receive, so it doesn't care to set a
			 * PacketDemultiplexer
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

	@Override
	public synchronized boolean handleDecision(String paxosID, String req,
			boolean doNotReplyToClient) {
		boolean executed = false;
		try {
			JSONObject reqJson = new JSONObject(req);
			ProposalPacket requestPacket = new ProposalPacket(reqJson);
			PaxosState state = this.allState.get(paxosID);
			if (state == null)
				state = new PaxosState();

			/*
			 * Initialize seqnum upon first decision. We know it is the first decision if seqnum==-1
			 * or if putState is true, i.e., checkpoint recovery has just happened and no other
			 * request has been executed.
			 */
			if (state.seqnum == -1)
				state.seqnum = requestPacket.slot;

			if (DEBUG)
				log.info("Node" + (this.niot != null ? getMyID() : null) + " "
						+ paxosID + " executing request with slot "
						+ requestPacket.slot + ", id = "
						+ requestPacket.requestID + " with value "
						+ requestPacket.requestValue + "; seqnum="
						+ state.seqnum + ": prev_state_value=" + state.value);

			/*
			 * Set state to current request value concatenated with the hash of the previous state.
			 * This allows us to easily compare using just the current state value that two RSMs
			 * executed the exact same set of state transitions to arrive at that state.
			 */
			state.value = requestPacket.requestValue + (digest(state.value));

			/*
			 * Assert that the next slot is always the next expected seqnum. This ensures that paxos
			 * is making the app execute requests in the correct slot number order. Note that
			 * state.seqnum is set exactly once at initialization to the arriving request's slot
			 * above and is then just incremented by one below for every executed decision.
			 */
			assert (state.seqnum == requestPacket.slot);
			/*
			 * increment seqnum (to the next expected seqnum and requestPacket.slot)
			 */
			state.committed.put(state.seqnum++, state.value);
			allState.put(paxosID, state); // needed if we initialized state above
			executed = true;
			state.numExecuted++;

			if (TESTPaxosConfig.shouldAssertRSMInvariant())
				assert (RSMInvariant(requestPacket.getPaxosID(),
						state.seqnum - 1)) : reqJson;
			state.committed.remove(state.seqnum - MAX_STORED_REQUESTS); // GC

			// testing and logging below
			this.notify(); // needed for paxos manager's unit-test
			assert (requestPacket.requestID >= 0) : requestPacket.toString();
			if (!doNotReplyToClient && niot != null) {
				if (DEBUG)
					log.info("App sending response to client "
							+ requestPacket.clientID + ": " + reqJson);
				if (TESTPaxosConfig.getSendReplyToClient())
					niot.sendToAddress(
							new InetSocketAddress(requestPacket
									.getClientAddress(), requestPacket
									.getClientPort()), reqJson);
				// niot.sendToID(requestPacket.clientID, reqJson);
				RequestInstrumenter.remove(requestPacket.requestID);
			} else if (DEBUG)
				log.info("Node" + getMyID() + " not sending reply to client: "
						+ reqJson);
		} catch (JSONException je) {
			je.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return executed;
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
		allState.put(paxosID, state);
		return true;
	}

	public int digest(String s) {
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

	public InterfaceJSONNIOTransport<Integer> setNIOTransport(
			InterfaceJSONNIOTransport<Integer> nio) {
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
		String state1 = null, state2 = null;
		if (app1.allState.containsKey(paxosID))
			state1 = app1.allState.get(paxosID).committed.get(seqnum);
		if (app2.allState.containsKey(paxosID))
			state2 = app2.allState.get(paxosID).committed.get(seqnum);
		assert (state1 == null || state2 == null || state1.equals(state2)) : 
			app1.getMyID() + ":" + paxosID + ":" + seqnum + ": " + state1 + " != " 
			+ app2.getMyID() + ":" + paxosID + ":" + seqnum + ": " + state2;
		return (state1 == null || state2 == null || state1.equals(state2));
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
		//private static Set<TESTPaxosReplicable> appMap = new HashSet<TESTPaxosReplicable>();
		private static HashMap<Integer, TESTPaxosReplicable> appMap = new HashMap<Integer, TESTPaxosReplicable>();

		private synchronized static void addApp(TESTPaxosReplicable app) {
			//appMap.add(app);
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
}
