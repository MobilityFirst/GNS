/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.testing;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.ProposalPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.RequestInstrumenter;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.InterfaceNIOTransport;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Keyable;
import edu.umass.cs.utils.MultiArrayMap;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         The default no-op application for gigapaxos testing. It simply echoes
 *         back the request to the client. But it does a number of other
 *         instrumentations and asserts for testing.
 */
public class TESTPaxosApp implements Replicable,
		ClientMessenger {
	private static final int MAX_STORED_REQUESTS = 1000;
	private MessageDigest md = null;
	private InterfaceNIOTransport<Integer, JSONObject> niot = null;

	private final MultiArrayMap<String, PaxosState> allState = ABSOLUTE_NOOP ? null
			: new MultiArrayMap<String, PaxosState>(
					Config.getGlobalInt(PC.PINSTANCES_CAPACITY));

	private class PaxosState implements Keyable<String> {
		PaxosState(String paxosID) {
			this.paxosID = paxosID;
		}

		public String getKey() {
			return this.paxosID;
		}

		private final String paxosID;
		private int seqnum = -1;
		private String value = null;
		private int numExecuted = 0;
		private HashMap<Integer, String> committed = null;
	}

	private static Logger log = PaxosManager.getLogger();

	/**
	 * @param nio
	 */
	public TESTPaxosApp(JSONNIOTransport<Integer> nio) {
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
	private TESTPaxosApp() {
		// app uses nio only to send, not receive, so no PacketDemultiplexer
		try {
			md = MessageDigest.getInstance("SHA");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	static {
		TESTPaxosConfig.load();
	}
	private static final boolean ABSOLUTE_NOOP = Config
			.getGlobalBoolean(TESTPaxosConfig.TC.ABSOLUTE_NOOP_APP);
	private static final long APP_DELAY = Config
			.getGlobalLong(TESTPaxosConfig.TC.TEST_APP_DELAY);

	private boolean wasteTime(long usDelay) {
		long nanoT = System.nanoTime();
		while (System.nanoTime() - nanoT < usDelay * 1000) {
			for (int i = 0; i < 100; i++)
				Math.random();
			try {
				if (usDelay > 10000)
					Thread.sleep(1);
				else if (usDelay > 500)
					Thread.sleep(0);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		log.info(this + " handled request in " + (System.nanoTime() - nanoT)
				/ 1000 + "us");
		return true;
	}

	public String toString() {
		return this.getClass().getSimpleName() + this.getMyID();
	}

	/*
	 * This is the main execution method. The app is supposed to be agnostic to
	 * slot numbers, but this method takes as input a ProposalPacket as opposed
	 * to a RequestPacket only for tetsing purposes.
	 */
	private boolean handleDecision(ProposalPacket requestPacket,
			boolean doNotReplyToClient) {
		if (APP_DELAY > 0)
			return wasteTime(APP_DELAY);
		else if (ABSOLUTE_NOOP)
			return true;

		// else the older testing code
		try {
			String paxosID = requestPacket.getPaxosID();
			PaxosState state = this.allState.putIfAbsent(paxosID,
					state = new PaxosState(paxosID));

			/*
			 * Initialize seqnum upon first decision. We know it is the first
			 * decision if seqnum==-1 or if putState is true, i.e., checkpoint
			 * recovery has just happened and no other request has been
			 * executed.
			 */
			if (state.seqnum == -1)
				state.seqnum = requestPacket.slot;
			else
				state.seqnum++;

			log.log(Level.FINE,
					"Node{0} executing {1}; seqnum={2}, prev_state={3}",
					new Object[] { (this.niot != null ? getMyID() : "[?]"),
							requestPacket.getSummary(), state.seqnum,
							Util.truncate(state.value, 16, 16) });

			/*
			 * Set state to current request value concatenated with the hash of
			 * the previous state. This allows us to easily compare using just
			 * the current state value that two RSMs executed the exact same set
			 * of state transitions to arrive at that state.
			 */
			if (TESTPaxosConfig.shouldAssertRSMInvariant())
				state.value = requestPacket.requestValue + digest(state.value);

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
			// maintain executed sequence if shouldAssertRSMInvariant
			if (TESTPaxosConfig.shouldAssertRSMInvariant()) {
				if (state.committed == null)
					state.committed = new LinkedHashMap<Integer, String>() {
						private static final long serialVersionUID = 1L;
						private static final int MAX_ENTRIES = 1000;

						protected boolean removeEldestEntry(
								Map.Entry<Integer, String> eldest) {
							if (this.size() > MAX_ENTRIES)
								return true;
							return false;
						}
					};
				state.committed.put(state.seqnum, state.value);
			}
			state.numExecuted++;

			if (TESTPaxosConfig.shouldAssertRSMInvariant())
				assert (RSMInvariant(requestPacket.getPaxosID(),
						requestPacket.slot)) : requestPacket;
			state.committed.remove(state.seqnum - MAX_STORED_REQUESTS); // GC

			if (TESTPaxosConfig.PAXOS_MANAGER_UNIT_TEST)
				synchronized (this) {
					this.notify();
				}
			assert (requestPacket.requestID >= 0) : requestPacket.toString();
			if (!doNotReplyToClient && niot != null) {
				this.sendResponseToClient(requestPacket);
			} else
				log.log(Level.FINE, "Node{0} not sending reply {1} to client",
						new Object[] { getMyID(), requestPacket.getSummary() });
		} catch (JSONException je) {
			je.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return true;
	}

	private void sendResponseToClient(ProposalPacket requestPacket)
			throws JSONException, IOException {
		if (!TESTPaxosConfig.getSendReplyToClient())
			return;
		RequestInstrumenter.remove(requestPacket.requestID);
		/*
		 * Entry replica check must be done here as requests with different
		 * entry replicas can get batched when forwarded between replicas.
		 */
		assert (requestPacket.getEntryReplica() != -1);
		if (requestPacket.getEntryReplica() == this.getMyID()) {
			log.log(Level.FINE, "App {0} sending response to client {1}",
					new Object[] { getMyID(), requestPacket.getSummary() });
			niot.sendToAddress((requestPacket.getClientAddress()),
					new ProposalPacket(0, requestPacket.getACK())
							.toJSONObject());
		}
		// send responses for batched requests as well
		if (requestPacket.getBatched() != null)
			for (RequestPacket batchedReq : requestPacket.getBatched())
				// single-level recursion as batches can not be deeper
				this.sendResponseToClient(new ProposalPacket(
						requestPacket.slot, batchedReq));
	}

	@Override
	public synchronized String checkpoint(String paxosID) {
		if (ABSOLUTE_NOOP)
			return paxosID
					+ ":"
					+ Long.toHexString(((long) (Math.random() * Long.MAX_VALUE)));
		PaxosState state = this.allState.get(paxosID);
		if (state != null)
			return state.value;
		return null;
	}

	@Override
	public synchronized boolean restore(String paxosID, String value) {
		if(ABSOLUTE_NOOP) return true;
		if (!allState.containsKey(paxosID))
			allState.putIfAbsent(paxosID, new PaxosState(paxosID));
		PaxosState state = allState.get(paxosID);
		state.value = value;
		state.seqnum = -1; // reset seqnum upon state transfer
		return true;
	}

	synchronized int digest(String s) {
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

	void shutdown() {
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
	/**
	 * @param paxosID
	 * @return Number committed.
	 */
	public synchronized int getNumCommitted(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if (state != null)
			return state.seqnum;
		return 0;
	}

	/**
	 * @param paxosID
	 * @return Number executed that in general may be different from number
	 *         committed in case of checkpoint transfers.
	 */
	public synchronized int getNumExecuted(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if (state != null)
			return state.numExecuted;
		return 0;
	}

	synchronized String getRequest(String paxosID, int reqnum) {
		PaxosState state = this.allState.get(paxosID);
		if (state != null)
			return state.committed.get(reqnum);
		return null;
	}

	/**
	 * @param paxosID
	 * @return Hash of current state.
	 */
	public synchronized int getHash(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if (state != null)
			return state.value.hashCode();
		return 0;
	}

	/**
	 * @throws InterruptedException
	 */
	public synchronized void waitToFinish() throws InterruptedException {
		this.wait();
	}

	/**
	 * @param paxosID
	 * @param slot
	 * @throws InterruptedException
	 */
	public synchronized void waitToFinish(String paxosID, int slot)
			throws InterruptedException {
		PaxosState state = null;
		while ((state = this.allState.get(paxosID)) == null
				|| state.seqnum < slot)
			this.wait();
	}

	synchronized String toString(String paxosID) {
		String s = "";
		PaxosState state = this.allState.get(paxosID);
		s += "[App" + this.niot.getMyID() + ":" + paxosID + ": " + "seqnum="
				+ (state != null ? state.seqnum : -1) + "; state="
				+ (state != null ? state.value : "null") + ";]";
		return s;
	}

	boolean RSMInvariant(TESTPaxosApp app1, TESTPaxosApp app2, String paxosID,
			int seqnum) {
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
	private boolean hasHoles(TESTPaxosApp app1, TESTPaxosApp app2,
			String paxosID, int seqnum) {
		for (int i = seqnum; i >= 0; i--)
			if (app1.allState.get(paxosID).committed.get(i) == null
					&& app2.allState.get(paxosID).committed.get(i) != null
					|| app2.allState.get(paxosID).committed.get(i) == null
					&& app1.allState.get(paxosID).committed.get(i) != null)
				return true;
		return false;
	}

	private String getTrace(TESTPaxosApp app1, TESTPaxosApp app2,
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
	boolean RSMInvariant(String paxosID, int seqnum) {
		boolean invariant = true;
		TESTPaxosApp[] replicas = AllApps.getReplicas(paxosID).toArray(
				new TESTPaxosApp[0]);
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
	boolean RSMInvariant(String paxosID) {
		boolean invariant = true;
		TESTPaxosApp[] replicas = AllApps.getReplicas(paxosID).toArray(
				new TESTPaxosApp[0]);
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
	static class AllApps {
		// private static Set<TESTPaxosReplicable> appMap = new
		// HashSet<TESTPaxosReplicable>();
		private static HashMap<Integer, TESTPaxosApp> appMap = new HashMap<Integer, TESTPaxosApp>();

		private synchronized static void addApp(TESTPaxosApp app) {
			// appMap.add(app);
			appMap.put(app.getMyID(), app);
		}

		private synchronized static Set<TESTPaxosApp> getReplicas(String paxosID) {
			Set<TESTPaxosApp> replicas = new HashSet<TESTPaxosApp>();
			for (TESTPaxosApp app : appMap.values()) {
				if (app.allState.containsKey(paxosID))
					replicas.add(app);
			}
			return replicas;
		}

		public synchronized static String toString(String paxosID) {
			String s = "";
			for (TESTPaxosApp app : getReplicas(paxosID)) {
				s += "\n" + app.toString(paxosID);
			}
			return s;
		}
	}

	@Override
	public boolean execute(Request request) {
		return execute(request, false);
	}

	@Override
	public Request getRequest(String stringified)
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
		IntegerPacketType[] types = { PaxosPacket.PaxosPacketType.PAXOS_PACKET };
		return new HashSet<IntegerPacketType>(Arrays.asList(types));
	}

	@Override
	public boolean execute(Request request,
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

	boolean sendEchoResponse(RequestPacket request) {
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

	@Override
	public void setClientMessenger(
			SSLMessenger<?, JSONObject> messenger) {
		// do nothing
	}
}
