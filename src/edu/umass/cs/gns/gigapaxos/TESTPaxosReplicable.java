package edu.umass.cs.gns.gigapaxos;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.logging.Logger;

import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.ProposalPacket;

/**
@author V. Arun
 */
public class TESTPaxosReplicable implements Replicable {
	private static final boolean DEBUG=PaxosManager.DEBUG;
	public static final int MAX_STORED_REQUESTS = 1000;
	private MessageDigest md = null ;
	private InterfaceJSONNIOTransport<Integer> niot = null;

	private HashMap<String,PaxosState> allState = new HashMap<String,PaxosState>();
	private class PaxosState {
		protected int seqnum=-1;
		protected String value = "Initial state";
		protected int numExecuted=0;
		protected HashMap<Integer,String> committed = new HashMap<Integer,String>();
		protected boolean putState=false;
	}
	private static Logger log = Logger.getLogger(TESTPaxosReplicable.class.getName()); // GNS.getLogger();

	TESTPaxosReplicable(JSONNIOTransport<Integer> nio) { // app uses nio only to send, not receive, so it doesn't care to set a PacketDemultiplexer
		try {
			md = MessageDigest.getInstance("SHA");
			setNIOTransport(nio);
		} catch(Exception e) {e.printStackTrace();}
	}
	TESTPaxosReplicable() { // app uses nio only to send, not receive, so it doesn't care to set a PacketDemultiplexer
		try {
			md = MessageDigest.getInstance("SHA");
		} catch(Exception e) {e.printStackTrace();}
	}
	public void shutdown() {
		this.allState.clear();
	}
	public void setNIOTransport(InterfaceJSONNIOTransport<Integer> nio) {
		niot = nio;
	}
	@Override
	public synchronized boolean handleDecision(String paxosID, String req, boolean recovery) {
		boolean executed = false;
		try {
			JSONObject reqJson = new JSONObject(req);
			ProposalPacket requestPacket = new ProposalPacket(reqJson);
			PaxosState state = this.allState.get(paxosID);
			if(state==null) state = new PaxosState();
			if(state.seqnum==-1) state.seqnum = requestPacket.slot;

			if(!TESTPaxosConfig.getSendReplyToClient()) {
				log.info("Testing: PaxosID " + paxosID + " executing request with slot " + 
					requestPacket.slot + ", id = " + requestPacket.requestID + " with value " + 
					requestPacket.requestValue +"; seqnum="+ state.seqnum+": prev_state_value="+state.value);
			}

			state.value = requestPacket.requestValue + (digest(state.value));
			if(state.putState) state.seqnum = requestPacket.slot;
			assert(state.seqnum==requestPacket.slot);
			state.committed.put(state.seqnum++, state.value);
			state.committed.remove(state.seqnum-MAX_STORED_REQUESTS); // garbage collection
			allState.put(paxosID, state);
			executed=true;
			state.numExecuted++;
			state.putState=false;
			this.notify();
			assert(requestPacket.requestID>=0) : requestPacket.toString();
			if(niot!=null && requestPacket.getReplyToClient() && TESTPaxosConfig.getSendReplyToClient()) {
				if(DEBUG) log.info("App sending response to client " + requestPacket.clientID + ": " + reqJson);
				niot.sendToID(requestPacket.clientID, reqJson);
			}
		} catch(JSONException je) {je.printStackTrace();}
		catch(IOException ioe) {ioe.printStackTrace();}
		return executed;
	}
	public int digest(String s) {
		md.update(s.getBytes());
		byte[] digest = md.digest();
		int dig=0;
		for(int i=0; i<digest.length; i++) {
			dig += (int)digest[i];
		}
		return dig;
	}

	@Override
	public synchronized String getState(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if(state!=null) return state.value;
		return null;
	}

	@Override
	public synchronized boolean updateState(String paxosID, String value) {
		PaxosState state = this.allState.get(paxosID);
		if(state==null) state = new PaxosState();
		state.value = value;
		state.putState = true;
		return true;
	}

	/* Testing methods below.
	 */
	public synchronized int getNumCommitted(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if(state!=null) return state.seqnum;
		return 0;
	}
	public synchronized int getNumExecuted(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if(state!=null) return state.numExecuted;
		return 0;
	}
	public synchronized String getRequest(String paxosID, int reqnum) {
		PaxosState state = this.allState.get(paxosID);
		if(state!=null) return state.committed.get(reqnum);
		return null;
	}
	public synchronized int getHash(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if(state!=null) return state.value.hashCode();
		return 0;
	}
	public synchronized void waitToFinish() throws InterruptedException {
		this.wait();
	}
	public synchronized void waitToFinish(String paxosID, int slot) throws InterruptedException {
		PaxosState state = null;
		while((state = this.allState.get(paxosID))==null || state.seqnum<slot) this.wait();
	}
}
