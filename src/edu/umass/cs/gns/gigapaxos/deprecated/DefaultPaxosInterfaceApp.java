package edu.umass.cs.gns.gigapaxos.deprecated;

import java.security.MessageDigest;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import java.util.Set;

/**
@author V. Arun
 */
/* The default "app" using paxos. It has some support for computing 
 * the hash of the current state in a manner reflecting the entire
 * sequence of operations, so that we can assert the state machine
 * replication invariant. It also sends the reply to the client
 * if the flag is set in RequestPacket (used by the coordinator).
 */
public class DefaultPaxosInterfaceApp implements Replicable {
	public static final int MAX_STORED_REQUESTS = 1000;
	private static MessageDigest md ;

	private HashMap<String,PaxosState> allState = new HashMap<String,PaxosState>();

  
	private class PaxosState {
		protected int seqnum=-1;
		protected String value = "Initial state";
		protected int numExecuted=0;
		protected HashMap<Integer,String> committed = new HashMap<Integer,String>();
	}

	DefaultPaxosInterfaceApp() {
		try {
			md = MessageDigest.getInstance("SHA");
		} catch(Exception e) {e.printStackTrace();}
	}
	@Override
	public synchronized boolean handleDecision(String paxosID, String req, boolean recovery) {
		boolean executed = false;
		try {
			JSONObject reqJson = new JSONObject(req);
			ProposalPacket requestPacket = new ProposalPacket(reqJson);
			PaxosState state = this.allState.get(paxosID);
			if(state==null) state = new PaxosState();
			System.out.println("Testing: PaxosID " + paxosID + " executing request with slot " + 
					requestPacket.slot + ", id = " + requestPacket.requestID + " with value " + 
					requestPacket.requestValue +"; seqnum="+ state.seqnum+": prev_state_value="+state.value);
			state.value = requestPacket.requestValue + (digest(state.value));
			if(state.seqnum==-1) state.seqnum = requestPacket.slot;
			assert(state.seqnum==requestPacket.slot); // asserts in-order invariant
			state.committed.put(state.seqnum++, state.value);
			state.committed.remove(state.seqnum-MAX_STORED_REQUESTS); // garbage collection
			allState.put(paxosID, state);
			executed=true;
			state.numExecuted++;
			TESTPaxosConfig.execute(requestPacket.requestID);
			this.notify();
		} catch(JSONException je) {je.printStackTrace();}
		return executed;
	}
	public int digest(String s) {
		md.update(s.getBytes());
		byte[] digest = md.digest();
		int dig=0;
		for(int i=0; i<digest.length; i++) {
			dig = (int)digest[i];
		}
		return dig;
	}

	@Override
	public String getState(String paxosID) {
		PaxosState state = this.allState.get(paxosID);
		if(state!=null) return state.value;
		return null;
	}

	@Override
	public boolean updateState(String paxosID, String value) {
		PaxosState state = this.allState.get(paxosID);
		if(state==null) state = new PaxosState();
		state.value = value;
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
        
        @Override
  public boolean handleRequest(InterfaceRequest request, boolean doNotReplyToClient) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean handleRequest(InterfaceRequest request) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public InterfaceRequest getRequest(String stringified) throws RequestParseException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}
