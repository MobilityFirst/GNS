package edu.umass.cs.gns.packet;

import org.json.JSONException;
import org.json.JSONObject;
/**************** FIXME Package deprecated by nsdesign/packet. this will soon be deleted. **/
/**  @deprecated
@author V. Arun. 
 */
public abstract class PaxosPacket extends Packet {

	public static final String ptype = "PT"; // Name of packet type field in JSON representation
	public static final String paxosIDKey = "PAXOS_ID";
	
	/**
	 * Response from Paxos replica to client.
	 */
	public static final int RESPONSE = 0;
	/**
	 * Request sent from client to Paxos Replica.
	 */
	public static final int REQUEST = 1;
	/**
	 * 
	 */
	public static String[] typeToString = {"RESPONSE", "REQUEST", "PREPARE", "ACCEPT", "RESEND_ACCEPT",
		"PROPOSAL", "DECISION", "PREPARE_REPLY", "ACCEPT_REPLY", "FAILURE_DETECT", "FAILURE_RESPONSE"
	};

	public static final int PREPARE = 2;  
	public static final int ACCEPT = 3;
	public static final int RESEND_ACCEPT = 4; 
	public static final int PROPOSAL = 5;
	public static final int DECISION = 6; 
	public static final int PREPARE_REPLY = 7; 
	public static final int ACCEPT_REPLY = 8;
	public static final int FAILURE_DETECT = 9; 
	public static final int FAILURE_RESPONSE = 10; 
	public static final int NODE_STATUS = 11; 
	public static final int SEND_STATE = 21;
	public static final int SEND_STATE_NO_RESPONSE = 22;
	public static final int REQUEST_STATE = 23;
	public static final int SYNC_REQUEST = 31;
	public static final int SYNC_REPLY = 32;
	public static final int START = 41;
	public static final int STOP = 42;
	
	protected int packetType;
	protected String paxosID;

	abstract public int getType();
	public abstract JSONObject toJSONObject() throws JSONException;

	public void putPaxosID(String pid) {
		this.paxosID = pid;
	}
	public String getPaxosID() {
		return this.paxosID;
	}
	
	@Override
	public String toString() {
		try {
			return this.toJSONObject().toString();
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}	
}
