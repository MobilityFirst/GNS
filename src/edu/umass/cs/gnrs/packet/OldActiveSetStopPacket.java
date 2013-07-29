package edu.umass.cs.gnrs.packet;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnrs.nameserver.NameRecordKey;
import edu.umass.cs.gnrs.packet.Packet.PacketType;

public class OldActiveSetStopPacket extends BasicPacket
{
//	private final static String ID = "packetID"; // new active set ID
	
	private final static String NAME = "name";
	
	//private final static String RECORDKEY = "recordKey";

	private final static String PRIMARY_SENDER = "primarySender";
	
	private final static String ACTIVE_RECEIVER = "activeReceiver";

	private final static String PAXOS_ID_TO_BE_STOPPED = "paxosID";
	
	/**
	 * name for which the proposal is being done.
	 */
	String name;

//	/**
//	 * name record key 
//	 */
//	NameRecordKey recordKey;
	
	/**
	 * primary node that sent this message 
	 */
	int primarySender;
	
	/**
	 * active who received this message from primary
	 */
	int activeReceiver;
	
	/**
	 * Paxos ID that is requested to be deleted.
	 */
	String paxosIDTobeStopped;
	
	/**
	 * 
	 * @param name
	 * @param primarySender
	 * @param newActives
	 */
	public OldActiveSetStopPacket(String name, //NameRecordKey recordKey, 
                int primarySender, 
			int activeReceiver, String paxosIDToBeStopped, PacketType type1) {
		this.name = name;
		//this.recordKey = recordKey;
		this.type = type1;
		this.primarySender = primarySender;
		this.activeReceiver = activeReceiver;
		this.paxosIDTobeStopped = paxosIDToBeStopped;
	}
	
	public OldActiveSetStopPacket(JSONObject json) throws JSONException {
		this.type = Packet.getPacketType(json);
		this.name = json.getString(NAME);
		//this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));
		this.primarySender = json.getInt(PRIMARY_SENDER);
		this.activeReceiver = json.getInt(ACTIVE_RECEIVER);
		this.paxosIDTobeStopped = json.getString(PAXOS_ID_TO_BE_STOPPED);
	}

	/**
	 * JSON object that is implemented.
	 * @return
	 * @throws JSONException
	 */
	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		Packet.putPacketType(json, getType());
		json.put(NAME, name);
		//json.put(RECORDKEY, recordKey.getName());
		json.put(PRIMARY_SENDER, primarySender);
		json.put(ACTIVE_RECEIVER, activeReceiver);
		json.put(PAXOS_ID_TO_BE_STOPPED, paxosIDTobeStopped);
		return json;
	}
	
	public String getName() {
		return name;
	}
	
//	public NameRecordKey getRecordKey() {
//		return recordKey;
//	}
	
	public int getPrimarySender() {
		return primarySender;
	}
	
	public int getActiveReceiver() {
		return activeReceiver;
	}
	
	public String getPaxosIDToBeStopped() {
		return paxosIDTobeStopped;
	}
	
	public void changePacketTypeToConfirm() {
		setType(PacketType.OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY);
	}

	public void changePacketTypeToPaxosStop() {
		setType(PacketType.ACTIVE_PAXOS_STOP);
	}
}
