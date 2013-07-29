package edu.umass.cs.gnrs.packet;

import java.util.HashSet;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnrs.nameserver.NameRecordKey;
import edu.umass.cs.gnrs.packet.Packet.PacketType;

public class ChangeActiveStatusPacket extends BasicPacket
{
	
	private static final String PAXOS_ID = "paxosID";
	private final static String NAME = "name";
	
	//private final static String RECORDKEY = "recordKey";


	/**
	 * name for which the proposal is being done.
	 */
	String name;

//	/**
//	 * name record key 
//	 */
//	NameRecordKey recordKey;

	/**
	 * 
	 */
	String paxosID;
	
	/**
	 * Depending on packet type, two information are conveyed. 
	 * if packet type = Either old active is set to not running, or new active is set to running.
	 * @param paxosID
	 */
	public ChangeActiveStatusPacket(String paxosID, String name, //NameRecordKey recordKey, 
                PacketType packetType) {
		this.setType(packetType);
		this.paxosID = paxosID;
		this.name = name;
		//this.recordKey = recordKey;
	}
	
	

	public ChangeActiveStatusPacket(JSONObject json) throws JSONException {
		
		this.type = Packet.getPacketType(json);
		this.name = json.getString(NAME);
		//this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));
		this.paxosID = json.getString(PAXOS_ID);
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
		json.put(PAXOS_ID, this.paxosID);
		return json;
	}
	
	

	/**
	 * 
	 * @return
	 */
	public String getName() {
		return name;
	}
	
//	/**
//	 * 
//	 * @return
//	 */
//	public NameRecordKey getRecordKey() {
//		return recordKey;
//	}
	
	/**
	 * 
	 * @return
	 */
	public String getPaxosID(){
		
		return paxosID;
	}
}
