package edu.umass.cs.gns.packet;


import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.packet.Packet.PacketType;


/**
 * This packet is sent among replica controllers after a group change for a name is complete.
 * When a replica controller receives this message, it updates the database record for the name
 * to indicate the completion of group change.
 * It contains two fields: <code>name</code> and  <code>paxosID</code>. paxosID is the ID of the paxos
 * group between new set of active replicas.
 */
public class ChangeActiveStatusPacket extends BasicPacket
{
	
	private static final String PAXOS_ID = "paxosID";
	private final static String NAME = "name";

	/**
	 * name for which the proposal is being done.
	 */
	String name;

	/**
	 * ID of the paxos group between new set of active replicas.
	 */
	String paxosID;
	
	/**
	 * Depending on packet type, two information are conveyed. 
	 * if packet type = Either old active is set to not running, or new active is set to running.
	 * @param paxosID
	 */
	public ChangeActiveStatusPacket(String paxosID, String name, PacketType packetType) {
		this.setType(packetType);
		this.paxosID = paxosID;
		this.name = name;
		//this.recordKey = recordKey;
	}
	
	

	public ChangeActiveStatusPacket(JSONObject json) throws JSONException {
		
		this.type = Packet.getPacketType(json);
		this.name = json.getString(NAME);
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

	/**
	 * 
	 * @return
	 */
	public String getPaxosID(){
		
		return paxosID;
	}
}
