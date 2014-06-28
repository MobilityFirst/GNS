package edu.umass.cs.gns.protocoltask.json;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.packet.BasicPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;

/**
@author V. Arun
 */
/* This class is concretized to use integer node IDs, long keys, and JSON
 * messages. 
 */
public abstract class ProtocolPacket extends BasicPacket implements ProtocolEvent<Packet.PacketType,Long> {
	public static final String SENDER = "SENDER";
	public static final String INITIATOR = "INITIATOR";
	public static final String KEY = "KEY";

	private int initiator=-1;
	private int sender = -1;
	private long key=-1;

	public ProtocolPacket(int sid, int iid) {
		this.sender = sid;
		this.initiator = iid;
	}

	public ProtocolPacket(JSONObject json) throws JSONException {
		this.setType(Packet.getPacketType(json));
		this.sender = (json.has(SENDER) ? json.getInt(SENDER) : -1);
		this.initiator = (json.has(INITIATOR) ? json.getInt(INITIATOR) : -1);
		this.key = (json.has(KEY) ? json.getLong(KEY) : -1);
	}

	public int getInitiator() {return this.initiator;}
	public void setSender(int id) {this.sender = id;}
	public int getSender() {return this.sender;}
	public int flip(int rcvr) { // flip sender and rcvr
		int prevSender = this.sender; 
		this.sender = rcvr;
		return prevSender;
	}

	public abstract JSONObject toJSONObjectImpl() throws JSONException;
	
	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = toJSONObjectImpl();
		json.put(Packet.PACKET_TYPE, this.getType().getInt());
		json.put(SENDER, this.sender);
		json.put(INITIATOR, this.initiator);
		json.put(KEY, this.key);
		return json;
	}

	@Override
	public Long getKey() {
		return key;
	}
	@Override
	public void setKey(Long key) {
		this.key = key;
	}
	
	@Override
	public Object getMessage() {
		return this;
	}
	
	public static void main(String[] args) {
		
	}
}
