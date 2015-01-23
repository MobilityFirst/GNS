package edu.umass.cs.gns.protocoltask.json;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.protocoltask.ThresholdProtocolEvent;
import edu.umass.cs.gns.util.Stringifiable;

/**
 * @author V. Arun
 */
/*
 * This class is concretized to use integer node IDs, long keys, and JSON messages, but the
 * PacketType is left generic. The reason is so that implementors can define and use their own
 * packet types. Note that it is not possible to extend the enum Packet.PacketType and it is not
 * modular to have to put *all* packet types in the Packet class.
 */
public abstract class ProtocolPacket<NodeIDType, EventType> implements
		ThresholdProtocolEvent<NodeIDType, EventType, String> {

	public static enum Keys {
		SENDER, INITIATOR, KEY
	}; // all protocol packets carry these fields

	protected EventType type;

	private NodeIDType sender = null;
	private NodeIDType initiator = null;
	private String key = null;

	public ProtocolPacket(NodeIDType initiator) {
		this.sender = initiator;
		this.initiator = initiator;
	}

	public ProtocolPacket(ProtocolPacket<NodeIDType, EventType> pkt) {
		this.sender = pkt.sender;
		this.initiator = pkt.initiator;
	}

	public ProtocolPacket(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		this.type = getPacketType(json);
		this.sender = (json.has(Keys.SENDER.toString()) ? unstringer.valueOf((json
				.get(Keys.SENDER.toString())).toString()) : null);
		this.initiator = (json.has(Keys.INITIATOR.toString()) ? unstringer.valueOf((json
				.get(Keys.INITIATOR.toString())).toString()) : null);
		this.key = (json.has(Keys.KEY.toString()) ? json.getString(Keys.KEY
				.toString()) : null);
	}
	
	/* FIXME: Maybe disable this option altogether? This option is
	 * convenient for just primitive types as JSON will handle it.
	 * Not sure we can do anything but suppress the warnings.
	 */
	@SuppressWarnings("unchecked")
	protected ProtocolPacket(JSONObject json) throws JSONException {
		this.type = getPacketType(json);
		this.sender = (json.has(Keys.SENDER.toString()) ? (NodeIDType) (json
				.get(Keys.SENDER.toString())) : null);
		this.initiator = (json.has(Keys.INITIATOR.toString()) ? (NodeIDType) (json
				.get(Keys.INITIATOR.toString())) : null);
		this.key = (json.has(Keys.KEY.toString()) ? json.getString(Keys.KEY
				.toString()) : null);
	}

	@Override
	public NodeIDType getSender() {
		return this.sender;
	}

	protected void setSender(NodeIDType id) {
		this.sender = id;
	}

	public NodeIDType getInitiator() {
		return this.initiator;
	}

	protected NodeIDType flip(NodeIDType rcvr) { // flip sender and rcvr
		NodeIDType prevSender = this.sender;
		this.sender = rcvr;
		return prevSender;
	}

	@Override
	public EventType getType() {
		return this.type;
	}

	public void setType(EventType type) {
		this.type = type;
	}

	public abstract JSONObject toJSONObjectImpl() throws JSONException;

	public abstract EventType getPacketType(JSONObject json)
			throws JSONException;

	public abstract void putPacketType(JSONObject json, EventType type)
			throws JSONException;

	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = toJSONObjectImpl();
		// Packet.putPacketType(json, this.getType());
		this.putPacketType(json, getType());
		json.put(Keys.SENDER.toString(), this.sender);
		json.put(Keys.INITIATOR.toString(), this.initiator);
		json.put(Keys.KEY.toString(), this.key);
		return json;
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public Object getMessage() {
		return this;
	}

	public static void main(String[] args) {
		JSONObject json = new JSONObject();
		Integer id = 3;
		String ID = "ID";
		try {
			json.put(ID, id);
			System.out.println((Integer) json.get(ID));
		} catch (JSONException je) {
			je.printStackTrace();
		}
	}
}
