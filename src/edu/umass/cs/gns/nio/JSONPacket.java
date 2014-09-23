package edu.umass.cs.gns.nio;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.json.JSONException;
import org.json.JSONObject;

/**
@author V. Arun
 */

/* An abstract class that all json packets should extend.
 */
public abstract class JSONPacket {
	public static final String PACKET_TYPE="type";
	protected final int type;
	
	public JSONPacket(IntegerPacketType t) {
		this.type = t.getInt();
	}
	public JSONPacket(JSONObject json) throws JSONException {
		this.type = getPacketType(json);
	}
	
	public abstract JSONObject toJSONObjectImpl() throws JSONException;
	
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = toJSONObjectImpl();
		json.put(PACKET_TYPE, type);
		return json;
	}
	public String toString() {
		try {
			return toJSONObject().toString();
		} catch(JSONException je) {
			je.printStackTrace();
		}
		return null;
	}
	
	public static final Integer getPacketType(JSONObject json) 	throws JSONException {
		if(json.has(PACKET_TYPE)) 
			return json.getInt(PACKET_TYPE);
		else return null;
	}
	public static final InetAddress getSenderAddress(JSONObject json) 	throws JSONException {
		if(json.has(JSONNIOTransport.DEFAULT_IP_FIELD)) {
			try {
				return InetAddress.getByName(json.getString(JSONNIOTransport.DEFAULT_IP_FIELD).replaceAll("[a-zA-Z]*/", ""));
			} catch(UnknownHostException uhe) {
				uhe.printStackTrace();
			}
		}
		return null;
	}
}
