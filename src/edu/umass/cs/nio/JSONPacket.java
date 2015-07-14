package edu.umass.cs.nio;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author V. Arun
 */

/*
 * An abstract class that all json packets should extend.
 */
public abstract class JSONPacket {
	/**
	 * JSON key for the integer packet type.
	 */
	public static final String PACKET_TYPE = "type";
	protected final int type;

	/**
	 * @param t
	 */
	public JSONPacket(IntegerPacketType t) {
		this.type = t.getInt();
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public JSONPacket(JSONObject json) throws JSONException {
		this.type = getPacketType(json);
	}

	/**
	 * @return JSONObject corresponding to fields in classes extending this
	 *         class.
	 * @throws JSONException
	 */
	protected abstract JSONObject toJSONObjectImpl() throws JSONException;

	/**
	 * @return JSONObject corresponding to this class' (including subclasses)
	 *         fields.
	 * @throws JSONException
	 */
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = toJSONObjectImpl();
		json.put(PACKET_TYPE, type);
		return json;
	}

	public String toString() {
		try {
			return toJSONObject().toString();
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return null;
	}
	
	/* ********************* static helper methods below *******************/

	/**
	 * @param json
	 * @return Integer packet type.
	 * @throws JSONException
	 */
	public static final Integer getPacketType(JSONObject json)
			throws JSONException {
		if (json.has(PACKET_TYPE))
			return json.getInt(PACKET_TYPE);
		else
			return null;
	}
	
	/**
	 * Puts type into json.
	 * 
	 * @param json
	 * @param type
	 */
	public static final void putPacketType(JSONObject json, int type) {
		try {
			json.put(PACKET_TYPE, type);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Puts type.getInt() into json.
	 * 
	 * @param json
	 * @param type
	 */
	public static final void putPacketType(JSONObject json, IntegerPacketType type) {
		try {
			json.put(PACKET_TYPE, type.getInt());
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
