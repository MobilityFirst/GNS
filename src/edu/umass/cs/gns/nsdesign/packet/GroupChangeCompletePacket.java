package edu.umass.cs.gns.nsdesign.packet;


import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * This packet is sent among replica controllers after a group change for a name is complete.
 * When a replica controller receives this message, it updates the database record for the name
 * to indicate the completion of group change.
 * It contains two fields: <code>name</code> and  <code>version</code>. version is the ID of the
 * group between new set of active replicas.
 */
public class GroupChangeCompletePacket extends BasicPacket {

	private static final String VERSION = "version";
	private final static String NAME = "name";

	/**
	 * name for which the proposal is being done.
	 */
	String name;

	/**
	 * ID of the group between new set of active replicas.
	 */
	int version;

	/**
	 * Depending on packet type, two information are conveyed.
	 * if packet type = Either old active is set to not running, or new active is set to running.
	 * @param version
	 */
	public GroupChangeCompletePacket(int version, String name) {
		this.setType(PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY);
		this.version = version;
		this.name = name;
	}



	public GroupChangeCompletePacket(JSONObject json) throws JSONException {

		this.type = Packet.getPacketType(json);
		this.name = json.getString(NAME);
		this.version = json.getInt(VERSION);
	}

	/**
	 * JSON object that is implemented.
	 * @return
	 * @throws org.json.JSONException
	 */
	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		Packet.putPacketType(json, getType());
		json.put(NAME, name);
		json.put(VERSION, this.version);
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
	public int getVersion(){
		
		return version;
	}
}
