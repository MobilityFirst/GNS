package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

public class RequestActivesPacket extends BasicPacket
{


	//public final static String RECORDKEY = "recordkey";
	public final static String NAME = "name";
	public static final String ACTIVES = "actives";
	public final static String LNSID = "lnsid";
	
	String name;
	//NameRecordKey recordKey;
	Set<Integer> activeNameServers;
	int lnsID;
	
	public RequestActivesPacket(String name, //NameRecordKey recordKey,
                int lnsID) {
		this.name = name;
		//this.recordKey = recordKey;
		this.type = PacketType.REQUEST_ACTIVES;
		this.lnsID = lnsID;
	}
	
	
	public RequestActivesPacket(JSONObject json) throws JSONException {
		this.name = json.getString(NAME);
		//this.recordKey = new NameRecordKey(json.getString(RECORDKEY));
		this.activeNameServers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(ACTIVES));
		this.type = PacketType.REQUEST_ACTIVES;
		this.lnsID = json.getInt(LNSID);
	}
	
	@Override
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(NAME,name);
		//json.put(RECORDKEY,recordKey.getName());
		json.put(ACTIVES,new JSONArray(activeNameServers));
		Packet.putPacketType(json, getType());
		json.put(LNSID, lnsID);
		return json;
	}

	
	public void setActiveNameServers(Set<Integer> activeNameServers) {
		this.activeNameServers = activeNameServers;
	}
	

	public String getName() {
		return name;
	}
	
//	public NameRecordKey getRecordKey() {
//		return recordKey;
//	}
	
	public Set<Integer> getActiveNameServers() {
		return activeNameServers;
	}
	
	public int getLNSID() {
		return lnsID;
	}
}
