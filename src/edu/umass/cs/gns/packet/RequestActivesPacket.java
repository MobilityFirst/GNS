package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;


/**
 * This packet is sent by local name server to a name server to request the current active replicas
 * for a name. The name server replies also uses this packet to send the reply to a local name server.
 *
 * If the name server is a replica controller for this name and the name exists, then the field
 * <code>activeNameServers</code> in the packet is filled with the current set of active name servers.
 * Otherwise, <code>activeNameServers</code> is set to null.
 *
 * @author Abhigyan
 */
public class RequestActivesPacket extends BasicPacket
{

	public final static String NAME = "name";
	public static final String ACTIVES = "actives";
	public final static String LNSID = "lnsid";

  /**
   * Name for which the active replicas are being requested
   */
	String name;

  /**
   * Local name server sending the request.
   */
  int lnsID;

  /**
   * Active name servers for the name. This field is populated when name server
   * sends a reply to a local name server.
   */
	Set<Integer> activeNameServers;


	public RequestActivesPacket(String name, int lnsID) {
		this.name = name;
		this.type = PacketType.REQUEST_ACTIVES;
		this.lnsID = lnsID;
	}

	public RequestActivesPacket(JSONObject json) throws JSONException {
		this.name = json.getString(NAME);
		this.activeNameServers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(ACTIVES));
		this.type = PacketType.REQUEST_ACTIVES;
		this.lnsID = json.getInt(LNSID);
  }

	@Override
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(NAME,name);
		json.put(ACTIVES,new JSONArray(activeNameServers));
		Packet.putPacketType(json, getType());
		json.put(LNSID, lnsID);
		return json;
	}

	public void setActiveNameServers(Set<Integer> activeNameServers) {
		this.activeNameServers = activeNameServers;
//    this.activeChangeInProgress = activeChangeInProgress;
	}

	public String getName() {
		return name;
	}

	public Set<Integer> getActiveNameServers() {
		return activeNameServers;
	}
	
	public int getLNSID() {
		return lnsID;
	}
}
