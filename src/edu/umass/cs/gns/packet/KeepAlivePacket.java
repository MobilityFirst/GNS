package edu.umass.cs.gns.packet;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 8/9/13
 * Time: 1:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class KeepAlivePacket extends BasicPacket{

  private static final String NAME = "name";

  private static final String PAXOS_ID = "paxosID";

  private static final String SENDER = "sender";


  private String name;

  private String paxosID;

  private int sender;

  public KeepAlivePacket(String name, String paxosID, int sender, Packet.PacketType type) {
    this.type = type;
    this.name = name;
    this.paxosID = paxosID;
    this.sender = sender;

  }

  public KeepAlivePacket(JSONObject json) throws JSONException{
    this.type = Packet.getPacketType(json);
    this.name = json.getString(NAME);
    this.sender = json.getInt(SENDER);
    if (json.has(PAXOS_ID)) {
      this.paxosID = json.getString(PAXOS_ID);
    }

  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json,type);
    json.put(NAME, name);
    json.put(SENDER, sender);
    if (paxosID != null) {
      json.put(PAXOS_ID, paxosID);
    }
    return json;
  }


  public String getName() {
    return name;
  }


  public String getPaxosID() {
    return paxosID;
  }

  public int getSender() {
    return sender;
  }
}
