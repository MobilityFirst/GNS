package edu.umass.cs.gns.packet;

import org.json.JSONException;
import org.json.JSONObject;

public class UpdateNameServerPacket extends BasicPacket {

  
  public final static String NODE_ID = "nodeID";
  public final static String NAME = "name";
  public final static String LOOKUP = "lookup";
  public final static String UPDATE = "update";
  private int nodeID;
  private String name;
  private int numLookupRequest;
  private int numUpdateRequest;

  public UpdateNameServerPacket(int nodeID, String name, int numLookupRequest, int numUpdateRequest) {
    this.type = Packet.PacketType.UPDATE_NAMESERVER;
    this.nodeID = nodeID;
    this.name = name;
    this.numLookupRequest = numLookupRequest;
    this.numUpdateRequest = numUpdateRequest;
  }

  public UpdateNameServerPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.UPDATE_NAMESERVER) {
      Exception e = new Exception("UpdateNameServerPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
    }

    this.type = Packet.PacketType.UPDATE_NAMESERVER;
    this.nodeID = json.getInt(NODE_ID);
    this.name = json.getString(NAME);
    this.numLookupRequest = json.getInt(LOOKUP);
    this.numUpdateRequest = json.getInt(UPDATE);
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();

    Packet.putPacketType(json, getType());
    json.put(NODE_ID, getNodeID());
    json.put(NAME, getName());
    json.put(LOOKUP, getNumLookupRequest());
    json.put(UPDATE, getNumUpdateRequest());

    return json;
  }

  /**
   * @return the nodeID
   */
  public int getNodeID() {
    return nodeID;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the numLookupRequest
   */
  public int getNumLookupRequest() {
    return numLookupRequest;
  }

  /**
   * @return the numUpdateRequest
   */
  public int getNumUpdateRequest() {
    return numUpdateRequest;
  }
}
