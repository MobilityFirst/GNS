package edu.umass.cs.gns.packet;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 8/16/13
 * Time: 10:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class AddCompletePacket extends  BasicPacket {


  private final static String NAME = "name";

  private String name;


  public AddCompletePacket(String name) {
    this.type = Packet.PacketType.ADD_COMPLETE;
    this.name = name;
  }

  public AddCompletePacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.name = json.getString(NAME);
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(NAME, name);
    return json;
  }

  public String getName() {
    return name;
  }

}
