
package edu.umass.cs.gnsserver.gnsapp.packet.admin;

import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithClientAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;

import org.json.JSONException;
import org.json.JSONObject;


public class SentinalPacket extends BasicPacketWithClientAddress {


  public final static String ID = "id";
  private int id;


  public SentinalPacket(int id) {
    this.type = Packet.PacketType.SENTINAL;
    this.id = id;
  }


  public SentinalPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.SENTINAL) {
      Exception e = new Exception("SentinalPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.id = json.getInt(ID);
    this.type = Packet.getPacketType(json);
  }


  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, id);
    return json;
  }


  public int getId() {
    return id;
  }
}
