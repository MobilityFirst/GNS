package edu.umass.cs.gnsserver.gnsApp.packet.admin;

import edu.umass.cs.gnsserver.gnsApp.packet.BasicPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class implements a packet that tells the name server
 * to initial the status system.
 * 
 * @author Westy
 */
public class StatusInitPacket extends BasicPacket {

  /**
   * Constructs a new status init packet
   */
  public StatusInitPacket() {
    this.type = Packet.PacketType.STATUS_INIT;
  }

  /**
   * Constructs new StatusInitPacket from a JSONObject.
   * 
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public StatusInitPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.STATUS_INIT) {
      Exception e = new Exception("StatusInitPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
  }

  /**
   * Converts a StatusInitPacket to a JSONObject.
   * 
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());

    return json;
  }
}
