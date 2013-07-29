package edu.umass.cs.gnrs.packet;


import org.json.JSONException;
import org.json.JSONObject;

/*************************************************************
 * This class implements a packet that contains status information
 * 
 * @author Westy
 ************************************************************/
public class StatusInitPacket extends BasicPacket {
  
  /**
   * Constructs a new status init packet
   * @param id
   * @param jsonObject 
   */
  public StatusInitPacket() {
    this.type = Packet.PacketType.STATUS_INIT;
  }

  /*************************************************************
   * Constructs new StatusPacket from a JSONObject
   * @param json JSONObject representing this packet
   * @throws JSONException
   ************************************************************/
  public StatusInitPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.STATUS_INIT) {
      Exception e = new Exception("StatusPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
  }

  /*************************************************************
   * Converts a ActiveNSUpdatePacket to a JSONObject.
   * @return JSONObject representing this packet.
   * @throws JSONException
   ************************************************************/
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());

    return json;
  }

}
