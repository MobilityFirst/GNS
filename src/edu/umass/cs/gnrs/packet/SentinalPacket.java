package edu.umass.cs.gnrs.packet;


import org.json.JSONException;
import org.json.JSONObject;

/*************************************************************
 * This class implements the packet used to mark the end.
 * 
 * @author Westy
 ************************************************************/
public class SentinalPacket extends BasicPacket {

  /*************************************************************
   * Constructs new SentinalPacket.
   ************************************************************/
  public SentinalPacket() {
    this.type = Packet.PacketType.SENTINAL;
  }

  /*************************************************************
   * Constructs new SentinalPacket from a JSONObject
   * @param json JSONObject representing this packet
   * @throws JSONException
   ************************************************************/
  public SentinalPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.SENTINAL) {
      Exception e = new Exception("SentinalPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
  }
 

  /*************************************************************
   * Converts a SentinalPacket to a JSONObject.
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
