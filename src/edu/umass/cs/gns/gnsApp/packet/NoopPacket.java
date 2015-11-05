package edu.umass.cs.gns.gnsApp.packet;

import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class implements a packet that contains no information.
 * 
 * @author Westy
 */
public class NoopPacket extends BasicPacket implements ReconfigurableRequest {

  /**
   * Constructs a new NoopPacket.
   */
  public NoopPacket() {
    this.type = Packet.PacketType.NOOP;
  }

  /**
   * Constructs new StatusPacket from a JSONObject
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public NoopPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.NOOP) {
      throw new JSONException("NOOP: wrong packet type " + Packet.getPacketType(json));
    }
  }

  /**
   * Converts a StatusPacket to a JSONObject.
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    return json;
  }

  @Override
  public int getEpochNumber() {
    return -1;
  }

  @Override
  public boolean isStop() {
    return true;
  }

  @Override
  public String getServiceName() {
    return "Noop";
  }
}
