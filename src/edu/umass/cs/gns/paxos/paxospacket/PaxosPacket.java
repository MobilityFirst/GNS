package edu.umass.cs.gns.paxos.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;

@Deprecated
public abstract class PaxosPacket {

  public static final String PACKET_TYPE_FIELD_NAME = "PT";

  // One type of packet can have more than one value of packetType.
  public int packetType;

  public abstract JSONObject toJSONObject() throws JSONException;

  @Override
  public String toString() {
    try {
      return this.toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static PaxosPacketType getPacketType(JSONObject json) throws JSONException {
    if (hasPacketTypeField(json)) {
      return PaxosPacketType.getPacketType(json.getInt(PACKET_TYPE_FIELD_NAME));
    }
    // Not sure what to return here
    return PaxosPacketType.NULL;
  }

  public static boolean hasPacketTypeField(JSONObject json) {
    return json.has(PACKET_TYPE_FIELD_NAME);
  }
}
