package edu.umass.cs.gnrs.packet;

import edu.umass.cs.gnrs.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

public class NameServerLoadPacket extends BasicPacket {

  private int nsID;
  private double loadValue;

  public NameServerLoadPacket(int nsID, double loadValue) {
    this.type = PacketType.NAME_SERVER_LOAD;
    this.nsID = nsID;
    this.loadValue = loadValue;
  }

  public NameServerLoadPacket(JSONObject json) throws JSONException {
    this.nsID = json.getInt("nsID");
    this.loadValue = json.getDouble("loadValue");
    this.type = PacketType.NAME_SERVER_LOAD;
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put("nsID", getNsID());
    json.put("loadValue", getLoadValue());
    return json;
  }

  /**
   * @return the nsID
   */
  public int getNsID() {
    return nsID;
  }

  /**
   * @return the loadValue
   */
  public double getLoadValue() {
    return loadValue;
  }

  public void setLoadValue(double loadValue) {
    this.loadValue = loadValue;
  }
  
}
