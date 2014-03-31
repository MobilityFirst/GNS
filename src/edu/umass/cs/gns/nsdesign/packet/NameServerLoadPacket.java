package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

public class NameServerLoadPacket extends BasicPacket {

  private int nsID;
  private int lnsID;
  private double loadValue;

  public NameServerLoadPacket(int nsID, int lnsID, double loadValue) {
    this.type = PacketType.NAME_SERVER_LOAD;
    this.nsID = nsID;
    this.loadValue = loadValue;
    this.lnsID = lnsID;
  }

  public NameServerLoadPacket(JSONObject json) throws JSONException {
    this.nsID = json.getInt("nsID");
    this.loadValue = json.getDouble("lV");
    this.type = PacketType.NAME_SERVER_LOAD;
    this.lnsID = json.getInt("lnsID");
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put("nsID", getNsID());
    json.put("lV", getLoadValue());
    json.put("lnsID", lnsID);
    return json;
  }

  /**
   * @return the nsID
   */
  public int getNsID() {
    return nsID;
  }

  public int getLnsID() {
    return lnsID;
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
