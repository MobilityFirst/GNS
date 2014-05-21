package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

public class NameServerLoadPacket extends BasicPacket {

  private int reportingNodeID;
  private int requestingNodeID;
  private double loadValue;

  public NameServerLoadPacket(int reportingNodeID, int requestingNodeID, double loadValue) {
    this.type = PacketType.NAME_SERVER_LOAD;
    this.reportingNodeID = reportingNodeID;
    this.loadValue = loadValue;
    this.requestingNodeID = requestingNodeID;
  }

  public NameServerLoadPacket(JSONObject json) throws JSONException {
    this.reportingNodeID = json.getInt("reportingNodeID");
    this.loadValue = json.getDouble("lV");
    this.type = PacketType.NAME_SERVER_LOAD;
    this.requestingNodeID = json.getInt("requestingNodeID");
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put("reportingNodeID", getReportingNodeID());
    json.put("lV", getLoadValue());
    json.put("requestingNodeID", requestingNodeID);
    return json;
  }

  /**
   * @return the reportingNodeID
   */
  public int getReportingNodeID() {
    return reportingNodeID;
  }

  public int getRequestingNodeID() {
    return requestingNodeID;
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
