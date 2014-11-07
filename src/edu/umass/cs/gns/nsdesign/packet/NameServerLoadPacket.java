package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

public class NameServerLoadPacket<NodeIDType> extends BasicPacket {

  private final NodeIDType reportingNodeID;
  private final NodeIDType requestingNodeID;
  private double loadValue;

  public NameServerLoadPacket(NodeIDType reportingNodeID, NodeIDType requestingNodeID, double loadValue) {
    this.type = PacketType.NAME_SERVER_LOAD;
    this.reportingNodeID = reportingNodeID;
    this.loadValue = loadValue;
    this.requestingNodeID = requestingNodeID;
  }

  public NameServerLoadPacket(JSONObject json) throws JSONException {
    this.type = PacketType.NAME_SERVER_LOAD;
    this.reportingNodeID = (NodeIDType)json.get("reportingNodeID");
    this.loadValue = json.getDouble("lV");
    this.requestingNodeID = (NodeIDType)json.get("requestingNodeID");
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put("reportingNodeID", reportingNodeID);
    json.put("lV", getLoadValue());
    json.put("requestingNodeID", requestingNodeID);
    return json;
  }

  /**
   * @return the reportingNodeID
   */
  public NodeIDType getReportingNodeID() {
    return reportingNodeID;
  }

  public NodeIDType getRequestingNodeID() {
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
