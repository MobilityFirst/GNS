package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

public class NameServerLoadPacket extends BasicPacket {

  private NodeId<String> reportingNodeID;
  private NodeId<String> requestingNodeID;
  private double loadValue;

  public NameServerLoadPacket(NodeId<String> reportingNodeID, NodeId<String> requestingNodeID, double loadValue) {
    this.type = PacketType.NAME_SERVER_LOAD;
    this.reportingNodeID = reportingNodeID;
    this.loadValue = loadValue;
    this.requestingNodeID = requestingNodeID;
  }

  public NameServerLoadPacket(JSONObject json) throws JSONException {
    this.type = PacketType.NAME_SERVER_LOAD;
    this.reportingNodeID = new NodeId<String>(json.getString("reportingNodeID"));
    this.loadValue = json.getDouble("lV");
    this.requestingNodeID = new NodeId<String>(json.getString("requestingNodeID"));
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put("reportingNodeID", reportingNodeID.toString());
    json.put("lV", getLoadValue());
    json.put("requestingNodeID", requestingNodeID.toString());
    return json;
  }

  /**
   * @return the reportingNodeID
   */
  public NodeId<String> getReportingNodeID() {
    return reportingNodeID;
  }

  public NodeId<String> getRequestingNodeID() {
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
