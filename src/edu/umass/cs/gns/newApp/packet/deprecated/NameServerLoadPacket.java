package edu.umass.cs.gns.newApp.packet.deprecated;

import edu.umass.cs.gns.newApp.packet.BasicPacket;
import edu.umass.cs.gns.newApp.packet.Packet;
import edu.umass.cs.gns.newApp.packet.Packet.PacketType;
import edu.umass.cs.gns.nio.Stringifiable;

import org.json.JSONException;
import org.json.JSONObject;

@Deprecated
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

  public NameServerLoadPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    this.type = PacketType.NAME_SERVER_LOAD;
    this.reportingNodeID = unstringer.valueOf(json.getString("reportingNodeID"));
    this.loadValue = json.getDouble("lV");
    this.requestingNodeID = unstringer.valueOf(json.getString("requestingNodeID"));
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
