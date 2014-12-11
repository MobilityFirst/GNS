package edu.umass.cs.gns.paxos.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;

public class FailureDetectionPacket<NodeIDType> extends PaxosPacket {

  /**
   * ID of node that sent the packet.
   */
  public NodeIDType senderNodeID;

  /**
   * ID of destination node.
   */
  public NodeIDType responderNodeID;

  /**
   * Status of responder node. true = node is up, false = node is down.
   */
  public boolean status;

  public FailureDetectionPacket(NodeIDType senderNodeID, NodeIDType responderNodeID,
          boolean status, PaxosPacketType packetType) {
    this.packetType = packetType.getInt();
    this.senderNodeID = senderNodeID;
    this.responderNodeID = responderNodeID;
    this.status = status;
  }

  public FailureDetectionPacket(JSONObject json) throws JSONException {
    this.packetType = json.getInt(PaxosPacket.PACKET_TYPE_FIELD_NAME);
    this.senderNodeID = (NodeIDType) json.get("sender");
    this.responderNodeID = (NodeIDType) json.get("responder");
    this.status = json.getBoolean("status");
  }

  public FailureDetectionPacket<NodeIDType> getFailureDetectionResponse() {
    return new FailureDetectionPacket(this.senderNodeID,
            this.responderNodeID, true, PaxosPacketType.FAILURE_RESPONSE);
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, packetType);
    json.put("status", status);
    json.put("sender", senderNodeID);
    json.put("responder", responderNodeID);
    return json;
  }

}
