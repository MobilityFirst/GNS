package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/5/13
 * Time: 7:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class SynchronizePacket extends PaxosPacket {

  public NodeId<String> nodeID;

  public SynchronizePacket(NodeId<String> nodeID) {
    this.packetType = PaxosPacketType.SYNC_REQUEST.getInt();
    this.nodeID = nodeID;

  }

  String NODE = "x1";

  public SynchronizePacket(JSONObject jsonObject) throws JSONException {

    this.packetType = PaxosPacketType.SYNC_REQUEST.getInt();
    this.nodeID = new NodeId<String>(jsonObject.getString(NODE));

  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, this.packetType);
    json.put(NODE, nodeID.get());
    return json;
  }
}
