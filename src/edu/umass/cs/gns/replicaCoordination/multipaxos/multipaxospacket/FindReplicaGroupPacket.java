package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;
import edu.umass.cs.gns.util.Util;

/**
 * @author V. Arun
 */
public class FindReplicaGroupPacket extends PaxosPacket {

  private static final String NODE_ID = "NODE_ID";
  private static final String GROUP = "GROUP";

  public final NodeId<String> nodeID; // node ID sending the request
  public final NodeId<String>[] group;

  public FindReplicaGroupPacket(NodeId<String> id, JSONObject msg) throws JSONException {
    super(msg);
    this.packetType = PaxosPacketType.FIND_REPLICA_GROUP;
    this.nodeID = id;
    this.group = null;
  }

  public FindReplicaGroupPacket(NodeId<String>[] members, FindReplicaGroupPacket frg) throws JSONException {
    super(frg);
    this.packetType = PaxosPacketType.FIND_REPLICA_GROUP;
    this.nodeID = frg.nodeID;
    this.group = members;
  }

  public FindReplicaGroupPacket(JSONObject msg) throws JSONException {
    super(msg);
    this.packetType = PaxosPacketType.FIND_REPLICA_GROUP;
    this.nodeID = new NodeId<String>(msg.getString(NODE_ID));
    ArrayList<NodeId<String>> members = new ArrayList<NodeId<String>>();
    if (msg.has(GROUP)) {
      this.group = Util.stringToNodeIdArray(msg.getString(GROUP));
    } else {
      this.group = null;
    }
  }

  @Override
  public JSONObject toJSONObjectImpl() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(NODE_ID, this.nodeID);
    if (this.group != null && this.group.length > 0) {
      json.put(GROUP, Util.arrayOfNodeIdsToString(group));
    }
    return json;
  }

  public static NodeId<String> getNodeID(JSONObject msg) throws JSONException {
    NodeId<String> id = GNSNodeConfig.INVALID_NAME_SERVER_ID;
    if (msg.has(PaxosPacket.PAXOS_PACKET_TYPE)) {
      PaxosPacketType msgType = PaxosPacketType.getPaxosPacketType(msg.getInt(PaxosPacket.PAXOS_PACKET_TYPE));
      switch (msgType) {
        case ACCEPT:
          id = new NodeId<String>(msg.getString(AcceptPacket.NODE));
          break;
        case ACCEPT_REPLY:
          id = new NodeId<String>(msg.getInt(AcceptReplyPacket.NODE_ID));
          break;
        case PREPARE:
          id = new NodeId<String>(msg.getInt(PreparePacket.COORDINATOR));
          break;
        case DECISION:
          id = (new Ballot(msg.getString(PValuePacket.BALLOT))).coordinatorID;
          break;
      }
    }
    return id;
  }

  public static void main(String[] args) {
    try {
      JSONObject msg = new JSONObject();
      msg.put(PaxosPacket.PAXOS_ID, "paxos0");
      msg.put(PaxosPacket.PAXOS_VERSION, (short) 3);
      FindReplicaGroupPacket frg = new FindReplicaGroupPacket(new NodeId<String>(23), msg);
      System.out.println(frg);

      NodeId[] members = {new NodeId<String>(23), new NodeId<String>(44), new NodeId<String>(55)};
      FindReplicaGroupPacket frgReply = new FindReplicaGroupPacket(members, frg);
      System.out.println(frgReply.toJSONObject());
      FindReplicaGroupPacket frgReplyCopy = new FindReplicaGroupPacket(frgReply.toJSONObject());
      assert (frgReply.nodeID.equals(frgReplyCopy.nodeID));
      assert (frgReply.group.length == frgReplyCopy.group.length);
      System.out.println(frgReplyCopy.toJSONObject());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
