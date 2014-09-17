package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.paxos.Ballot;
import org.json.JSONException;
import org.json.JSONObject;

public class AcceptPacket extends PaxosPacket {

  /**
   * nodeID of the node that sent the message
   */
  public NodeId<String> nodeID;
  /**
   * 
   */
  public PValuePacket pValue;
  /**
   * slotNumber up to which decisions have been applied at sending node
   */
  public int slotNumberAtReplica;
  String NODE = "x1";
  String SLOT = "x2";

  public AcceptPacket(NodeId<String> nodeID, PValuePacket pValue, PaxosPacketType packetType, int slotNumber) {
    this.packetType = packetType.getInt();
    this.nodeID = nodeID;
    this.pValue = pValue;
    this.slotNumberAtReplica = slotNumber;

  }

  public AcceptPacket(JSONObject json) throws JSONException {
    this.pValue = new PValuePacket(json);
    this.packetType = json.getInt(PaxosPacket.PACKET_TYPE_FIELD_NAME);
    this.nodeID = new NodeId<String>(json.getString(NODE));
    this.slotNumberAtReplica = json.getInt(SLOT);
  }

  public AcceptPacket getAcceptReplyPacket(NodeId<String> nodeID, Ballot ballot) {
    if (ballot == null) {
      ballot = this.pValue.ballot;
    }
    ProposalPacket proposalPacket = new ProposalPacket(this.pValue.proposal.slot, pValue.proposal.req, PaxosPacketType.NULL, 0);
    PValuePacket pValuePacket = new PValuePacket(ballot, proposalPacket);
    AcceptPacket acceptPacket = new AcceptPacket(nodeID, pValuePacket, PaxosPacketType.ACCEPT_REPLY, 0);
    return acceptPacket;
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = this.pValue.toJSONObject();
    json.put(NODE, nodeID.get());
    json.put(SLOT, slotNumberAtReplica);
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, this.packetType);
    return json;
  }
}
