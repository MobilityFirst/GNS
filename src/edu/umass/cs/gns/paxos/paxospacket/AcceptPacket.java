package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.gns.paxos.Ballot;
import org.json.JSONException;
import org.json.JSONObject;

public class AcceptPacket<NodeIDType> extends PaxosPacket {

  /**
   * nodeID of the node that sent the message
   */
  public NodeIDType nodeID;
  /**
   * 
   */
  public PValuePacket<NodeIDType> pValue;
  /**
   * slotNumber up to which decisions have been applied at sending node
   */
  public int slotNumberAtReplica;
  String NODE = "x1";
  String SLOT = "x2";

  public AcceptPacket(NodeIDType nodeID, PValuePacket<NodeIDType> pValue, PaxosPacketType packetType, int slotNumber) {
    this.packetType = packetType.getInt();
    this.nodeID = nodeID;
    this.pValue = pValue;
    this.slotNumberAtReplica = slotNumber;

  }

  public AcceptPacket(JSONObject json) throws JSONException {
    this.pValue = new PValuePacket<NodeIDType>(json);
    this.packetType = json.getInt(PaxosPacket.PACKET_TYPE_FIELD_NAME);
    this.nodeID = (NodeIDType) json.get(NODE);
    this.slotNumberAtReplica = json.getInt(SLOT);
  }

  public AcceptPacket<NodeIDType> getAcceptReplyPacket(NodeIDType nodeID, Ballot<NodeIDType> ballot) {
    if (ballot == null) {
      ballot = this.pValue.ballot;
    }
    ProposalPacket<NodeIDType> proposalPacket = new ProposalPacket<NodeIDType>(this.pValue.proposal.slot, pValue.proposal.req, PaxosPacketType.NULL, 0);
    PValuePacket<NodeIDType> pValuePacket = new PValuePacket<NodeIDType>(ballot, proposalPacket);
    AcceptPacket<NodeIDType> acceptPacket = new AcceptPacket<NodeIDType>(nodeID, pValuePacket, PaxosPacketType.ACCEPT_REPLY, 0);
    return acceptPacket;
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = this.pValue.toJSONObject();
    json.put(NODE, nodeID);
    json.put(SLOT, slotNumberAtReplica);
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, this.packetType);
    return json;
  }
}
