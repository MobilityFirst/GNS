package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.PaxosPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.Ballot;

public class AcceptPacket extends PaxosPacket {

  /**
   * nodeID of the node that sent the message
   */
  public int nodeID;
  /**
   * 
   */
  public PValuePacket pValue;
  /**
   * slotNumber up to which decisions have been applied at sending node
   */
  public int slotNumberAtReplica;
  String NODE = "node";
  String SLOT = "slot#@replica";

  public AcceptPacket(int nodeID, PValuePacket pValue, int packetType, int slotNumber) {
    this.packetType = packetType;
    this.nodeID = nodeID;
    this.pValue = pValue;
    this.slotNumberAtReplica = slotNumber;
  }

  public AcceptPacket(JSONObject json) throws JSONException {
    this.pValue = new PValuePacket(json);
    this.packetType = json.getInt(PaxosPacketType.ptype);
    this.nodeID = json.getInt(NODE);
    this.slotNumberAtReplica = json.getInt(SLOT);
  }

  public AcceptPacket getAcceptReplyPacket(int nodeID, Ballot ballot) {
    if (ballot == null) {
      ballot = this.pValue.ballot;
    }
    ProposalPacket proposalPacket = new ProposalPacket(this.pValue.proposal.slot, pValue.proposal.req, 0, 0);
    PValuePacket pValuePacket = new PValuePacket(ballot, proposalPacket);
    AcceptPacket acceptPacket = new AcceptPacket(nodeID, pValuePacket, PaxosPacketType.ACCEPT_REPLY, 0);
    return acceptPacket;
  }
  
  public int getType() {
	  return this.packetType;
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = this.pValue.toJSONObject();
    json.put(NODE, nodeID);
    json.put(SLOT, slotNumberAtReplica);
    json.put(PaxosPacketType.ptype, this.packetType);
    Packet.putPacketType(json, PacketType.PAXOS_PACKET); json.put(PaxosPacket.paxosIDKey, this.paxosID);
	json.put(PaxosPacket.paxosIDKey, this.getPaxosID()); 
    return json;
  }
}
