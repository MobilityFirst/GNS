package edu.umass.cs.gnrs.packet.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;
import paxos.Ballot;

public class AcceptPacket extends Packet{

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
    String NODE = "x1";
    String SLOT = "x2";

	
	
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

//    public void makeAcceptReplyPacket(int nodeID, int slotNumberAtReplica, Ballot ballot) {
//        this.pValue.proposal.req = null;
//        this.packetType = PaxosPacketType.ACCEPT_REPLY;
//        this.nodeID = nodeID;
//        this.slotNumberAtReplica = slotNumberAtReplica;
//        if (ballot != null) pValue.ballot = ballot;
//
//
//    }

    public AcceptPacket getAcceptReplyPacket(int nodeID,Ballot ballot) {
        if (ballot == null) {
            ballot = this.pValue.ballot;
        }
        ProposalPacket proposalPacket = new ProposalPacket(this.pValue.proposal.slot,pValue.proposal.req,0);
        PValuePacket pValuePacket = new PValuePacket(ballot,proposalPacket);
        AcceptPacket acceptPacket = new AcceptPacket(nodeID,pValuePacket,PaxosPacketType.ACCEPT_REPLY,0);
        return acceptPacket;
    }
//	public AcceptPacket getAcceptReplyPacket(int nodeID,Ballot ballot, int slotNumberAtReplica) {
//		// if ballot number is greater.
//		if (ballot != null) {
//        }
//			AcceptPacket accept =  new AcceptPacket(nodeID,
//					new PValuePacket(ballot, pValue.proposal),
//					PaxosPacketType.ACCEPT_REPLY, slotNumberAtReplica);
//
//			return accept;
//		}
//		AcceptPacket accept = new AcceptPacket(nodeID,  this.pValue,
//				PaxosPacketType.ACCEPT_REPLY, slotNumberAtReplica);
//		return accept;
//	}
	
	@Override
	public JSONObject toJSONObject() throws JSONException
	{
        JSONObject json = this.pValue.toJSONObject();
		json.put(NODE, nodeID);
		json.put(SLOT, slotNumberAtReplica);
        json.put(PaxosPacketType.ptype, this.packetType);
		return json;
	}
	
}
