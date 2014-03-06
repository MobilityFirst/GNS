package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.PaxosPacket;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.Ballot;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/25/13
 * Time: 8:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class AcceptReplyPacket extends PaxosPacket {
    /**
     * nodeID of the node that sent the message
     */
    public int nodeID;

    /**
     * ballot number
     */
    public Ballot ballot;

    /**
     * slot number
     */
    public int slotNumber;

//    /**
//     * slot number
//     */
//    public int commitSlot;

    private static final  String NODE_ID = "node";
    private static final  String BALLOT_NUMBER = "ballot";
    private static final  String SLOT_NUMBER = "slot";
//    private static final  String COMMIT_SLOT = "ar4";

    public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber) {//, int commitSlot
        this.packetType = PaxosPacketType.ACCEPT_REPLY;
        this.nodeID = nodeID;
        this.ballot = ballot;
        this.slotNumber = slotNumber;
//        this.commitSlot = commitSlot;
    }
    public int getType() {
  	  return this.packetType;
    }

    public AcceptReplyPacket(JSONObject jsonObject) throws  JSONException{
        this.packetType = PaxosPacketType.ACCEPT_REPLY;
        this.nodeID = jsonObject.getInt(NODE_ID);
        this.ballot = new Ballot(jsonObject.getString(BALLOT_NUMBER));
        this.slotNumber = jsonObject.getInt(SLOT_NUMBER);
//        this.commitSlot = jsonObject.getInt(COMMIT_SLOT);
    }


    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject json= new JSONObject();
        json.put(PaxosPacketType.ptype, PaxosPacketType.ACCEPT_REPLY);
        Packet.putPacketType(json, PacketType.PAXOS_PACKET); json.put(PaxosPacket.paxosIDKey, this.paxosID);
        
        json.put(NODE_ID, nodeID);
        json.put(BALLOT_NUMBER, ballot.toString());
        json.put(SLOT_NUMBER, slotNumber);
//        jsonObject.put(COMMIT_SLOT, slotNumber);
        return json;
    }

}
