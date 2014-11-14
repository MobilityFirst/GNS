package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.gigapaxos.paxosutil.Ballot;

/**
 * 
 * @author V. Arun
 *
 */
public final class AcceptReplyPacket extends PaxosPacket {
    public final int nodeID; // sender nodeID
    public final Ballot ballot;
    public final int slotNumber;

    public final int committedSlot;

    /*
    protected static final  String NODE_ID = "node";
    private static final  String BALLOT_NUMBER = "ballot";
    private static final  String SLOT_NUMBER = "slot";
    private static final  String COMMITTED_SLOT = "committed_slot";
    */
    
    public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber, int committedSlot) {
    	super((PaxosPacket)null);
        this.packetType = PaxosPacketType.ACCEPT_REPLY;
        this.nodeID = nodeID;
        this.ballot = ballot;
        this.slotNumber = slotNumber;
        this.committedSlot = committedSlot;
    }
 
    public AcceptReplyPacket(JSONObject jsonObject) throws  JSONException{
    	super(jsonObject);
    	assert(PaxosPacket.getPaxosPacketType(jsonObject)==PaxosPacketType.ACCEPT_REPLY); // coz class is final
        this.packetType = PaxosPacketType.ACCEPT_REPLY;
        this.nodeID = jsonObject.getInt(PaxosPacket.NodeIDKeys.SENDER_NODE.toString());
        this.ballot = new Ballot(jsonObject.getString(PaxosPacket.NodeIDKeys.BALLOT.toString()));
        this.slotNumber = jsonObject.getInt(PaxosPacket.Keys.SLOT.toString());
        this.committedSlot = jsonObject.getInt(PaxosPacket.Keys.COMMITTED_SLOT.toString());
    }


    @Override
    public JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject json= new JSONObject();
        json.put(PaxosPacket.NodeIDKeys.SENDER_NODE.toString(), nodeID);
        json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(), ballot.toString());
        json.put(PaxosPacket.Keys.SLOT.toString(), slotNumber);
        json.put(PaxosPacket.Keys.COMMITTED_SLOT.toString(), this.committedSlot);
        return json;
    }

}
