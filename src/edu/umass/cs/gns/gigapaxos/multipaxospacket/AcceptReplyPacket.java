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
    
    private int requestID = 0; // used only for debugging

    public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber, int committedSlot) {
    	super((PaxosPacket)null);
        this.packetType = PaxosPacketType.ACCEPT_REPLY;
        this.nodeID = nodeID;
        this.ballot = ballot;
        this.slotNumber = slotNumber;
        this.committedSlot = committedSlot;
    }    
    public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber, int committedSlot, AcceptPacket accept) {
    	this(nodeID, ballot, slotNumber, committedSlot);
    	this.setRequestID(accept.requestID);
    }
 
    public AcceptReplyPacket(JSONObject jsonObject) throws  JSONException{
    	super(jsonObject);
    	assert(PaxosPacket.getPaxosPacketType(jsonObject)==PaxosPacketType.ACCEPT_REPLY); // coz class is final
        this.packetType = PaxosPacketType.ACCEPT_REPLY;
        this.nodeID = jsonObject.getInt(PaxosPacket.NodeIDKeys.SENDER_NODE.toString());
        this.ballot = new Ballot(jsonObject.getString(PaxosPacket.NodeIDKeys.BALLOT.toString()));
        this.slotNumber = jsonObject.getInt(PaxosPacket.Keys.SLOT.toString());
        this.committedSlot = jsonObject.getInt(PaxosPacket.Keys.COMMITTED_SLOT.toString());
        this.requestID = jsonObject.getInt(RequestPacket.Keys.REQUEST_ID.toString());
    }
    
    public void setRequestID(int id) {
    	this.requestID = id;
    }
    public int getRequestID() {
    	return this.requestID;
    }


    @Override
    public JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject json= new JSONObject();
        json.put(PaxosPacket.NodeIDKeys.SENDER_NODE.toString(), nodeID);
        json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(), ballot.toString());
        json.put(PaxosPacket.Keys.SLOT.toString(), slotNumber);
        json.put(PaxosPacket.Keys.COMMITTED_SLOT.toString(), this.committedSlot);
        json.put(RequestPacket.Keys.REQUEST_ID.toString(), this.requestID);
        return json;
    }

}
