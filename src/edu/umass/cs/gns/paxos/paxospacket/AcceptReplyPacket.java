package edu.umass.cs.gns.paxos.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gns.paxos.Ballot;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/25/13
 * Time: 8:49 AM
 * To change this template use File | Settings | File Templates.
 * @param <NodeIDType>
 */
public class AcceptReplyPacket<NodeIDType> extends PaxosPacket {
    /**
     * nodeID of the node that sent the message
     */
    public NodeIDType nodeID;

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

    private static final  String NODE_ID = "ar1";
    private static final  String BALLOT_NUMBER = "ar2";
    private static final  String SLOT_NUMBER = "ar3";
//    private static final  String COMMIT_SLOT = "ar4";

    public AcceptReplyPacket(NodeIDType nodeID, Ballot ballot, int slotNumber) {//, int commitSlot
        this.packetType = PaxosPacketType.ACCEPT_REPLY.getInt();
        this.nodeID = nodeID;
        this.ballot = ballot;
        this.slotNumber = slotNumber;
//        this.commitSlot = commitSlot;
    }

    public AcceptReplyPacket(JSONObject jsonObject) throws  JSONException{
        this.packetType = PaxosPacketType.ACCEPT_REPLY.getInt();
        this.nodeID = (NodeIDType) jsonObject.getString(NODE_ID);
        this.ballot = new Ballot(jsonObject.getString(BALLOT_NUMBER));
        this.slotNumber = jsonObject.getInt(SLOT_NUMBER);
//        this.commitSlot = jsonObject.getInt(COMMIT_SLOT);
    }

    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject= new JSONObject();
        jsonObject.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, this.packetType);
        jsonObject.put(NODE_ID, nodeID);
        jsonObject.put(BALLOT_NUMBER, ballot.toString());
        jsonObject.put(SLOT_NUMBER, slotNumber);
//        jsonObject.put(COMMIT_SLOT, slotNumber);
        return jsonObject;
    }

}
