package edu.umass.cs.gns.packet.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gns.paxos.Ballot;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/25/13
 * Time: 8:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class AcceptReplyPacket extends Packet {
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

    private static final  String NODE_ID = "ar1";
    private static final  String BALLOT_NUMBER = "ar2";
    private static final  String SLOT_NUMBER = "ar3";

    public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber) {
        this.packetType = PaxosPacketType.ACCEPT_REPLY;
        this.nodeID = nodeID;
        this.ballot = ballot;
        this.slotNumber = slotNumber;
    }

    public AcceptReplyPacket(JSONObject jsonObject) throws  JSONException{
        this.packetType = PaxosPacketType.ACCEPT_REPLY;
        this.nodeID = jsonObject.getInt(NODE_ID);
        this.ballot = new Ballot(jsonObject.getString(BALLOT_NUMBER));
        this.slotNumber = jsonObject.getInt(SLOT_NUMBER);
    }


    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject jsonObject= new JSONObject();
        jsonObject.put(PaxosPacketType.ptype, PaxosPacketType.ACCEPT_REPLY);
        jsonObject.put(NODE_ID, nodeID);
        jsonObject.put(BALLOT_NUMBER, ballot.toString());
        jsonObject.put(SLOT_NUMBER, slotNumber);
        return jsonObject;
    }

}
