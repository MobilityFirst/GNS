package edu.umass.cs.gnrs.packet.paxospacket;

import edu.umass.cs.gnrs.util.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/5/13
 * Time: 7:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class SynchronizeReplyPacket extends Packet{

    /**
     * node ID of sending node
     */
    public int nodeID;

    /**
     * maximum slot for which nodeID has received decision
     */
    public int maxDecisionSlot;

    /**
     * slot numbers less than max slot which are missing
     */
    public ArrayList<Integer> missingSlotNumbers;
    String NODE = "x1";
    String MAX_SLOT = "x2";
    String MISSING = "x3";
    String FLAG = "x4";

    public boolean flag;

    public SynchronizeReplyPacket(int nodeID, int maxDecisionSlot, ArrayList<Integer> missingSlotNumbers, boolean flag1) {
        this.packetType = PaxosPacketType.SYNC_REPLY;
        this.nodeID = nodeID;
        this.maxDecisionSlot = maxDecisionSlot;
        this.missingSlotNumbers = missingSlotNumbers;
        this.flag = flag1;
    }

    public SynchronizeReplyPacket(JSONObject json) throws JSONException{

        this.nodeID = json.getInt(NODE);
        this.maxDecisionSlot = json.getInt(MAX_SLOT);
        if (json.has(MISSING))
            missingSlotNumbers = JSONUtils.JSONArrayToArrayListInteger(json.getJSONArray(MISSING));
        else missingSlotNumbers = null;
        this.packetType = PaxosPacketType.SYNC_REPLY;
        this.flag = json.getBoolean(FLAG);
    }

    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject json = new JSONObject();
        json.put(PaxosPacketType.ptype, this.packetType);
        json.put(NODE, nodeID);
        json.put(MAX_SLOT, maxDecisionSlot);
        json.put(FLAG,flag);
        if (missingSlotNumbers!= null && missingSlotNumbers.size()>0)
            json.put(MISSING, new JSONArray(missingSlotNumbers));
        return json;

    }
}
