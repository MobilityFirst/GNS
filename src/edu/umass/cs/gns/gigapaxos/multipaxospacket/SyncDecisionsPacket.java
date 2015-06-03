package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import edu.umass.cs.utils.Util;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

/* A sync reply packet contains commits missing at the
 * sending node (nodeID). The receiver is expected to 
 * send to the sender the commits it is reporting as
 * missing in this sync reply.
 */
public final class SyncDecisionsPacket extends PaxosPacket{

	public final int nodeID; // sending node
	public final int maxDecisionSlot; 	// max decided slot at nodeID
	public final ArrayList<Integer> missingSlotNumbers;
	public final boolean missingTooMuch; // can be computed from missingSlotNumbers, but can also be specified explicitly by sender

	public SyncDecisionsPacket(int nodeID, int maxDecisionSlot, ArrayList<Integer> missingSlotNumbers, boolean flag) {
		super((PaxosPacket)null);
		this.missingTooMuch = flag;
		this.packetType = (missingTooMuch ? PaxosPacketType.SYNC_DECISIONS : PaxosPacketType.SYNC_DECISIONS); // missingTooMuch => checkpoint transfer
		this.nodeID = nodeID;
		this.maxDecisionSlot = maxDecisionSlot;
		this.missingSlotNumbers = missingSlotNumbers;
	}

	public SyncDecisionsPacket(JSONObject json) throws JSONException{
		super(json);
		this.nodeID = json.getInt(PaxosPacket.NodeIDKeys.SENDER_NODE.toString());
		this.maxDecisionSlot = json.getInt(PaxosPacket.Keys.MAX_SLOT.toString());
		if (json.has(PaxosPacket.Keys.MISSING.toString()))
			missingSlotNumbers = Util.JSONArrayToArrayListInteger(json.getJSONArray(PaxosPacket.Keys.MISSING.toString()));
		else missingSlotNumbers = null;
		this.missingTooMuch = json.getBoolean(PaxosPacket.Keys.IS_MISSING_TOO_MUCH.toString());
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.SYNC_DECISIONS || PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.CHECKPOINT_REQUEST); // coz class is final
		this.packetType = PaxosPacketType.SYNC_DECISIONS;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.SENDER_NODE.toString(), nodeID);
		json.put(PaxosPacket.Keys.MAX_SLOT.toString(), maxDecisionSlot);
		json.put(PaxosPacket.Keys.IS_MISSING_TOO_MUCH.toString(),missingTooMuch);
		if (missingSlotNumbers!= null && missingSlotNumbers.size()>0)
			json.put(PaxosPacket.Keys.MISSING.toString(), new JSONArray(missingSlotNumbers));
		return json;
	}
}
