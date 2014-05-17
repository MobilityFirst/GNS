package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import edu.umass.cs.gns.util.JSONUtils;
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

	private final static String NODE = "NODE_ID";
	private final static String MAX_SLOT = "MAX_SLOT";
	private final static String MISSING = "MISSING";
	private final static String FLAG = "MISSING_TOO_MUCH";

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
		this.nodeID = json.getInt(NODE);
		this.maxDecisionSlot = json.getInt(MAX_SLOT);
		if (json.has(MISSING))
			missingSlotNumbers = JSONUtils.JSONArrayToArrayListInteger(json.getJSONArray(MISSING));
		else missingSlotNumbers = null;
		this.missingTooMuch = json.getBoolean(FLAG);
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.SYNC_DECISIONS || PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.CHECKPOINT_REQUEST); // coz class is final
		this.packetType = PaxosPacketType.SYNC_DECISIONS;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(NODE, nodeID);
		json.put(MAX_SLOT, maxDecisionSlot);
		json.put(FLAG,missingTooMuch);
		if (missingSlotNumbers!= null && missingSlotNumbers.size()>0)
			json.put(MISSING, new JSONArray(missingSlotNumbers));
		return json;
	}
}
