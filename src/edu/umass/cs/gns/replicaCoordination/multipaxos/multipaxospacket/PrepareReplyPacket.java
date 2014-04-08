package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;

import java.util.HashMap;
import java.util.Map;

public class PrepareReplyPacket extends PaxosPacket {

	public final int coordinatorID;
	public final Ballot ballot;
	public final int receiverID;
	public  final int acceptorGCSlot;
	public final Map<Integer, PValuePacket> accepted;


	public PrepareReplyPacket(int coordinatorID, int receiverID, Ballot ballot, Map<Integer, PValuePacket> accepted, int slotNumber) {
		super(accepted==null || accepted.isEmpty() ? (PaxosPacket)null : accepted.values().iterator().next());
		this.coordinatorID = coordinatorID;
		this.receiverID = receiverID;
		this.ballot = ballot;
		this.accepted = accepted==null ? new HashMap<Integer,PValuePacket>() : accepted;
		this.acceptorGCSlot = slotNumber;
		this.packetType = PaxosPacketType.PREPARE_REPLY;
	}


	public PrepareReplyPacket(JSONObject json) throws JSONException{
		super(json);
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.PREPARE_REPLY);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.coordinatorID = json.getInt("coordinatorID");
		this.receiverID = json.getInt("receiverID");
		this.ballot = new Ballot(json.getString("ballot"));
		this.acceptorGCSlot = json.getInt("slotNumber");
		this.accepted = parseJsonForAccepted(json);
	}

	private HashMap<Integer, PValuePacket> parseJsonForAccepted(JSONObject json) throws JSONException {
		HashMap<Integer, PValuePacket> accepted = new HashMap<Integer, PValuePacket>();
		if (json.has("accepted")) {
			JSONArray jsonArray = json.getJSONArray("accepted");
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject element = jsonArray.getJSONObject(i);
				PValuePacket pval = new PValuePacket(element);
				accepted.put(pval.slot, pval);
			}
		}
		return accepted;
	}


	@Override
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put("coordinatorID", coordinatorID);
		json.put("receiverID", receiverID);
		json.put("ballot", ballot.toString());
		json.put("slotNumber", acceptorGCSlot);
		assert(this.packetType == PaxosPacketType.PREPARE_REPLY);
		addAcceptedToJSON(json);
		return json;
	}

	private void addAcceptedToJSON(JSONObject json) throws JSONException{
		if (accepted != null ) {
			JSONArray jsonArray  = new JSONArray();
			for (PValuePacket pValue : accepted.values()) {
				jsonArray.put(pValue.toJSONObject());
			}
			json.put("accepted", jsonArray);
		}
	}

}
