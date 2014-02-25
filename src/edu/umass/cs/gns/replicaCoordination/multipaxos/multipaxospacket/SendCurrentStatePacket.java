package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.PaxosPacket;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.paxos.Ballot;

public class SendCurrentStatePacket extends PaxosPacket {

	/**
	 * ID of the node sending its state
	 */
	public int sendingNodeID;

	/**
	 * currentBallot accepted by the sending node
	 */
	public Ballot currentBallot;

	/**
	 * if the node has any prepare messages pending.
	 */
	public Ballot prepareBallot;


	/**
	 * current slot number at this node.
	 */
	public int slotNumber;

	public JSONObject decisions;

	//    public String state;


	public int getType() {
		return this.packetType;
	}

	public SendCurrentStatePacket(int sendingNodeID, Ballot currentBallot, 
			Ballot prepareBallot, int packetType, int slotNumber, JSONObject decisions) {
		this.sendingNodeID = sendingNodeID;
		this.currentBallot = currentBallot;
		this.prepareBallot = prepareBallot;
		this.packetType =  packetType;
		this.slotNumber = slotNumber;
		this.decisions = decisions;
		//        this.state = state;
	}

	public SendCurrentStatePacket(JSONObject json) throws JSONException{
		this.packetType = json.getInt(PaxosPacketType.ptype);
		this.sendingNodeID = json.getInt("sendingNodeID");
		String curBallot = json.getString("currentBallot");
		if (curBallot.length() == 0) {
			this.currentBallot = null;
		}
		else {
			this.currentBallot = new Ballot(curBallot);
		}

		String prepareBallot = json.getString("prepareBallot");
		if (prepareBallot.length() == 0) {
			this.prepareBallot = null;
		}
		else {
			this.prepareBallot = new Ballot(prepareBallot);
		}
		this.slotNumber = json.getInt("slotNumber");
		if (json.get("decisions").equals(null)) {
			this.decisions = new JSONObject();
		}
		else {
			this.decisions = json.getJSONObject("decisions");
		}

		//        this.state = json.getString("state");

	}

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacketType.ptype, this.packetType);
	    Packet.putPacketType(json, PacketType.PAXOS_PACKET); json.put(PaxosPacket.paxosIDKey, this.paxosID);

		json.put("sendingNodeID", sendingNodeID);
		String ballot = "";
		if (currentBallot != null){
			ballot = currentBallot.toString();
		}
		json.put("currentBallot", ballot);

		ballot = "";
		if (prepareBallot != null) {
			ballot = prepareBallot.toString();
		}

		json.put("prepareBallot", ballot);
		json.put("slotNumber", slotNumber);
		json.put("decisions", decisions);

		//        json.put("state", state);
		//		System.out.println(decisions);
		//		System.out.println("JSON Object:" + json.toString());
		return json;
	}

	public static void main(String[] args) {

	}


}
