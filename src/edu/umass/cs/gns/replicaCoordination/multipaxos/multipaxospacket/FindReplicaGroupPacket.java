package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;

/**
@author V. Arun
 */
public class FindReplicaGroupPacket extends PaxosPacket {
	private static final String NODE_ID="NODE_ID";
	private static final String GROUP="GROUP";

	public final int nodeID; // node ID sending the request
	public final int[] group;

	public FindReplicaGroupPacket(int id, JSONObject msg) throws JSONException {
		super(msg);
		this.packetType = PaxosPacketType.FIND_REPLICA_GROUP;
		this.nodeID = id;
		this.group=null;
	}
	public FindReplicaGroupPacket(int[] members, FindReplicaGroupPacket frg) throws JSONException {
		super(frg);
		this.packetType = PaxosPacketType.FIND_REPLICA_GROUP;
		this.nodeID = frg.nodeID;
		this.group=members;
	}
	public FindReplicaGroupPacket(JSONObject msg) throws JSONException {
		super(msg);
		this.packetType = PaxosPacketType.FIND_REPLICA_GROUP;
		this.nodeID = msg.getInt(NODE_ID);
		ArrayList<Integer> members = new ArrayList<Integer>();
		if(msg.has(GROUP)) {
			String[] tokens = msg.getString(GROUP).split("\\s");
			for(String token : tokens) {
				try {
					members.add(Integer.parseInt(token));
				} catch (NumberFormatException nfe) {nfe.printStackTrace();}
			}
		}
		if(members.size()>0) this.group = new int[members.size()];
		else this.group=null;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(NODE_ID, this.nodeID);
		if(this.group!=null && this.group.length>0) {
			String numbers = "";
			for(int i=0; i<this.group.length; i++) numbers += this.group[i] + " ";
			json.put(GROUP, numbers);
		}
		return json;
	}

	public static int getNodeID(JSONObject msg) throws JSONException {
		int id=-1;
		if(msg.has(PaxosPacket.PAXOS_TYPE)) {
			PaxosPacketType msgType = PaxosPacketType.getPaxosPacketType(msg.getInt(PaxosPacket.PAXOS_TYPE));
			switch(msgType) {
			case ACCEPT:
				id = msg.getInt(AcceptPacket.NODE);
				break;
			case ACCEPT_REPLY:
				id = msg.getInt(AcceptReplyPacket.NODE_ID);
				break;
			case PREPARE:
				id = msg.getInt(PreparePacket.COORDINATOR);
				break;
			case DECISION: 
				id = (new Ballot(msg.getString(PValuePacket.BALLOT))).coordinatorID;
				break;
			}
		}
		return id;
	}

	public static void main(String[] args) {
		try {
			JSONObject msg = new JSONObject();
			msg.put(PaxosPacket.PAXOS_ID, "paxos0");
			msg.put(PaxosPacket.PAXOS_VERSION, (short)3);
			FindReplicaGroupPacket frg = new FindReplicaGroupPacket(23, msg);
			System.out.println(frg);

			int[] members = {23, 44, 55};
			FindReplicaGroupPacket frgReply = new FindReplicaGroupPacket(members, frg);
			System.out.println(frgReply.toJSONObject());
			FindReplicaGroupPacket frgReplyCopy = new FindReplicaGroupPacket(frgReply.toJSONObject());
			assert(frgReply.nodeID==frgReplyCopy.nodeID);
			assert(frgReply.group.length==frgReplyCopy.group.length);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
