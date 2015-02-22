package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class FindReplicaGroupPacket extends PaxosPacket {
	
	//private static final String NODE_ID="NODE_ID";
	//private static final String GROUP="GROUP";

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
		this.nodeID = msg.getInt(PaxosPacket.NodeIDKeys.SENDER_NODE.toString());
		JSONArray jsonGroup = null;
		if(msg.has(PaxosPacket.NodeIDKeys.GROUP.toString())) {
			jsonGroup = msg.getJSONArray(PaxosPacket.NodeIDKeys.GROUP.toString());
		}
		if(jsonGroup!=null && jsonGroup.length()>0) {
			this.group = new int[jsonGroup.length()];
			for(int i=0; i<jsonGroup.length(); i++) {
				this.group[i] = Integer.valueOf(jsonGroup.getString(i));
			}
		}
		else this.group = null;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.SENDER_NODE.toString(), this.nodeID);
		if(this.group!=null && this.group.length>0) {
			JSONArray jsonGroup = new JSONArray(Util.arrayToIntSet(group));
			json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), jsonGroup);
		}
		return json;
	}

	public static int getNodeID(JSONObject msg) throws JSONException {
		int id=-1;
		if(msg.has(PaxosPacket.PAXOS_PACKET_TYPE)) {
			PaxosPacketType msgType = PaxosPacketType.getPaxosPacketType(msg.getInt(PaxosPacket.PAXOS_PACKET_TYPE));
			switch(msgType) {
			case ACCEPT:
				id = msg.getInt(AcceptPacket.Keys.SENDER_NODE.toString());
				break;
			case ACCEPT_REPLY:
				id = msg.getInt(PaxosPacket.NodeIDKeys.SENDER_NODE.toString());
				break;
			case PREPARE:
				id = msg.getInt(PaxosPacket.NodeIDKeys.COORDINATOR.toString());
				break;
			case DECISION: 
				id = (new Ballot(msg.getString(PaxosPacket.NodeIDKeys.BALLOT.toString()))).coordinatorID;
				break;
			default:
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
