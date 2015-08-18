/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxospackets;

import java.util.Arrays;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.utils.Util;

/**
 * 
 * @author arun
 *
 *         This is a funky packet that is used to create paxos groups at nodes
 *         that missed its birthing, say, because they were down then. If a node
 *         receives a paxos packet for which it has no state, it uses this
 *         packet to find the group membership and create the paxos instance
 *         locally.
 */

@SuppressWarnings("javadoc")
public class FindReplicaGroupPacket extends PaxosPacket {

	/**
	 * Node ID sending the request.
	 */
	public final int nodeID;
	/**
	 * Replica group if known.
	 */
	public final int[] group;

	public FindReplicaGroupPacket(int id, JSONObject msg) throws JSONException {
		super(msg);
		this.packetType = PaxosPacketType.FIND_REPLICA_GROUP;
		this.nodeID = id;
		this.group = null;
	}

	public FindReplicaGroupPacket(int id, PaxosPacket pp) throws JSONException {
		super(pp);
		this.packetType = PaxosPacketType.FIND_REPLICA_GROUP;
		this.nodeID = id;
		this.group = null;
	}

	public FindReplicaGroupPacket(int[] members, FindReplicaGroupPacket frg)
			throws JSONException {
		super(frg);
		this.packetType = PaxosPacketType.FIND_REPLICA_GROUP;
		this.nodeID = frg.nodeID;
		this.group = members;
	}

	public FindReplicaGroupPacket(JSONObject msg) throws JSONException {
		super(msg);
		this.packetType = PaxosPacketType.FIND_REPLICA_GROUP;
		this.nodeID = msg.getInt(PaxosPacket.NodeIDKeys.SNDR.toString());
		JSONArray jsonGroup = null;
		if (msg.has(PaxosPacket.NodeIDKeys.GROUP.toString())) {
			jsonGroup = msg.getJSONArray(PaxosPacket.NodeIDKeys.GROUP
					.toString());
		}
		if (jsonGroup != null && jsonGroup.length() > 0) {
			this.group = new int[jsonGroup.length()];
			for (int i = 0; i < jsonGroup.length(); i++) {
				this.group[i] = Integer.valueOf(jsonGroup.getString(i));
			}
		} else
			this.group = null;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.SNDR.toString(), this.nodeID);
		if (this.group != null && this.group.length > 0) {
			JSONArray jsonGroup = new JSONArray(Util.arrayToIntSet(group));
			json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), jsonGroup);
		}
		return json;
	}

	public static int getNodeID(JSONObject msg) throws JSONException {
		int id = -1;
		if (msg.has(PaxosPacket.Keys.PT.toString())) {
			PaxosPacketType msgType = PaxosPacketType.getPaxosPacketType(msg
					.getInt(PaxosPacket.Keys.PT.toString()));
			switch (msgType) {
			case ACCEPT:
				id = msg.getInt(PaxosPacket.NodeIDKeys.SNDR.toString());
				break;
			case ACCEPT_REPLY:
				id = msg.getInt(PaxosPacket.NodeIDKeys.SNDR.toString());
				break;
			case PREPARE:
				id = (new Ballot(msg.getString(PaxosPacket.NodeIDKeys.B
						.toString()))).coordinatorID;
				// msg.getInt(PaxosPacket.NodeIDKeys.COORDINATOR.toString());
				break;
			case DECISION:
				id = (new Ballot(msg.getString(PaxosPacket.NodeIDKeys.B
						.toString()))).coordinatorID;
				break;
			default:
				break;
			}
		}
		return id;
	}

	public static int getNodeID(PaxosPacket pp) throws JSONException {
		int id = -1;
		assert (pp.getType() != null);
		PaxosPacketType msgType = pp.getType();
		switch (msgType) {
		case ACCEPT:
			id = ((AcceptPacket) pp).sender;
			break;
		case ACCEPT_REPLY:
			// this can actually never happen
			id = ((AcceptReplyPacket) pp).acceptor;
			break;
		case PREPARE:
			id = ((PreparePacket) pp).ballot.coordinatorID;
			break;
		case DECISION:
			id = ((PValuePacket) pp).ballot.coordinatorID;
			break;
		default:
			break;
		}
		return id;
	}

	public static void main(String[] args) {
		try {
			JSONObject msg = new JSONObject();
			msg.put(PaxosPacket.Keys.ID.toString(), "paxos0");
			msg.put(PaxosPacket.Keys.V.toString(), 3);
			FindReplicaGroupPacket frg = new FindReplicaGroupPacket(23, msg);
			System.out.println(frg);

			int[] members = { 23, 44, 55 };
			FindReplicaGroupPacket frgReply = new FindReplicaGroupPacket(
					members, frg);
			System.out.println(frgReply.toJSONObject());
			FindReplicaGroupPacket frgReplyCopy = new FindReplicaGroupPacket(
					frgReply.toJSONObject());
			assert (frgReply.nodeID == frgReplyCopy.nodeID);
			assert (frgReply.group.length == frgReplyCopy.group.length);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected String getSummaryString() {
		return nodeID + Arrays.toString(group);
	}
}
