/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxosutil;

import java.io.IOException;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.InterfaceNIOTransport;
import edu.umass.cs.nio.JSONMessenger;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This class is simply JSONMessenger adapted to use MessagingTask
 *            instead of GenericMessagingTask and to fix integer node IDs to
 *            NodeIDType as needed.
 * 
 *            It's a utility because there is nothing paxos specific here.
 */
@SuppressWarnings("javadoc")
public class Messenger<NodeIDType> extends JSONMessenger<NodeIDType> {

	private final IntegerMap<NodeIDType> nodeMap;

	public Messenger(InterfaceNIOTransport<NodeIDType, JSONObject> niot,
			IntegerMap<NodeIDType> nodeMap) {
		super(niot);
		this.nodeMap = nodeMap;
	}

	public void send(MessagingTask mtask) throws JSONException, IOException {
		// need to convert integers to NodeIDType.toString before sending
		super.send(toGeneric(mtask));
	}

	private JSONObject[] toJSONObjects(PaxosPacket[] msgs) throws JSONException {
		JSONObject[] jsonArray = new JSONObject[msgs.length];
		for (int i = 0; i < msgs.length; i++) {
			jsonArray[i] = fixNodeIntToString(msgs[i].toJSONObject());
		}
		return jsonArray;
	}

	private GenericMessagingTask<NodeIDType, JSONObject> toGeneric(
			MessagingTask mtask) throws JSONException {
		Set<NodeIDType> nodes = this.nodeMap
				.getIntArrayAsNodeSet(mtask.recipients);
		//PaxosManager.getLogger().info("***********" + Util.arrayOfIntToString(mtask.recipients) + " => " + nodes);
		return new GenericMessagingTask<NodeIDType, JSONObject>(
				nodes.toArray(), toJSONObjects(mtask.msgs));
	}

	// convert int to NodeIDType to String
	private JSONObject fixNodeIntToString(JSONObject json) throws JSONException {
		if (json.has(PaxosPacket.NodeIDKeys.BALLOT.toString())) {
			// fix ballot string
			Ballot ballot = new Ballot(
					json.getString(PaxosPacket.NodeIDKeys.BALLOT.toString()));
			json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(), Ballot
					.getBallotString(ballot.ballotNumber,
							intToString(ballot.coordinatorID)));
		} else if (json.has(PaxosPacket.NodeIDKeys.GROUP.toString())) {
			// fix group JSONArray
			JSONArray jsonArray = json
					.getJSONArray(PaxosPacket.NodeIDKeys.GROUP.toString());
			for (int i = 0; i < jsonArray.length(); i++) {
				int member = jsonArray.getInt(i);
				jsonArray.put(i, intToString(member));
			}
			json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), jsonArray);
		} else
			for (PaxosPacket.NodeIDKeys key : PaxosPacket.NodeIDKeys.values()) {
				if (json.has(key.toString())) {
					// simple default int->string fix
					int id = json.getInt(key.toString());
					json.put(key.toString(), intToString(id));
				}
			}
		return json;
	}

	private String intToString(int id) {
		return this.nodeMap.get(id).toString();
	}
}
