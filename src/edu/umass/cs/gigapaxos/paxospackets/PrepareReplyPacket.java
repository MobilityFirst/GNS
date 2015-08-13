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
package edu.umass.cs.gigapaxos.paxospackets;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public class PrepareReplyPacket extends PaxosPacket {

	/**
	 * Ballot of the PREPARE being replied to.
	 */
	public final Ballot ballot;
	/**
	 * Node ID of replier.
	 */
	public final int acceptor;
	/**
	 * Accepted pvalues from lower ballots.
	 */
	public final Map<Integer, PValuePacket> accepted;

	// first pvalue slot in accepted pvalues
	private int firstSlot;

	public PrepareReplyPacket(int receiverID, Ballot ballot,
			Map<Integer, PValuePacket> accepted, int gcSlot) {
		super(accepted == null || accepted.isEmpty() ? (PaxosPacket) null
				: accepted.values().iterator().next());
		this.acceptor = receiverID;
		this.ballot = ballot;
		this.accepted = accepted == null ? new HashMap<Integer, PValuePacket>()
				: accepted;
		this.firstSlot = gcSlot + 1;
		this.packetType = PaxosPacketType.PREPARE_REPLY;
	}

	public PrepareReplyPacket(JSONObject json) throws JSONException {
		super(json);
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.PREPARE_REPLY);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.acceptor = json.getInt(PaxosPacket.NodeIDKeys.ACCPTR.toString());
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.B
				.toString()));
		this.accepted = parseJsonForAccepted(json);
		this.firstSlot = json.getInt(PaxosPacket.Keys.PREPLY_MIN
				.toString());
	}

	private HashMap<Integer, PValuePacket> parseJsonForAccepted(JSONObject json)
			throws JSONException {
		HashMap<Integer, PValuePacket> accepted = new HashMap<Integer, PValuePacket>();
		if (json.has(PaxosPacket.Keys.ACC_MAP.toString())) {
			JSONArray jsonArray = json
					.getJSONArray(PaxosPacket.Keys.ACC_MAP.toString());
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject element = jsonArray.getJSONObject(i);
				PValuePacket pval = new PValuePacket(element);
				accepted.put(pval.slot, pval);
			}
		}
		return accepted;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.ACCPTR.toString(), acceptor);
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		assert (this.packetType == PaxosPacketType.PREPARE_REPLY);
		addAcceptedToJSON(json);
		json.put(PaxosPacket.Keys.PREPLY_MIN.toString(),
				this.firstSlot);
		return json;
	}

	public void setFirstSlot(int gcSlot) {
		this.firstSlot = gcSlot + 1;
	}

	public int getMaxSlot() {
		return getMaxSlot(this.accepted);
	}

	public int getMinSlot() {
		return getMinSlot(this.accepted);
	}

	private int getMinSlot(Map<Integer, PValuePacket> acceptedMap) {
		Integer minSlot = null;
		if (acceptedMap != null && !acceptedMap.isEmpty()) {
			for (Integer curSlot : acceptedMap.keySet()) {
				if (minSlot == null)
					minSlot = curSlot;
				if (curSlot - minSlot < 0)
					minSlot = curSlot;
			}
		} else
			minSlot = this.firstSlot;
		return minSlot;
	}

	// FIXME: wraparound
	private int getMaxSlot(Map<Integer, PValuePacket> acceptedMap) {
		Integer maxSlot = null;
		if (acceptedMap != null && !acceptedMap.isEmpty()) {
			for (Integer curSlot : acceptedMap.keySet()) {
				if (maxSlot == null)
					maxSlot = curSlot;
				if (curSlot - maxSlot > 0)
					maxSlot = curSlot;
			}
		} else
			maxSlot = this.firstSlot;
		return maxSlot;
	}

	private void addAcceptedToJSON(JSONObject json) throws JSONException {
		if (accepted != null) {
			JSONArray jsonArray = new JSONArray();
			for (PValuePacket pValue : accepted.values()) {
				jsonArray.put(pValue.toJSONObject());
			}
			json.put(PaxosPacket.Keys.ACC_MAP.toString(), jsonArray);
		}
	}

	@Override
	protected String getSummaryString() {
		return acceptor
				+ ":"
				+ ballot
				+ (!accepted.isEmpty() ? ", |accepted|=" + accepted.size()
						+ "[" + this.getMinSlot() + "-" + this.getMaxSlot()
						+ "]" : "");
	}
}