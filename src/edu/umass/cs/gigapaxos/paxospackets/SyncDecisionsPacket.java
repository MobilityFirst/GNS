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

import edu.umass.cs.utils.Util;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * @author arun
 *
 *         A sync reply packet contains commits missing at the sending node
 *         (nodeID). The receiver is expected to send to the sender the commits
 *         it is reporting as missing in this sync reply.
 */
@SuppressWarnings("javadoc")
public final class SyncDecisionsPacket extends PaxosPacket {

	/**
	 * Node sending the sync decisions request.
	 */
	public final int nodeID;
	/**
	 * Maximum slot up to which decisions have been received by the sending
	 * node.
	 */
	public final int maxDecisionSlot;
	/**
	 * Missing decision slot numbers beyond {@link #maxDecisionSlot
	 * maxDecisionSlot}.
	 */
	public final ArrayList<Integer> missingSlotNumbers;
	/**
	 * Whether we are missing too much, thereby prompting a checkpoint transfer
	 * instead of sending a large number of decisions. This flag is determined
	 * by default based on the number of missing decisions and the
	 * inter-checkpoint interval, but can also be explicitly specified by the
	 * sender.
	 */
	public final boolean missingTooMuch;

	public SyncDecisionsPacket(int nodeID, int maxDecisionSlot,
			ArrayList<Integer> missingSlotNumbers, boolean flag) {
		super((PaxosPacket) null);
		this.missingTooMuch = flag;
		this.packetType = (missingTooMuch ? PaxosPacketType.SYNC_DECISIONS
				: PaxosPacketType.SYNC_DECISIONS); // missingTooMuch =>
													// checkpoint transfer
		this.nodeID = nodeID;
		this.maxDecisionSlot = maxDecisionSlot;
		this.missingSlotNumbers = missingSlotNumbers;
	}

	public SyncDecisionsPacket(JSONObject json) throws JSONException {
		super(json);
		this.nodeID = json
				.getInt(PaxosPacket.NodeIDKeys.SNDR.toString());
		this.maxDecisionSlot = json
				.getInt(PaxosPacket.Keys.MAX_S.toString());
		if (json.has(PaxosPacket.Keys.MISSS.toString()))
			missingSlotNumbers = Util.JSONArrayToArrayListInteger(json
					.getJSONArray(PaxosPacket.Keys.MISSS.toString()));
		else
			missingSlotNumbers = null;
		this.missingTooMuch = json
				.getBoolean(PaxosPacket.Keys.CP_TX.toString());
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.SYNC_DECISIONS || PaxosPacket
				.getPaxosPacketType(json) == PaxosPacketType.CHECKPOINT_REQUEST); 
		this.packetType = PaxosPacketType.SYNC_DECISIONS;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.SNDR.toString(), nodeID);
		json.put(PaxosPacket.Keys.MAX_S.toString(), maxDecisionSlot);
		json.put(PaxosPacket.Keys.CP_TX.toString(),
				missingTooMuch);
		if (missingSlotNumbers != null && missingSlotNumbers.size() > 0)
			json.put(PaxosPacket.Keys.MISSS.toString(), new JSONArray(
					missingSlotNumbers));
		return json;
	}

	@Override
	protected String getSummaryString() {
		return nodeID
				+ "["
				+ this.missingSlotNumbers.get(0)
				+ ", "
				+ this.missingSlotNumbers
						.get(this.missingSlotNumbers.size() - 1) + "]";
	}
}
