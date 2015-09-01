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

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;

/**
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public class AcceptReplyPacket extends PaxosPacket {
	/**
	 * Sender node ID.
	 */
	public final int acceptor;
	/**
	 * Ballot in the ACCEPT request being replied to.
	 */
	public final Ballot ballot;
	/**
	 * Slot number in the ACCEPT request being replied to.
	 */
	public final int slotNumber;
	/**
	 * Maximum slot up to which this node has checkpointed state, a value that
	 * is used for garbage collection of logs.
	 */
	public final int maxCheckpointedSlot;

	private long requestID = 0; // used only for debugging

	public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber,
			int maxCheckpointedSlot, AcceptReplyPacket ar) {
		super(ar);
		this.packetType = PaxosPacketType.ACCEPT_REPLY;
		this.acceptor = nodeID;
		this.ballot = ballot;
		this.slotNumber = slotNumber;
		this.maxCheckpointedSlot = maxCheckpointedSlot;
		
	}
	public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber,
			int maxCheckpointedSlot) {
		this(nodeID, ballot, slotNumber, maxCheckpointedSlot, null);
	}

	// used only for instrumentation
	public AcceptReplyPacket(int nodeID, Ballot ballot, int slotNumber,
			int maxCheckpointedSlot, long requestID) {
		this(nodeID, ballot, slotNumber, maxCheckpointedSlot);
		this.setRequestID(requestID);
	}

	public AcceptReplyPacket(JSONObject jsonObject) throws JSONException {
		super(jsonObject);
		this.packetType = PaxosPacketType.ACCEPT_REPLY;
		this.acceptor = jsonObject.getInt(PaxosPacket.NodeIDKeys.SNDR
				.toString());
		this.ballot = new Ballot(
				jsonObject.getString(PaxosPacket.NodeIDKeys.B.toString()));
		this.slotNumber = jsonObject.getInt(PaxosPacket.Keys.S.toString());
		this.maxCheckpointedSlot = jsonObject
				.getInt(PaxosPacket.Keys.CP_S.toString());
		this.requestID = jsonObject.getInt(RequestPacket.Keys.QID
				.toString());
	}

	public void setRequestID(long id) {
		this.requestID = id;
	}

	public long getRequestID() {
		return this.requestID;
	}

	@Override
	protected String getSummaryString() {
		return acceptor + ", " + ballot + ", " + slotNumber + "("
				+ maxCheckpointedSlot + ")";
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.SNDR.toString(), acceptor);
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.S.toString(), slotNumber);
		json.put(PaxosPacket.Keys.CP_S.toString(),
				this.maxCheckpointedSlot);
		json.put(RequestPacket.Keys.QID.toString(), this.requestID);
		return json;
	}

}
