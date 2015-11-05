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
package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.SummarizableRequest;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

/*
 * @author arun
 * 
 * @param <NodeIDType>
 * 
 * This packet is for any state change to a reconfiguration record. It is
 * currently used only to mark the beginning of a reconfiguration.
 */
@SuppressWarnings("javadoc")
public class RCRecordRequest<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> implements
		ReplicableRequest, ReconfigurableRequest, SummarizableRequest {

	private static enum Keys {
		REQUEST_TYPE, START_EPOCH
	};

	/**
	 * RCRecordRequest sub-types.
	 */
	public static enum RequestTypes {
		/**
		 * Step 1 of reconfiguration. Takes RC record from READY(n) to
		 * WAIT_ACK_STOP(n).
		 */
		RECONFIGURATION_INTENT,

		/**
		 * Step 2 of reconfiguration. We have two steps so that just a single
		 * primary actually does the reconfiguration, and the other secondary
		 * nodes simply record the corresponding state change. Takes RC record
		 * from WAIT_ACK_STOP(n) to READY(n+1). Typically, after
		 * RECONFIGURATION_COMPLETE, the new epoch is considered operational.
		 */
		RECONFIGURATION_COMPLETE,

		/**
		 * Step 3 (optional) of reconfiguration that says that the previous
		 * epoch's final state has been completely dropped. At this point, the
		 * reconfiguration is really complete. Takes RC record from READY(n) to
		 * READY_READY(n) provided all prior reconfigurations have been "clean",
		 * i.e., they involved transitions to READY_READY after a transition to
		 * READY. The reason we track this is that clean records can be quickly
		 * deleted but unclean records have to wait for a MAX_FINAL_STATE_AGE
		 * timeout.
		 */
		RECONFIGURATION_PREV_DROPPED,

		/**
		 * Merges one RC group with another upon RC node deletes.
		 */
		RECONFIGURATION_MERGE,

	};

	private final RequestTypes reqType;
	/**
	 * The start epoch request that started this reconfiguration.
	 */
	public final StartEpoch<NodeIDType> startEpoch;
	private boolean coordType = true;

	/**
	 * @param initiator
	 * @param startEpoch
	 * @param reqType
	 */
	public RCRecordRequest(NodeIDType initiator,
			StartEpoch<NodeIDType> startEpoch, RequestTypes reqType) {
		super(initiator, ReconfigurationPacket.PacketType.RC_RECORD_REQUEST,
				startEpoch.getServiceName(), startEpoch.getEpochNumber());
		this.reqType = reqType;
		this.startEpoch = startEpoch;
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public RCRecordRequest(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		super(json, unstringer);
		this.reqType = RequestTypes.valueOf(json.get(
				Keys.REQUEST_TYPE.toString()).toString());
		this.coordType = false;
		this.startEpoch = json.has(Keys.START_EPOCH.toString()) ? new StartEpoch<NodeIDType>(
				(JSONObject) json.get(Keys.START_EPOCH.toString()), unstringer)
				: null;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(BasicReconfigurationPacket.Keys.IS_COORDINATION.toString(),
				this.coordType);
		json.put(Keys.REQUEST_TYPE.toString(), this.reqType);
		if (this.startEpoch != null)
			json.put(Keys.START_EPOCH.toString(),
					this.startEpoch.toJSONObject());
		return json;
	}

	public boolean isReconfigurationIntent() {
		return this.reqType.equals(RequestTypes.RECONFIGURATION_INTENT);
	}

	public boolean isReconfigurationMerge() {
		return this.reqType.equals(RequestTypes.RECONFIGURATION_MERGE);
	}

	public boolean isReconfigurationComplete() {
		return this.reqType.equals(RequestTypes.RECONFIGURATION_COMPLETE);
	}

	public boolean isReconfigurationPrevDropComplete() {
		return this.reqType.equals(RequestTypes.RECONFIGURATION_PREV_DROPPED);
	}

	public boolean isDeleteIntent() {
		return this.startEpoch.noCurEpochGroup()
				&& this.reqType.equals(RequestTypes.RECONFIGURATION_COMPLETE);
	}

	public boolean isDeleteIntentOrPrevDropComplete() {
		return this.isDeleteIntent()
				|| this.reqType
						.equals(RequestTypes.RECONFIGURATION_PREV_DROPPED);
	}
	public boolean isSplitIntent() {
		return this.isReconfigurationIntent() && this.startEpoch.isSplit();
	}

	@Override
	public IntegerPacketType getRequestType() {
		return this.getType();
	}

	@Override
	public boolean needsCoordination() {
		return this.coordType;
	}

	@Override
	public void setNeedsCoordination(boolean b) {
		this.coordType = b;
	}

	public boolean isNodeConfigChange() {
		return this.getServiceName().equals(
				AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString());
	}

	public RequestTypes getRCRequestType() {
		return this.reqType;
	}

	@Override
	public boolean isStop() {
		return false;
	}

	public static String getRCRequestTypeCompact(RequestTypes rType) {
		return rType.toString().replaceAll("RECONFIGURATION_", "");
	}

	@Override
	public String getSummary() {
		return this.getServiceName()
				+ ":"
				+ this.getEpochNumber()
				+ ":"
				+ this.getRCRequestType()
				+ (this.startEpoch.isSplitOrMerge() ? ":"
						+ this.startEpoch.getPrevGroupName() + ":"
						+ this.startEpoch.getPrevEpochNumber() : "");
	}

	/*
	 * Equal if same operation, i.e., the object type, request type, name,
	 * epoch, prevName, and prevEpoch are all equal.
	 */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof RCRecordRequest))
			return false;
		@SuppressWarnings("unchecked")
		RCRecordRequest<NodeIDType> req2 = (RCRecordRequest<NodeIDType>) o;
		return this.getServiceName().equals(req2.getServiceName())
				&& this.getEpochNumber() == req2.getEpochNumber()
				&& this.getRCRequestType().equals(req2.getRCRequestType())
				&& (this.startEpoch.getPrevGroupName().equals(
						req2.startEpoch.getPrevGroupName()) && this.startEpoch
						.getPrevEpochNumber() == req2.startEpoch
						.getPrevEpochNumber());
	}

	// equals => summary string equality => hashCode equality
	@Override
	public int hashCode() {
		return this.getSummary().hashCode();
	}

	public boolean lessThan(RCRecordRequest<NodeIDType> req2) {
		return this.getServiceName().equals(req2.getServiceName())
				&& this.getEpochNumber() - req2.getEpochNumber() < 0;
	}

}