package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.Stringifiable;
import edu.umass.cs.gns.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicableRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;

/*
 * This packet is for any state change to a reconfiguration record. It is
 * currently used only to mark the beginning of a reconfiguration.
 */
public class RCRecordRequest<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> implements
		InterfaceReplicableRequest {

	private static enum Keys {
		REQUEST_TYPE, START_EPOCH
	};

	public static enum RequestTypes {
		RECONFIGURATION_INTENT, RECONFIGURATION_COMPLETE, DELETE_COMPLETE, RECONFIGURATION_MERGE
	};

	private final RequestTypes reqType;
	public final StartEpoch<NodeIDType> startEpoch;
	private boolean coordType = true;

	public RCRecordRequest(NodeIDType initiator,
			StartEpoch<NodeIDType> startEpoch, RequestTypes reqType) {
		super(initiator, ReconfigurationPacket.PacketType.RC_RECORD_REQUEST,
				startEpoch.getServiceName(), startEpoch.getEpochNumber());
		this.reqType = reqType;
		this.startEpoch = startEpoch;
	}

	public RCRecordRequest(NodeIDType initiator, String serviceName,
			int epochNumber, RequestTypes reqType) {
		super(initiator, ReconfigurationPacket.PacketType.RC_RECORD_REQUEST,
				serviceName, epochNumber);
		this.reqType = reqType;
		this.startEpoch = null;
	}

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

	public boolean isDeleteConfirmation() {
		return this.reqType.equals(RequestTypes.DELETE_COMPLETE);
	}

	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
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

	public String getRCRequestTypeCompact() {
		String[] tokens = this.getRCRequestType().toString().split("_");
		return tokens[tokens.length - 1];
	}
}