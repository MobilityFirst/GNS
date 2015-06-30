package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.reconfiguration.InterfaceReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 * 
 * This request is unlike usual requests that can be converted to a String 
 * and back. This request is not meant to be serialized and sent over 
 * the network, but only passed internally within a single node.
 */
public class DefaultAppRequest implements
		InterfaceReplicableRequest, InterfaceReconfigurableRequest {
	protected enum Keys {STOP, SERVICE_NAME, EPOCH_NUMBER, REQUEST_VALUE};
	
	private final boolean stop;
	private final String serviceName;
	private final int epochNumber;
	private final String requestValue;
	private boolean isCoord = true;
	
	/**
	 * @param serviceName
	 * @param epochNumber
	 * @param stop
	 */
	public DefaultAppRequest(String serviceName, int epochNumber, boolean stop) {
		this.stop = stop;
		this.serviceName = serviceName;
		this.epochNumber = epochNumber;
		this.requestValue = InterfaceRequest.NO_OP;
	}
	/**
	 * @param json
	 * @throws JSONException
	 */
	public DefaultAppRequest(JSONObject json) throws JSONException {
		this.stop = json.getBoolean(Keys.STOP.toString());
		this.serviceName = json.getString(Keys.SERVICE_NAME.toString());
		this.epochNumber = json.getInt(Keys.EPOCH_NUMBER.toString());
		this.requestValue = json.getString(Keys.REQUEST_VALUE.toString());
	}

	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return null; // FIXME: Not sure what to do here
	}

	@Override
	public String getServiceName() {
		return this.serviceName;
	}
	
	@Override
	public String toString() {
		try {
			return this.toJSONObject().toString();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * @return JSONObject corresponding to this request.
	 * @throws JSONException
	 */
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.STOP.toString(), this.stop); 
		json.put(Keys.SERVICE_NAME.toString(), this.serviceName); 
		json.put(Keys.EPOCH_NUMBER.toString(), this.epochNumber);
		json.put(Keys.REQUEST_VALUE.toString(), this.requestValue);
		return json;
	}
	
	@Override
	public int getEpochNumber() {
		return this.epochNumber;
	}
	@Override
	public boolean isStop() {
		return this.stop;
	}
	@Override
	public boolean needsCoordination() {
		return isCoord;
	}
	@Override
	public void setNeedsCoordination(boolean b) {
		this.isCoord = b;
	}
}
