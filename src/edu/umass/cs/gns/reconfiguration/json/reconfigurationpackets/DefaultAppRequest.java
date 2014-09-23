package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;

/**
@author V. Arun
 */
public class DefaultAppRequest implements
		InterfaceRequest {
	public enum Keys {STOP, SERVICE_NAME};
	
	private final boolean stop;
	private final String serviceName;
	
	public DefaultAppRequest(String serviceName, boolean stop) {
		this.stop = stop;
		this.serviceName = serviceName;
	}
	public DefaultAppRequest(JSONObject json) throws JSONException {
		this.stop = json.getBoolean(Keys.STOP.toString());
		this.serviceName = json.getString(Keys.SERVICE_NAME.toString());
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
		JSONObject json = new JSONObject();
		try {
			json = this.toJSONObject();
		} catch(JSONException je) {
			je.printStackTrace();
		}
		return json!=null ? json.toString() : null;
	}
	
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.STOP.toString(), this.stop); 
		json.put(Keys.SERVICE_NAME.toString(), this.serviceName); 
		return json;
	}
}
