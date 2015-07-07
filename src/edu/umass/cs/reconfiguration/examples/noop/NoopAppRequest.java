package edu.umass.cs.reconfiguration.examples.noop;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.reconfiguration.InterfaceReplicableRequest;
import edu.umass.cs.reconfiguration.examples.AppRequest;

/**
@author V. Arun
 */
public class NoopAppRequest extends AppRequest implements InterfaceReconfigurableRequest, InterfaceReplicableRequest {

	private static enum Keys {ENTRY_REPLICA};
	
	// the first replica that received the request
	private String entryReplica = null; 
	
	/**
	 *  Can define IntegerPacketType types here other than
	 * those defined in AppRequest. The reconfiguration 
	 * package is agnostic to the contents of this class
	 * other than that it supports InterfaceRequest. The
	 * super class AppRequest is there only for convenience.
	 * 
	 * @param name 
	 * @param epoch 
	 * @param id 
	 * @param value 
	 * @param type 
	 * @param stop 
	 */

	public NoopAppRequest(String name, int epoch, int id, String value,
			IntegerPacketType type, boolean stop) {
		super(name, epoch, id, value, type, stop);
	}
	/**
	 * @param name
	 * @param id
	 * @param value
	 * @param type
	 * @param stop
	 */
	public NoopAppRequest(String name, int id, String value,
			IntegerPacketType type, boolean stop) {
		super(name, 0, id, value, type, stop);
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public NoopAppRequest(JSONObject json) throws JSONException {
		super(json);
		this.entryReplica = (json.has(Keys.ENTRY_REPLICA.toString()) ? json
				.getString(Keys.ENTRY_REPLICA.toString()) : this.entryReplica);
	}
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		if(this.entryReplica!=null) json.put(Keys.ENTRY_REPLICA.toString(), this.entryReplica);
		return json;
	}
	
	/**
	 * @param id
	 */
	public void setEntryReplica(String id) {
		this.entryReplica = id;
	}
	/**
	 * @return ID of entry replica that received this request.
	 */
	public String getEntryReplica() {
		return this.entryReplica;
	}

	public static void main(String[] args) {
	}
}
