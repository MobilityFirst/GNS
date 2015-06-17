package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.nio.StringifiableDefault;
import edu.umass.cs.reconfiguration.InterfaceReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 */
public class CreateServiceName extends
		BasicReconfigurationPacket<InetSocketAddress> implements
		InterfaceReplicableRequest {

	private static enum Keys {
		INITIAL_STATE
	};

	/**
	 * Unstringer needed to handle client InetSocketAddresses as opposed to
	 * NodeIDType.
	 */
	public static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	/**
	 * Initial state.
	 */
	public final String initialState;
	private boolean failed = false;

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param state
	 */
	public CreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state) {
		super(initiator, ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME,
				name, epochNumber);
		this.initialState = state;
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public CreateServiceName(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, CreateServiceName.unstringer); // ignores argument
													// unstringer
		this.setSender(JSONNIOTransport.getSenderAddress(json));
		this.initialState = json.optString(Keys.INITIAL_STATE.toString(), null);
		this.failed = json.optBoolean(DeleteServiceName.Keys.FAILED.toString());
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public CreateServiceName(JSONObject json) throws JSONException {
		this(json, unstringer);
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		if (initialState != null) {
			json.put(Keys.INITIAL_STATE.toString(), initialState);
		}
		if(failed)
			json.put(DeleteServiceName.Keys.FAILED.toString(), this.failed);
		return json;
	}

	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME;
	}

	@Override
	public boolean needsCoordination() {
		return false; // always false
	}

	@Override
	public void setNeedsCoordination(boolean b) {
		// do nothing
	}

	/**
	 * @return Initial state.
	 */
	public String getInitialState() {
		return initialState;
	}

	/**
	 * @return Returns this after setting as failed.
	 */
	public CreateServiceName setFailed() {
		this.failed = true;
		return this;
	}

	/**
	 * @return Whether this request failed.
	 */
	public boolean isFailed() {
		return this.failed;
	}
	

}
