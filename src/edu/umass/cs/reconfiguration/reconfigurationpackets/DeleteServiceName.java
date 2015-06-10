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
public class DeleteServiceName extends
		BasicReconfigurationPacket<InetSocketAddress> implements
		InterfaceReplicableRequest {

	private static enum Keys {
		FAILED
	};

	/**
	 * Needed for unstringing InetSocketAddresses.
	 */
	public static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	private boolean failed = false;

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 */
	public DeleteServiceName(InetSocketAddress initiator, String name,
			int epochNumber) {
		super(initiator, ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME,
				name, epochNumber);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public DeleteServiceName(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, DeleteServiceName.unstringer); // ignores argument
													// unstringer
		this.setSender(JSONNIOTransport.getSenderAddress(json));
		this.failed = json.optBoolean(Keys.FAILED.toString());
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public DeleteServiceName(JSONObject json) throws JSONException {
		this(json, unstringer);
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		if (this.failed)
			json.put(Keys.FAILED.toString(), this.failed);
		return json;
	}

	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME;
	}

	@Override
	public boolean needsCoordination() {
		return false; 
	}

	@Override
	public void setNeedsCoordination(boolean b) {
		// do nothing
	}

	/**
	 * @return Returns this after setting as failed.
	 */
	public DeleteServiceName setFailed() {
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
