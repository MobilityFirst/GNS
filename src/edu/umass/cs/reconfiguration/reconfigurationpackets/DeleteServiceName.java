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

	public static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	// private boolean coordType = false;

	private boolean failed = false;

	public DeleteServiceName(InetSocketAddress initiator, String name,
			int epochNumber) {
		super(initiator, ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME,
				name, epochNumber);
	}

	public DeleteServiceName(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, DeleteServiceName.unstringer); // ignores argument
													// unstringer
		this.setSender(JSONNIOTransport.getSenderAddress(json));
		this.failed = json.optBoolean(Keys.FAILED.toString());
	}

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
		return false; // coordType;
	}

	@Override
	public void setNeedsCoordination(boolean b) {
		// coordType = b;
	}

	public DeleteServiceName setFailed() {
		this.failed = true;
		return this;
	}

	public boolean isFailed() {
		return this.failed;
	}
}
