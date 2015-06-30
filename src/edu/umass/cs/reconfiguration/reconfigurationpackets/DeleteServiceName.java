package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.nio.StringifiableDefault;

/**
 * @author V. Arun
 * 
 *         This class has no special fields in addition to a generic
 *         ClientReconfigurationPacket. It just needs information about
 *         isFailed(), the correct set of reconfigurators, and the response
 *         message, all of which are in ClientReconfigurationPacket anyway.
 */
public class DeleteServiceName extends ClientReconfigurationPacket {

	/**
	 * Needed for unstringing InetSocketAddresses.
	 */
	protected static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

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
		super(json, DeleteServiceName.unstringer); // ignores unstringer
		this.setSender(JSONNIOTransport.getSenderAddress(json));
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
		return json;
	}
}
