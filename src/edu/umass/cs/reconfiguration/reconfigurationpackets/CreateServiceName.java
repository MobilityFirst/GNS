package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.nio.StringifiableDefault;

/**
 * @author V. Arun
 * 
 *         This class has a field to specify the initial state in addition to
 *         the default fields in ClientReconfigurationPacket.
 */
public class CreateServiceName extends ClientReconfigurationPacket {

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
		super(json, CreateServiceName.unstringer); // ignores unstringer
		assert(this.getSender()!=null);
		//this.setSender(JSONNIOTransport.getSenderAddress(json)); 
		this.initialState = json.optString(Keys.INITIAL_STATE.toString(), null);
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
		if (initialState != null)
			json.put(Keys.INITIAL_STATE.toString(), initialState);
		return json;
	}

	/**
	 * @return Initial state.
	 */
	public String getInitialState() {
		return initialState;
	}
}
