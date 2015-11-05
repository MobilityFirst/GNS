package edu.umass.cs.gigapaxos.examples;

import java.net.InetSocketAddress;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 *         A class like this is needed only if the app wants to use request
 *         types other than RequestPacket, which is generally useful only if the
 *         app wants reconfigurability. For using just gigapaxos (without
 *         reconfiguration), implementing a class like this should be
 *         unnecessary. Applications can simply encapsulate requests as
 *         {@link RequestPacket}; doing so will also be more efficient than
 *         using app-specific request types.
 */
public class PaxosAppRequest extends JSONPacket implements
		ReplicableRequest, ClientRequest {

	/**
	 * Packet type class for NoopApp requests.
	 */
	public enum PacketType implements IntegerPacketType {
		/**
		 * Default app request.
		 */
		DEFAULT_APP_REQUEST(401),
		/**
		 * App request coordination packet type. Curently not used.
		 */
		APP_REQUEST_NO_COORDINATION(402), ;

		/******************************** BEGIN static ******************************************/
		private static HashMap<Integer, PacketType> numbers = new HashMap<Integer, PacketType>();
		/*
		 * ************* BEGIN static code block to ensure correct
		 * initialization *************
		 */
		static {
			for (PacketType type : PacketType.values()) {
				if (!PacketType.numbers.containsKey(type.number)) {
					PacketType.numbers.put(type.number, type);
				} else {
					assert (false) : "Duplicate or inconsistent enum type";
					throw new RuntimeException(
							"Duplicate or inconsistent enum type");
				}
			}
		}

		/*
		 * *************** END static code block to ensure correct
		 * initialization *************
		 */
		/**
		 * @param type
		 * @return PacketType from int type.
		 */
		public static PacketType getPacketType(int type) {
			return PacketType.numbers.get(type);
		}

		/********************************** END static ******************************************/

		private final int number;

		PacketType(int t) {
			this.number = t;
		}

		@Override
		public int getInt() {
			return this.number;
		}
	}

	/**
	 * These app keys by design need not be the same as those in
	 * BasicReconfigurationPacket
	 */
	@SuppressWarnings("javadoc")
	public enum Keys {
		SERVICE_NAME, EPOCH, REQUEST_ID, REQUEST_VALUE, STOP, IS_COORDINATION, CLIENT_ADDRESS, ACK, RESPONSE_VALUE
	};

	private final String name;
	private final int epoch;
	private final long id;
	private final boolean stop;
	private final String value;

	private String response = null;

	private InetSocketAddress clientAddress = null;

	private boolean coordType = true;

	/**
	 * @param name
	 * @param epoch
	 * @param id
	 * @param value
	 * @param type
	 * @param stop
	 */
	public PaxosAppRequest(String name, int epoch, long id, String value,
			IntegerPacketType type, boolean stop) {
		super(type);
		this.name = name;
		this.epoch = epoch;
		this.id = id;
		this.stop = stop;
		this.value = value;
	}

	/**
	 * @param name
	 * @param id
	 * @param value
	 * @param type
	 * @param stop
	 */
	public PaxosAppRequest(String name, long id, String value,
			IntegerPacketType type, boolean stop) {
		this(name, 0, id, value, type, stop);
	}

	/**
	 * @param name
	 * @param value
	 * @param type
	 * @param stop
	 */
	public PaxosAppRequest(String name, String value, IntegerPacketType type,
			boolean stop) {
		this(name, 0, (long) (Math.random() * Long.MAX_VALUE), value, type,
				stop);
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public PaxosAppRequest(JSONObject json) throws JSONException {
		super(json);
		this.name = json.getString(Keys.SERVICE_NAME.toString());
		this.epoch = json.getInt(Keys.EPOCH.toString());
		this.id = json.getLong(Keys.REQUEST_ID.toString());
		this.stop = json.getBoolean(Keys.STOP.toString());
		this.value = json.getString(Keys.REQUEST_VALUE.toString());
		this.coordType = (json.has(Keys.IS_COORDINATION.toString()) ? json
				.getBoolean(Keys.IS_COORDINATION.toString()) : false);

		this.clientAddress = json.has(Keys.CLIENT_ADDRESS.toString()) ? Util
				.getInetSocketAddressFromString(json
						.getString(Keys.CLIENT_ADDRESS.toString())) : null;

		this.response = json.has(Keys.RESPONSE_VALUE.toString()) ? json
				.getString(Keys.REQUEST_VALUE.toString()) : null;
	}

	@Override
	public IntegerPacketType getRequestType() {
		return PacketType.getPacketType(this.type);
	}

	@Override
	public String getServiceName() {
		return this.name;
	}

	/**
	 * @return Request value.
	 */
	public String getValue() {
		return this.value;
	}

	/**
	 * @return Unique request ID.
	 */
	public long getRequestID() {
		return this.id;
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.SERVICE_NAME.toString(), this.name);
		json.put(Keys.EPOCH.toString(), this.epoch);
		json.put(Keys.REQUEST_ID.toString(), this.id);
		json.put(Keys.STOP.toString(), this.stop);
		json.put(Keys.REQUEST_VALUE.toString(), this.value);
		json.put(Keys.IS_COORDINATION.toString(), this.coordType);
		if (this.clientAddress != null)
			json.put(Keys.CLIENT_ADDRESS.toString(),
					this.clientAddress.toString());
		json.putOpt(Keys.RESPONSE_VALUE.toString(), this.response);
		return json;
	}

	/**
	 * @return InetSocketAddress of sender. Same as {@link #getClientAddress()}.
	 */
	public InetSocketAddress getSenderAddress() {
		return this.clientAddress;
	}

	@Override
	public boolean needsCoordination() {
		return this.coordType;
	}

	@Override
	public void setNeedsCoordination(boolean b) {
		this.coordType = b;
	}

	@Override
	public InetSocketAddress getClientAddress() {
		return this.clientAddress;
	}

	@Override
	public ClientRequest getResponse() {
		return new PaxosAppRequest(this.name, this.epoch, this.id,
				Keys.ACK.toString(), PacketType.getPacketType(type), this.stop);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PaxosAppRequest request = new PaxosAppRequest("name1", 0, 0,
				"request1", PaxosAppRequest.PacketType.DEFAULT_APP_REQUEST,
				false);
		System.out.println(request);
		try {
			PaxosAppRequest request2 = (new PaxosAppRequest(
					request.toJSONObject()));
			assert (request.toString().equals(request2.toString()));
			System.out.println("SUCCESS");
		} catch (JSONException je) {
			je.printStackTrace();
		}
	}

	/**
	 * @param response
	 */
	public void setResponse(String response) {
		this.response = response;
	}
}
