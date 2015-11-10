/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.examples;

import java.net.InetSocketAddress;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

/**
 * @author V. Arun
 * 
 *         A class like this is necessary if the app wants to use its own packet
 *         types to send requests and responses (as opposed to just using
 *         {@link RequestPacket}. The main requirements of application requests
 *         is that they must implement at least InterfaceRequest, and must
 *         implement InterfaceReplicable, InterfaceReconfigurable, and
 *         InterfceClientRequest in order to respectively support replication,
 *         reconfiguration, and the ability to delegate client response
 *         messaging to gigapaxos.
 */

public class AppRequest extends JSONPacket implements
		ReconfigurableRequest, ReplicableRequest,
		ClientRequest {

	/**
	 * Packet type class for NoopApp requests.
	 */
	public enum PacketType implements IntegerPacketType {
		/**
		 * Default app request.
		 */
		DEFAULT_APP_REQUEST(401),
		/**
		 * Another app request type that is currently not used for anything.
		 */
		ANOTHER_APP_REQUEST(402), ;

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
	public enum Keys {
		/**
		 * 
		 */
		NAME,

		/**
		 * 
		 */
		EPOCH,

		/**
		 * 
		 */
		QID,

		/**
		 * 
		 */
		QVAL,

		/**
		 * 
		 */
		STOP,

		/**
		 * 
		 */
		COORD,

		/**
		 * 
		 */
		CSA,

		/**
		 * 
		 */
		RVAL
	};

	/**
	 */
	public enum ResponseCodes {
		/**
		 * 
		 */
		ACK
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
	 * @param value
	 * @param type
	 * @param stop
	 */
	public AppRequest(String name, String value, IntegerPacketType type,
			boolean stop) {
		this(name, 0, (int) (Math.random() * Integer.MAX_VALUE), value, type,
				stop);
	}

	/**
	 * @param name
	 * @param epoch
	 * @param id
	 * @param value
	 * @param type
	 * @param stop
	 */
	public AppRequest(String name, int epoch, long id, String value,
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
	public AppRequest(String name, long id, String value,
			IntegerPacketType type, boolean stop) {
		this(name, 0, id, value, type, stop);
	}


	/**
	 * @param value
	 * @param req
	 */
	public AppRequest(String value, AppRequest req) {
		this(req.name, req.epoch, req.id, value, PacketType
				.getPacketType(req.type), req.stop);
		this.clientAddress = req.clientAddress;
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public AppRequest(JSONObject json) throws JSONException {
		super(json);
		this.name = json.getString(Keys.NAME.toString());
		this.epoch = json.getInt(Keys.EPOCH.toString());
		this.id = json.getInt(Keys.QID.toString());
		this.stop = json.getBoolean(Keys.STOP.toString());
		this.value = json.getString(Keys.QVAL.toString());
		this.coordType = (json.has(Keys.COORD.toString()) ? json
				.getBoolean(Keys.COORD.toString()) : false);
		/*
		 * We read from json using JSONNIOTransport convention, but there is no
		 * corresponding operation in toJSONObjectImpl().
		 */
		InetSocketAddress isa = MessageNIOTransport.getSenderAddress(json);
		this.clientAddress = json.has(Keys.CSA.toString()) ? Util
				.getInetSocketAddressFromString(json.getString(Keys.CSA
						.toString())) : isa;

		this.response = json.has(Keys.RVAL.toString()) ? json
				.getString(Keys.RVAL.toString()) : null;

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
	 * @return Request ID.
	 */
	public long getRequestID() {
		return this.id;
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.NAME.toString(), this.name);
		json.put(Keys.EPOCH.toString(), this.epoch);
		json.put(Keys.QID.toString(), this.id);
		json.put(Keys.STOP.toString(), this.stop);
		json.put(Keys.QVAL.toString(), this.value);
		json.put(Keys.COORD.toString(), this.coordType);
		if (this.clientAddress != null)
			json.put(Keys.CSA.toString(), this.clientAddress.toString());
		json.putOpt(Keys.RVAL.toString(), this.response);
		return json;
	}

	/**
	 * @return Sending client's address.
	 */
	public InetSocketAddress getSenderAddress() {
		return this.clientAddress;
	}

	@Override
	public int getEpochNumber() {
		return this.epoch;
	}

	@Override
	public boolean isStop() {
		return this.stop;
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
		return new AppRequest(ResponseCodes.ACK.toString(), this);
	}

	/**
	 * @param response
	 */
	public void setResponse(String response) {
		this.response = response;
	}

	/**
	 * It is a bad idea to use an oblivious stop request. An oblivious stop
	 * request is not actually passed to the application's handleRequest(.)
	 * method, so it won't know that it has been stopped and will not be in a
	 * position to do any garbage collection if needed.
	 * 
	 * @param name
	 * @param epoch
	 * @return Default oblivious stop request.
	 */
	public static AppRequest getObliviousPaxosStopRequest(String name, int epoch) {
		return new AppRequest(name, epoch,
				(long) (Math.random() * Long.MAX_VALUE),
				Request.NO_OP,
				AppRequest.PacketType.DEFAULT_APP_REQUEST, true);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AppRequest request = new AppRequest("name1", 0, 0, "request1",
				AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
		System.out.println(request);
		try {
			AppRequest request2 = (new AppRequest(request.toJSONObject()));
			assert (request.toString().equals(request2.toString()));
		} catch (JSONException je) {
			je.printStackTrace();
		}
		System.out.println("SUCCESS");
	}
}
