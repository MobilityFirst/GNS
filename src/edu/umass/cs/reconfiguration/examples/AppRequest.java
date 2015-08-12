/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.examples;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 */

@SuppressWarnings("javadoc")
public class AppRequest extends JSONPacket implements InterfaceReconfigurableRequest, InterfaceReplicableRequest {

	/**
	 * JSON key for packet type.
	 */
	public static final String PACKET_TYPE = JSONPacket.PACKET_TYPE;

	/**
	 * Packet type class for NoopApp requests.
	 */
	public enum PacketType implements IntegerPacketType {
		/**
		 * Default app request.
		 */
		DEFAULT_APP_REQUEST (401),
		/**
		 * App request coordination packet type. Curently not used.
		 */
		APP_COORDINATION (402),
		;

		/********************************BEGIN static******************************************/
		private static HashMap<Integer,PacketType> numbers = new HashMap<Integer,PacketType>();
		/* ************* BEGIN static code block to ensure correct initialization **************/
		static {
			for(PacketType type : PacketType.values()) {
				if(!PacketType.numbers.containsKey(type.number)) {
					PacketType.numbers.put(type.number, type);
				} else {
					assert(false) : "Duplicate or inconsistent enum type";
					throw new RuntimeException("Duplicate or inconsistent enum type");
				}
			}
		}
		/* *************** END static code block to ensure correct initialization **************/
		/**
		 * @param type
		 * @return PacketType from int type.
		 */
		public static PacketType getPacketType(int type) {
			return PacketType.numbers.get(type);
		}
		/**********************************END static******************************************/

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
	 *  These app keys by design need not be the same as those in BasicReconfigurationPacket
	 */
	public enum Keys {SERVICE_NAME, EPOCH, REQUEST_ID, REQUEST_VALUE, STOP, IS_COORDINATION, CLIENT_IP, CLIENT_PORT};

	private final String name;
	private final int epoch;
	private final int id;
	private final boolean stop;
	private final String value;

	private InetAddress clientIP=null;
	private int clientPort=-1;
	
	private boolean coordType = true;

	public AppRequest(String name, int epoch, int id, String value, IntegerPacketType type, boolean stop) {
		super(type);
		this.name = name;
		this.epoch = epoch;
		this.id = id;
		this.stop = stop;
		this.value = value;
	}
	public AppRequest(String name, int id, String value, IntegerPacketType type, boolean stop) {
		super(type);
		this.name = name;
		this.epoch = 0;
		this.id = id;
		this.stop = stop;
		this.value = value;
	}
	public AppRequest(String name, String value, IntegerPacketType type, boolean stop) {
		super(type);
		this.name = name;
		this.epoch = 0;
		this.id = (int)(Math.random()*Integer.MAX_VALUE);
		this.stop = stop;
		this.value = value;
		
	}

	public AppRequest(JSONObject json) throws JSONException {
		super(json);
		this.name = json.getString(Keys.SERVICE_NAME.toString());
		this.epoch = json.getInt(Keys.EPOCH.toString());
		this.id = json.getInt(Keys.REQUEST_ID.toString());
		this.stop = json.getBoolean(Keys.STOP.toString());
		this.value = json.getString(Keys.REQUEST_VALUE.toString());
		this.coordType = (json.has(Keys.IS_COORDINATION.toString()) ? json.getBoolean(Keys.IS_COORDINATION.toString()) : false);
		/* We read from json using JSONNIOTransport convention, but there is no 
		 * corresponding operation in toJSONObjectImpl().
		 */
		try {
			InetSocketAddress isa = JSONNIOTransport.getSenderAddress(json);
			this.clientIP = json.has(Keys.CLIENT_IP.toString()) ? InetAddress
					.getByName(json.getString(Keys.CLIENT_IP.toString()).replaceAll("[^0-9.]*", ""))
					: (isa != null ? isa.getAddress() : this.clientIP);
			this.clientPort = json.has(Keys.CLIENT_PORT.toString()) ? json
					.getInt(Keys.CLIENT_PORT.toString()) : (isa != null ? isa
					.getPort() : this.clientPort);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return PacketType.getPacketType(this.type);
	}

	@Override
	public String getServiceName() {
		return this.name;
	}
	
	public String getValue() {
		return this.value;
	}
	public int getRequestID() {
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
		if(this.clientIP!=null) json.put(Keys.CLIENT_IP.toString(), this.clientIP.toString());
		if(this.clientPort > 0) json.put(Keys.CLIENT_PORT.toString(), this.clientPort);
		return json;
	}
	
	public InetAddress getSenderAddress() {
		return this.clientIP;
	}
	public int getSenderPort() {
		return this.clientPort;
	}

	public static void main(String[] args) {
		AppRequest request = new AppRequest("name1", 0, 0, "request1", AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
		System.out.println(request);
		try {
			AppRequest request2 = (new AppRequest(request.toJSONObject()));
			assert(request.toString().equals(request2.toString()));
		} catch(JSONException je) {
			je.printStackTrace();
		}
		System.out.println("SUCCESS");
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
}
