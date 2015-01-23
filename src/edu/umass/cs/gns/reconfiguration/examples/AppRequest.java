package edu.umass.cs.gns.reconfiguration.examples;

import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.JSONPacket;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicableRequest;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;

/**
@author V. Arun
 */

public class AppRequest extends JSONPacket implements InterfaceReconfigurableRequest, InterfaceReplicableRequest {

	public static final String PACKET_TYPE = JSONPacket.PACKET_TYPE;

	public enum PacketType implements IntegerPacketType {
		DEFAULT_APP_REQUEST (401),
		APP_COORDINATION (402),
		;

		/********************************BEGIN static******************************************/
		private static HashMap<Integer,PacketType> numbers = new HashMap<Integer,PacketType>();
		/************** BEGIN static code block to ensure correct initialization **************/
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
		/**************** END static code block to ensure correct initialization **************/
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

	// These app keys by design need not be the same as those in BasicReconfigurationPacket
	public enum Keys {SERVICE_NAME, EPOCH, REQUEST_ID, REQUEST_VALUE, IS_STOP, IS_COORDINATION};

	private final String name;
	private final int epoch;
	private final int id;
	private final boolean stop;
	private final String value;
	
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

	public AppRequest(JSONObject json) throws JSONException {
		super(json);
		this.name = json.getString(Keys.SERVICE_NAME.toString());
		this.epoch = json.getInt(Keys.EPOCH.toString());
		this.id = json.getInt(Keys.REQUEST_ID.toString());
		this.stop = json.getBoolean(Keys.IS_STOP.toString());
		this.value = json.getString(Keys.REQUEST_VALUE.toString());
		this.coordType = (json.has(Keys.IS_COORDINATION.toString()) ? json.getBoolean(Keys.IS_COORDINATION.toString()) : false);
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
	
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.SERVICE_NAME.toString(), this.name);
		json.put(Keys.EPOCH.toString(), this.epoch);
		json.put(Keys.REQUEST_ID.toString(), this.id);
		json.put(Keys.IS_STOP.toString(), this.stop);
		json.put(Keys.REQUEST_VALUE.toString(), this.value);
		json.put(Keys.IS_COORDINATION.toString(), this.coordType);
		return json;
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
