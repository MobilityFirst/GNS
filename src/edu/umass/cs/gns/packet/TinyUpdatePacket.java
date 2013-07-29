package edu.umass.cs.gns.packet;

import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.main.GNS;


public class TinyUpdatePacket  extends BasicPacket{
	private final static String VAL = "v";
	int name;
	int nameServerID;
	int requestID;
	
	public TinyUpdatePacket(int name, int nameserverID, int requestID) {
		this.name = name;
		this.nameServerID = nameserverID;
		this.requestID = requestID;
		type =  Packet.PacketType.TINY_UPDATE;
	}
	
	public TinyUpdatePacket(JSONObject json) throws JSONException {
		String x = json.getString(VAL);
//		System.out.println("TINY UPDATE MSG" + x);
		
		String [] vals = x.split(":");
//		System.out.println("TINY UPDATE MSG" + vals[0]);
		this.name = Integer.parseInt(vals[0]);
		this.nameServerID = Integer.parseInt(vals[1]);
		this.requestID = Integer.parseInt(vals[2]);
		type = Packet.PacketType.TINY_UPDATE;
	}
	
	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		Packet.putPacketType(json, getType());
		json.put(VAL, name + ":" + nameServerID + ":" + requestID);
		return json;
	}

	
	public int getName() {
		return name;
	}



	public int getNameServerId() {
		return nameServerID;
	}
	
	public int getNSRequestID() {
		return requestID;
	}

	
	public static void main(String [] args) {
		TinyUpdatePacket t = new TinyUpdatePacket(12333,12, 1234);
		
		int size = 0;
		try {
			JSONObject json = t.toJSONObject();
			TinyUpdatePacket t1 = new TinyUpdatePacket(json);
//			System.out.println(t1.toJSONObject().toString());
			Random r = new Random();
			int packetID = r.nextInt(100000);
	    json.put("tP", packetID);
	    json.put("tA", 0);
			System.out.println("PACKET IS:" + json.toString());
			size = json.toString().getBytes().length;
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Size = " + size);
		 
	}
}
