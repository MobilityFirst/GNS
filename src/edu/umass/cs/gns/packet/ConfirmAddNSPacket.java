package edu.umass.cs.gns.packet;

import org.json.JSONException;
import org.json.JSONObject;

public class ConfirmAddNSPacket extends BasicPacket {

  private final static String PACKET_ID = "id";
  private final static String NAMESERVER_ID = "ns";
  
  private int packetID;
  private int nameServerID;
  
  public ConfirmAddNSPacket(int packetID, int nameServerID) {
    this.type = Packet.PacketType.CONFIRM_ADD_NS;
    this.packetID = packetID;
    this.nameServerID = nameServerID;
  }

  public ConfirmAddNSPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.packetID = json.getInt(PACKET_ID);
    this.nameServerID = json.getInt(NAMESERVER_ID);
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(PACKET_ID, packetID);
    json.put(NAMESERVER_ID, nameServerID);
    return json;
  }

  public int getPacketID() {
    return packetID;
  }

  public int getNameServerID() {
    return nameServerID;
  }

//
//  public static void main(String[] args) {
//  	ConfirmAddNSPacket confirmUpdateNSPacket = new ConfirmAddNSPacket(21, 4);
//
//  	try {
//			System.out.println(confirmUpdateNSPacket.toJSONObject());
//			JSONObject json  =  confirmUpdateNSPacket.toJSONObject();
//			ConfirmAddNSPacket packet2 = new ConfirmAddNSPacket(json);
//			System.out.println(packet2.toJSONObject());
//			int size = json.toString().getBytes().length;
//
//		System.out.println("Confirm update packet size = " + size);
//		} catch (JSONException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//  }
}
