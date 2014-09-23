package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This class puts a given packet type on outgoing packets and sends them via GNSNIOTransportInterface.
 *
 * Created by abhigyan on 3/29/14.
 * 
 * Arun: Edited to make member fields private. FIXME: Need to change
 * the name to StampAndSend or something to reflect that this class is 
 * actually sending the packet.
 */
public class PacketTypeStamper implements InterfaceJSONNIOTransport<Integer> {
  private final InterfaceJSONNIOTransport<Integer> nio;
  private final Packet.PacketType type;

  public PacketTypeStamper(InterfaceJSONNIOTransport<Integer> nio, Packet.PacketType type) {
    this.nio = nio;
    this.type = type;
  }
  
  public Integer getMyID() {return this.nio.getMyID();}

  @Override
  public void stop() {
    nio.stop();
  }

  @Override
  public int sendToID(Integer id, JSONObject jsonData) throws IOException {
    try {
      // Creating a copy of json so that modifications to the original object does not modify the outgoing packet.
      // This was created to fix a bug we were seeing.
      JSONObject jsonCopy = new JSONObject(jsonData.toString());
      Packet.putPacketType(jsonCopy, type);
      return nio.sendToID(id, jsonCopy);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return -1;
  }

  @Override
  public int sendToAddress(InetSocketAddress isa, JSONObject jsonData) throws IOException {
    try {
      // Creating a copy of json so that modifications to the original object does not modify the outgoing packet.
      // This was created to fix a bug we were seeing.
      JSONObject jsonCopy = new JSONObject(jsonData.toString());
      Packet.putPacketType(jsonCopy, type);
      return nio.sendToAddress(isa, jsonCopy);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return -1;
  }

  /**
   * TEST CODE
   */
  public static void main(String[] args) throws JSONException, IOException {
    System.out.println("Test if the send methods mark outgoing packets with correct packet types:");
    final Packet.PacketType type1 = Packet.PacketType.PAXOS_PACKET;
    InterfaceJSONNIOTransport<Integer> jsonnioTransport = new InterfaceJSONNIOTransport<Integer>() {
      @Override
      public int sendToID(Integer id, JSONObject jsonData) throws IOException {
        System.out.println("Sending Packet: " + jsonData);
        try {
          assert Packet.getPacketType(jsonData).equals(type1): "Packet type not matched";

        } catch (JSONException e) {
          e.printStackTrace();
        }
        return 0;
      }

      @Override
      public int sendToAddress(InetSocketAddress isa, JSONObject jsonData) throws IOException {
        System.out.println("Sending Packet: " + jsonData);
        try {
          assert Packet.getPacketType(jsonData).equals(type1): "Packet type not matched";
        } catch (JSONException e) {
          e.printStackTrace();
        }
        return 0;
      }

      @Override
      public Integer getMyID() {
        return 0;
      }

      @Override
      public void stop() {

      }

	@Override
	public void addPacketDemultiplexer(AbstractPacketDemultiplexer pd) {
		throw new RuntimeException("Method not yet implemented");
	}
    };

    PacketTypeStamper packetTypeStamper = new PacketTypeStamper(jsonnioTransport, type1);
    JSONObject sample = new JSONObject();
    sample.put("Apple" , "Banana");
    packetTypeStamper.sendToID(100, sample);
    packetTypeStamper.sendToAddress(null, sample);
    System.out.println("TEST SUCCESS.");
  }

@Override
public void addPacketDemultiplexer(AbstractPacketDemultiplexer pd) {
	throw new RuntimeException("Method not yet implemented");	
}
}
