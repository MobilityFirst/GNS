package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.packet.Packet;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.InterfaceJSONNIOTransport;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This class puts a given packet type on outgoing packets and sends them via GNSNIOTransportInterface.
 *
 * Created by abhigyan on 3/29/14.
 *
 * Arun: Edited to make member fields private.
 *
 * @param <NodeIDType>
 */
public class PacketTypeStampAndSend<NodeIDType> implements InterfaceJSONNIOTransport<NodeIDType> {

  private final InterfaceJSONNIOTransport<NodeIDType> nio;
  private final Packet.PacketType type;

  public PacketTypeStampAndSend(InterfaceJSONNIOTransport<NodeIDType> nio, Packet.PacketType type) {
    this.nio = nio;
    this.type = type;
  }

  @Override
  public NodeIDType getMyID() {
    return this.nio.getMyID();
  }

  @Override
  public void stop() {
    nio.stop();
  }

  @Override
  public int sendToID(NodeIDType id, JSONObject jsonData) throws IOException {
    try {
      // Creating a copy of json so that modifications to the 
      // original object does not modify the outgoing packet.
      String originalDataString = jsonData.toString();
      int originalSize = originalDataString.length();
      JSONObject jsonCopy = new JSONObject(originalDataString);
      Packet.putPacketType(jsonCopy, type);
      int alteredSize = jsonCopy.toString().length();
      int written = nio.sendToID(id, jsonCopy);
      // A little hair here because the caller expects us to write
      // the amount they sent, not the altered amount
//      if (Config.debuggingEnabled) {
//        GNS.getLogger().fine("############## " + originalSize + " actualWritten=" + written);
//      }
      if (written == alteredSize) {
        return originalSize;
      } else {
        return written;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to stamp due to JSON error:" + e);
      e.printStackTrace();
    }
    return -1;
  }

  @Override
  public int sendToAddress(InetSocketAddress isa, JSONObject jsonData) throws IOException {
    try {
      // Creating a copy of json so that modifications to the original object does not modify the outgoing packet.
      // This was created to fix a bug we were seeing.
      String originalDataString = jsonData.toString();
      int originalSize = originalDataString.length();
      JSONObject jsonCopy = new JSONObject(originalDataString);
      Packet.putPacketType(jsonCopy, type);
      int alteredSize = jsonCopy.toString().length();
      int written = nio.sendToAddress(isa, jsonCopy);
      // a little hair here because the caller expects us to write
      // the amount they sent, not the altered amount
      //GNS.getLogger().info("############## " + originalSize + " actualWritten=" + written);
      if (written == alteredSize) {
        return originalSize;
      } else {
        return written;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return -1;
  }

  /**
   * TEST CODE
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws JSONException, IOException {
    System.out.println("Test if the send methods mark outgoing packets with correct packet types:");
    final Packet.PacketType type1 = Packet.PacketType.PAXOS_PACKET;
    InterfaceJSONNIOTransport<String> jsonnioTransport = new InterfaceJSONNIOTransport<String>() {
      @Override
      public int sendToID(String id, JSONObject jsonData) throws IOException {
        System.out.println("Sending Packet: " + jsonData);
        try {
          assert Packet.getPacketType(jsonData).equals(type1) : "Packet type not matched";

        } catch (JSONException e) {
          e.printStackTrace();
        }
        return 0;
      }

      @Override
      public int sendToAddress(InetSocketAddress isa, JSONObject jsonData) throws IOException {
        System.out.println("Sending Packet: " + jsonData);
        try {
          assert Packet.getPacketType(jsonData).equals(type1) : "Packet type not matched";
        } catch (JSONException e) {
          e.printStackTrace();
        }
        return 0;
      }

      @Override
      public String getMyID() {
        return null;
      }

      @Override
      public void stop() {

      }

      @Override
      public void addPacketDemultiplexer(AbstractPacketDemultiplexer<?> pd) {
    	  throw new RuntimeException("Method not yet implemented");
      }
    };

    PacketTypeStampAndSend packetTypeStamper = new PacketTypeStampAndSend(jsonnioTransport, type1);
    JSONObject sample = new JSONObject();
    sample.put("Apple", "Banana");
    packetTypeStamper.sendToID("100", sample);
    packetTypeStamper.sendToAddress(null, sample);
    System.out.println("TEST SUCCESS.");
  }

  @Override
  public void addPacketDemultiplexer(AbstractPacketDemultiplexer pd) {
    nio.addPacketDemultiplexer(pd);
  }
}
