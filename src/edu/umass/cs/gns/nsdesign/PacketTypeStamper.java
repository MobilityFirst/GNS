package edu.umass.cs.gns.nsdesign;

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
public class PacketTypeStamper implements InterfaceJSONNIOTransport {
  private final InterfaceJSONNIOTransport nio;
  private final Packet.PacketType type;

  public PacketTypeStamper(InterfaceJSONNIOTransport nio, Packet.PacketType type) {
    this.nio = nio;
    this.type = type;
  }
  
  public int getMyID() {return this.nio.getMyID();}

  @Override
  public void stop() {
    nio.stop();
  }


  @Override
  public int sendToID(int id, JSONObject jsonData) throws IOException {
    try {
      Packet.putPacketType(jsonData, type);
      return nio.sendToID(id, jsonData);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return -1;
  }

  @Override
  public int sendToAddress(InetSocketAddress isa, JSONObject jsonData) throws IOException {
    try {
      Packet.putPacketType(jsonData, type);
      return nio.sendToAddress(isa, jsonData);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return -1;
  }

}
