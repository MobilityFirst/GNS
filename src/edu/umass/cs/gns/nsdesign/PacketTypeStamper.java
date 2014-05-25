package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nio.GNSNIOTransportInterface;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * This class puts a given packet type on outgoing packets and sends them via GNSNIOTransportInterface.
 *
 * Created by abhigyan on 3/29/14.
 * 
 * Arun: Edited to make member fields private. FIXME: Need to change
 * the name to StampAndSend or something to reflect that this class is 
 * actually sending the packet.
 */
public class PacketTypeStamper implements GNSNIOTransportInterface {
  private GNSNIOTransportInterface nio;
  private Packet.PacketType type;

  public PacketTypeStamper(GNSNIOTransportInterface nio, Packet.PacketType type) {
    this.nio = nio;
    this.type = type;
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

}
