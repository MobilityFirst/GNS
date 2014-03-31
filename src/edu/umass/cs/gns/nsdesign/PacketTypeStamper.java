package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nio.GNSNIOTransportInterface;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Set;

/**
 * This class puts a given packet type on outgoing packets and sends them via GNSNIOTransportInterface.
 *
 * Created by abhigyan on 3/29/14.
 */
public class PacketTypeStamper implements GNSNIOTransportInterface {
  GNSNIOTransportInterface nio;
  Packet.PacketType type;

  public PacketTypeStamper(GNSNIOTransportInterface nio, Packet.PacketType type) {
    this.nio = nio;
    this.type = type;
  }


  @Override
  public int sendToIDs(Set<Integer> destIDs, JSONObject jsonData) throws IOException {
    try {
      Packet.putPacketType(jsonData, type);
      return nio.sendToIDs(destIDs, jsonData);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return -1;
  }

  @Override
  public int sendToIDs(short[] destIDs, JSONObject jsonData) throws IOException {
    try {
      Packet.putPacketType(jsonData, type);
      return nio.sendToIDs(destIDs, jsonData);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return -1;
  }

  @Override
  public int sendToIDs(short[] destIDs, JSONObject jsonData, int excludeID) throws IOException {
    try {
      Packet.putPacketType(jsonData, type);
      return nio.sendToIDs(destIDs, jsonData, excludeID);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return -1;
  }

  @Override
  public int sendToIDs(Set<Integer> destIDs, JSONObject jsonData, int excludeID) throws IOException {
    try {
      Packet.putPacketType(jsonData, type);
      return nio.sendToIDs(destIDs, jsonData, excludeID);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return -1;
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
  public void run() {
    throw  new UnsupportedOperationException();
  }
}
