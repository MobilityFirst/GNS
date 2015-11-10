/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client;

import org.json.JSONObject;
import edu.umass.cs.gnsclient.client.tcp.packet.Packet;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import org.json.JSONException;

/**
 * 
 * @author westy
 */
public class PacketDemultiplexer extends AbstractJSONPacketDemultiplexer{
  
  BasicUniversalTcpClient client;

  public PacketDemultiplexer(BasicUniversalTcpClient client) {
    this.client = client;
    this.register(Packet.PacketType.COMMAND);
    this.register(Packet.PacketType.COMMAND_RETURN_VALUE);
  }
  
@Override
  public boolean handleMessage(JSONObject jsonObject) {
    long receivedTime = System.currentTimeMillis();
    try {
      switch (Packet.getPacketType(jsonObject)) {
        case COMMAND:
          break;
        case COMMAND_RETURN_VALUE:
          client.handleCommandValueReturnPacket(jsonObject, receivedTime);
          break;
        default:
          return false;
      }
    } catch (JSONException e) {
      return false;
    }
    return true;
  }
  
}
