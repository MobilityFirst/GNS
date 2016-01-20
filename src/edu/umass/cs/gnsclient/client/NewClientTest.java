/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gigapaxos.interfaces.Request;
import static edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME;
import static edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME;
import java.io.IOException;

/**
 *
 * @author westy
 */
public class NewClientTest {

  private static void sendDeleteCreate(NewBasicUniversalTcpClient client) throws IOException {

//    client.sendReconfigurationRequest(DELETE_SERVICE_NAME, "test@gns.name", "{}",
//            (Request response) -> {
//              GNSClient.getLogger().info("Received response: " + response);
//            });
    client.sendReconfigurationRequest(CREATE_SERVICE_NAME, "test@gns.name", "{}",
            (Request response) -> {
              GNSClient.getLogger().info("Received response: " + response);
            });
  }

  public static void main(String args[]) {
    NewBasicUniversalTcpClient client;
    try {
      client = new NewBasicUniversalTcpClient();
    } catch (IOException e) {
      GNSClient.getLogger().severe("Problem creating client:" + e);
      return;
    }
    try {
      sendDeleteCreate(client);
      //GuidUtils.lookupOrCreateAccountGuid(client, "test@gns.name", "frank", true);
      //client.checkConnectivity();
    } catch (Exception e) {
      GNSClient.getLogger().severe("Problem executing command:" + e);
    }
  }
}
