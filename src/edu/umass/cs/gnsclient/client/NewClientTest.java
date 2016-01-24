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

  public static void main(String args[]) {
    NewClientBase client;
    try {
      client = new NewClientBase();
    } catch (IOException e) {
      GNSClient.getLogger().severe("Problem creating client:" + e);
      return;
    }
    try {
      client.sendReconfigurationRequest(CREATE_SERVICE_NAME, "test@gns.name", "{}",
              (Request response) -> {
                GNSClient.getLogger().info("Received response: " + response);
              });
    } catch (Exception e) {
      GNSClient.getLogger().severe("Problem executing command:" + e);
      System.exit(-1);
    }
    System.exit(0);

//    try {
//      client.newAccountGuidCreate("test@gns.name", "frank");
//    } catch (Exception e) {
//      GNSClient.getLogger().severe("Problem executing command:" + e);
//      System.exit(-1);
//    }
//    System.exit(0);
  }

}
