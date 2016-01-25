/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnsserver.gnsApp.packet.CommandPacket;
import org.json.JSONObject;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import static edu.umass.cs.gnsserver.gnsApp.packet.Packet.PacketType.COMMAND;
import static edu.umass.cs.gnsserver.gnsApp.packet.Packet.PacketType.COMMAND_RETURN_VALUE;
import static edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.json.JSONException;

/**
 *
 * @author westy
 */
public class NewClientSimpleExample extends NewClientSendAndCallback {

  public NewClientSimpleExample() throws IOException {
    super(new HashSet<>(Arrays.asList(Packet.PacketType.COMMAND, Packet.PacketType.COMMAND_RETURN_VALUE)));
  }
  
  private class ClientRequestCallback implements RequestCallback {
    @Override
    public void handleResponse(Request response) {
      try {
        JSONObject jsonObject = new JSONObject(response.toString());
        switch (Packet.getPacketType(jsonObject)) {
          case COMMAND:
            GNSClient.getLogger().severe("Should not have received a Command packet!!");
            break;
          case COMMAND_RETURN_VALUE:
            GNSClient.getLogger().severe("Received response:" + response);
            break;
          default:
            break;
        }
      } catch (JSONException e) {
        GNSClient.getLogger().severe("Unable to parse response: " + e);
      }

    }
  }

  public static void main(String args[]) {
    NewClientSimpleExample client;
    try {
      client = new NewClientSimpleExample();
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
      //System.exit(-1);
    }
    //System.exit(0);
  }

}
