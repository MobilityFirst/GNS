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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.BasicUniversalTcpClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.tcp.CommandResult;
import edu.umass.cs.gnsclient.client.tcp.packet.CommandPacket;
import edu.umass.cs.gnsclient.client.tcp.packet.NSResponseCode;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import static edu.umass.cs.gnscommon.GnsProtocol.FIELD;
import static edu.umass.cs.gnscommon.GnsProtocol.GUID;
import static edu.umass.cs.gnscommon.GnsProtocol.READ;
import static edu.umass.cs.gnscommon.GnsProtocol.READER;
import static edu.umass.cs.gnscommon.GnsProtocol.REPLACE_USER_JSON;
import static edu.umass.cs.gnscommon.GnsProtocol.USER_JSON;
import static edu.umass.cs.gnscommon.GnsProtocol.WRITER;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.net.InetSocketAddress;
import org.json.JSONObject;

/**
 * In this example we demonstrate the asynchronous client.
 *
 * @author westy
 */
public class ClientAsynchExample {

  private static final String ACCOUNT_ALIAS = "gnstest@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS

  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, GnsException,
          InvalidKeyException, SignatureException, Exception {

    // Bring up the server selection dialog
    InetSocketAddress address = ServerSelectDialog.selectServer();
    // Create the client
    BasicUniversalTcpClient client = new BasicUniversalTcpClient(address.getHostName(), address.getPort());
    GuidEntry guidEntry = null;
    try {
      // Create a guid (which is also an account guid)
      guidEntry = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, "password", true);
    } catch (Exception e) {
      System.out.println("Exception during accountGuid creation: " + e);
      e.printStackTrace();
      System.exit(1);
    }
    System.out.println("Client connected to GNS at " + address.getHostName() + ":" + address.getPort());

    JSONObject command;
    if (args.length > 0 && args[0].equals("-write")) {
      JSONObject json = new JSONObject("{\"occupation\":\"busboy\","
              + "\"friends\":[\"Joe\",\"Sam\",\"Billy\"],"
              + "\"gibberish\":{\"meiny\":\"bloop\",\"einy\":\"floop\"},"
              + "\"location\":\"work\",\"name\":\"frank\"}");
      command = client.createAndSignCommand(guidEntry.getPrivateKey(), REPLACE_USER_JSON,
              GUID, guidEntry.getGuid(), USER_JSON, json.toString(), WRITER, guidEntry.getGuid());
    } else {
      command = client.createAndSignCommand(guidEntry.getPrivateKey(), READ,
              GUID, guidEntry.getGuid(), FIELD, "occupation",
              READER, guidEntry.getGuid());
    }
    System.out.println("Command is " + command.toString());
    CommandPacket commandPacket = new CommandPacket(-1, command);
    System.out.println("Packet is " + commandPacket.toString());
    do {
      int id = client.generateNextRequestID();
      System.out.println("Generated id is " + id);
      commandPacket.setRequestId(id);
      client.sendCommandPacketAsynch(commandPacket);
      System.out.println("Sent command packet ");
    // Do busy wait until we get a reponse.
      // This is only a good way in an example. In real code don't do this!
      do {
        ThreadUtils.sleep(1000);
      } while (!client.isAsynchResponseReceived(id));
      // Pull the reponse from the client using the id
      CommandResult commandResult = client.removeAsynchResponse(id);
      System.out.println("Last latency was " + commandResult.getClientLatency() + "\n"
              + "commandResult for  " + id + " is "
              + (commandResult.getErrorCode().equals(NSResponseCode.NO_ERROR)
                      ? commandResult.getResult()
                      : commandResult.getErrorCode().toString()));
    } while (true);
    //System.exit(0);
  }
}
