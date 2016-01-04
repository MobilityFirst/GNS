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
import static edu.umass.cs.gnscommon.GnsProtocol.GUID;
import static edu.umass.cs.gnscommon.GnsProtocol.REPLACE_USER_JSON;
import static edu.umass.cs.gnscommon.GnsProtocol.USER_JSON;
import static edu.umass.cs.gnscommon.GnsProtocol.WRITER;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.nio.NIOTransport;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.net.InetSocketAddress;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;

/**
 * In this example we demonstrate the asynchronous client.
 *
 * @author westy
 */
public class ClientAsynchExample {

  private static final String ACCOUNT_ALIAS = "gnstest@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static BasicUniversalTcpClient client;
  private static GuidEntry guid;

  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, GnsException,
          InvalidKeyException, SignatureException, Exception {

    Logger log = NIOTransport.getLogger();
    log.setLevel(Level.FINEST);
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.FINEST);
    log.addHandler(handler);
    // Bring up the server selection dialog
    InetSocketAddress address = ServerSelectDialog.selectServer();
    // Create the client
    client = new BasicUniversalTcpClient(address.getHostName(), address.getPort());
    try {
      // Create a guid (which is also an account guid)
      guid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, "password", true);
    } catch (Exception e) {
      System.out.println("Exception during accountGuid creation: " + e);
      System.exit(1);
    }
    System.out.println("Client connected to GNS at " + address.getHostName() + ":" + address.getPort());

    // Create a JSON Object to initialize our guid record
    JSONObject json = new JSONObject("{\"occupation\":\"busboy\","
            + "\"friends\":[\"Joe\",\"Sam\",\"Billy\"],"
            + "\"gibberish\":{\"meiny\":\"bloop\",\"einy\":\"floop\"},"
            + "\"location\":\"work\",\"name\":\"frank\"}");

    JSONObject command = client.createAndSignCommand(guid.getPrivateKey(), REPLACE_USER_JSON, GUID,
            guid, USER_JSON, json.toString(), WRITER, guid.getGuid());
    System.out.println("Command is " + command.toString());
    int id = client.generateNextRequestID();
    System.out.println("Generated id is " + id);
    CommandPacket commandPacket = new CommandPacket(id, command);
    System.out.println("Packet is " + commandPacket.toString());

    // Do busy wait until we get a reponse.
    // This is only a good way in an example. In real code don't do this!
    do {
      client.sendCommandPacketAsynch(commandPacket);
      System.out.println("Sent command packet ");
      ThreadUtils.sleep(1000);
    } while (!client.isAsynchResponseReceived(id));
    CommandResult commandResult = client.removeAsynchResponse(id);
    System.out.println("commandResult is "
            + (commandResult.getErrorCode().equals(NSResponseCode.NO_ERROR)
                    ? commandResult.getResult()
                    : commandResult.getErrorCode().toString()));

  }

}
