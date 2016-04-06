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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientCommandProcessorConfig;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.DelayProfiler;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.logging.Level;

/**
 * Handles sending and receiving of commands.
 *
 * @author westy, arun
 */
public class CommandHandler {

  // handles command processing
  private static final CommandModule commandModule = new CommandModule();

  private static long commandCount = 0;

  /**
   * Handles command packets coming in from the client.
   *
   * @param incomingJSON
   * @param handler
   * @throws JSONException
   * @throws UnknownHostException
   */
  private static void handlePacketCommandRequest(CommandPacket packet, boolean doNotReplyToClient,
          GNSApp app)
          throws JSONException, UnknownHostException {
    final Long receiptTime = System.currentTimeMillis(); // instrumentation
    ClientRequestHandlerInterface handler = app.getRequestHandler();
    ClientCommandProcessorConfig.getLogger().log(Level.FINE,
            "Command packet received: {0}", new Object[]{packet.getSummary()});
    final JSONObject jsonFormattedCommand = packet.getCommand();
    // Adds a field to the command to allow us to process the authentication of the signature
    addMessageWithoutSignatureToCommand(jsonFormattedCommand, handler);
    final GnsCommand command = commandModule.lookupCommand(jsonFormattedCommand);
    runCommand(jsonFormattedCommand, command, handler, packet, doNotReplyToClient, app, receiptTime);
  }

  private static final long LONG_DELAY_THRESHOLD = 1;

  private static void runCommand(JSONObject jsonFormattedCommand, GnsCommand command,
          ClientRequestHandlerInterface handler, CommandPacket packet, boolean doNotReplyToClient, GNSApp app, long receiptTime) {
    try {
      final Long executeCommandStart = System.currentTimeMillis(); // instrumentation
      // Other than this line, one below and some catches all of this method is instrumentation.
      CommandResponse<String> returnValue = executeCommand(command, jsonFormattedCommand, handler);

      // instrumentation
      DelayProfiler.updateDelay("executeCommand", executeCommandStart);
      if (System.currentTimeMillis() - executeCommandStart > LONG_DELAY_THRESHOLD) {
        DelayProfiler.updateDelay(packet.getRequestType() + "."
                + command.getCommandName(), executeCommandStart);
      }
      if (System.currentTimeMillis() - executeCommandStart > LONG_DELAY_THRESHOLD) {
        ClientCommandProcessorConfig
                .getLogger()
                .log(Level.WARNING,
                        "Command {0} took {1}ms of execution delay (delay logging threshold={2}ms)",
                        new Object[]{
                          command.getSummary(),
                          (System.currentTimeMillis() - executeCommandStart),
                          LONG_DELAY_THRESHOLD});
      }
      // the last arguments here in the call below are instrumentation that the client can use to determine LNS load
      CommandValueReturnPacket returnPacket = new CommandValueReturnPacket(packet.getClientRequestId(),
              packet.getLNSRequestId(),
              packet.getServiceName(), returnValue,
              // FIXME: for info purposes add something to record stuff this back in
              0, 0,
              //handler.getReceivedRequests(), handler.getRequestsPerSecond(),
              System.currentTimeMillis() - receiptTime);

      try {
        ClientCommandProcessorConfig.getLogger().log(Level.FINE,
                "Handling command reply: {0}",
                new Object[]{returnPacket});
        // Possibly send the return value back to the client
        handleCommandReturnValuePacketForApp(returnPacket, doNotReplyToClient, app);
      } catch (IOException e) {
        ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
                "Problem replying to command: {0}", e);
      }

    } catch (JSONException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
              "Problem  executing command: {0}", e);
      e.printStackTrace();
    }
  }

  // this little dance is because we need to remove the signature to get the message that was signed
  // alternatively we could have the client do it but that just means a longer message
  // OR we could put the signature outside the command in the packet, 
  // but some packets don't need a signature
  private static void addMessageWithoutSignatureToCommand(JSONObject command, ClientRequestHandlerInterface handler) throws JSONException {
    if (command.has(SIGNATURE)) {
      String signature = command.getString(SIGNATURE);
      command.remove(SIGNATURE);
      String commandSansSignature = CanonicalJSON.getCanonicalForm(command);
      //String commandSansSignature = JSONUtils.getCanonicalJSONString(command);
      ClientCommandProcessorConfig.getLogger().log(Level.FINE,
              "########CANONICAL JSON: {0}",
              new Object[]{commandSansSignature});
      command.put(SIGNATURE, signature);
      command.put(SIGNATUREFULLMESSAGE, commandSansSignature);
    }
  }

  /**
   * Executes the given command with the parameters supplied in the JSONObject.
   *
   * @param command
   * @param json
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> executeCommand(GnsCommand command, JSONObject json, ClientRequestHandlerInterface handler) {
    try {
      if (command != null) {
        ClientCommandProcessorConfig.getLogger().log(Level.FINE,
                "Executing command: {0} in packet {1}",
                new Object[]{command, json});
        return command.execute(json, handler);
      } else {
        return new CommandResponse<>(BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED
                + " - Don't understand " + json.toString());
      }
    } catch (JSONException e) {
      //e.printStackTrace();
      return new CommandResponse<>(BAD_RESPONSE + " " + JSON_PARSE_ERROR + " " + e
              + " while executing command.");
    } catch (NoSuchAlgorithmException | InvalidKeySpecException | ParseException |
            SignatureException | InvalidKeyException | UnsupportedEncodingException e) {
      return new CommandResponse<>(BAD_RESPONSE + " " + QUERY_PROCESSING_ERROR + " " + e);
    }
  }

  /**
   * Called when a command packet is received by the app.
   *
   * @param packet
   * @param doNotReplyToClient
   * @param app
   * @throws JSONException
   * @throws IOException
   */
  public static void handleCommandPacketForApp(CommandPacket packet, boolean doNotReplyToClient, GNSApp app) 
          throws JSONException, IOException {
    // Squirrel away the host and port so we know where to send the command return value
    // A little unnecessary hair for debugging... also peek inside the command.
    JSONObject command;
    String commandString = null;
    String guid = null;
    if ((command = packet.getCommand()) != null) {
      commandString = command.optString(COMMANDNAME, null);
      guid = command.optString(GUID, command.optString(NAME, null));
    }
    //GNSConfig.getLogger().info("FROM: " + packet.getSenderAddress());
    app.outStandingQueries.put(packet.getClientRequestId(),
            new CommandRequestInfo(packet.getSenderAddress(), packet.getSenderPort(),
                    commandString, guid, packet.getMyListeningAddress()));
    handlePacketCommandRequest(packet, doNotReplyToClient, app);
  }

  private static long lastStatsTime = 0;

  /**
   * Called when a command return value packet is received by the app.
   *
   * @param returnPacket
   * @param doNotReplyToClient
   *
   * @param app
   * @throws JSONException
   * @throws IOException
   */
  public static void handleCommandReturnValuePacketForApp(CommandValueReturnPacket returnPacket,
          boolean doNotReplyToClient, GNSApp app) throws JSONException, IOException {
    long id = returnPacket.getClientRequestId();
    CommandRequestInfo sentInfo;
    if ((sentInfo = app.outStandingQueries.get(id)) != null) {
      ClientCommandProcessorConfig.getLogger()
              .log(Level.FINE,
                      "{0}:{1} => {2} -> {3}",
                      new Object[]{
                        sentInfo.getCommand(),
                        sentInfo.getGuid(),
                        returnPacket.getSummary(),
                        sentInfo.getHost() + "/"
                        + sentInfo.getPort()});
      if (!doNotReplyToClient) {
        app.sendToClient(new InetSocketAddress(sentInfo.getHost(), sentInfo.getPort()), returnPacket, returnPacket.toJSONObject(), sentInfo.myListeningAddress);
      }

      // shows us stats every 100 commands, but not more than once every 5 seconds
      if (commandCount++ % 100 == 0) {
        if (System.currentTimeMillis() - lastStatsTime > 5000) {
          ClientCommandProcessorConfig.getLogger().log(Level.INFO, "{0}",
                  new Object[]{DelayProfiler.getStats()});
          lastStatsTime = System.currentTimeMillis();
        }
      }
    } else {
      ClientCommandProcessorConfig.getLogger().severe("Command packet info not found for "
              + id + ": " + returnPacket.getSummary());
    }
  }
}
