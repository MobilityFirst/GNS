/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Abhigyan Sharma, Westy */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AbstractUpdate;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnscommon.packets.PacketUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientCommandProcessorConfig;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

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
   * @param packet
   * @param doNotReplyToClient
   * @param app
   *
   * @throws JSONException
   * @throws UnknownHostException
   */
  public static void handleCommandPacket(CommandPacket packet,
          boolean doNotReplyToClient, GNSApplicationInterface<String> app) throws JSONException,
          UnknownHostException {
    runCommand(addMessageWithoutSignatureToCommand(packet),
            commandModule.lookupCommand(PacketUtils.getCommand(packet)),
            app.getRequestHandler(), doNotReplyToClient, app);
  }

  private static final long LONG_DELAY_THRESHOLD = 1;

  private static void runCommand(CommandPacket commandPacket,
          AbstractCommand command, ClientRequestHandlerInterface handler,
          boolean doNotReplyToClient, GNSApplicationInterface<String> app) {
    JSONObject jsonFormattedCommand = PacketUtils.getCommand(commandPacket);
    try {
      long receiptTime = System.currentTimeMillis(); // instrumentation
      final Long executeCommandStart = System.currentTimeMillis(); // instrumentation
      // Other than this line, one below and some catches all of this
      // method is instrumentation.
      CommandResponse returnValue = executeCommand(command,
              commandPacket, handler);

      assert (commandPacket.getRequestType() != null) : "request type is null";
      assert (commandPacket.getCommandType() != null) : "command type is null";
      assert (command != null) : "command is null";
      // instrumentation
      DelayProfiler.updateDelay("executeCommand", executeCommandStart);
      if (System.currentTimeMillis() - executeCommandStart > LONG_DELAY_THRESHOLD) {
        DelayProfiler.updateDelay(commandPacket.getRequestType() + "."
                + command.getCommandType(),
                executeCommandStart);
      }
      if (System.currentTimeMillis() - executeCommandStart > LONG_DELAY_THRESHOLD) {
        ClientCommandProcessorConfig
                .getLogger()
                .log(Level.FINE,
                        "{0} command {1} took {2}ms of execution delay (delay logging threshold={2}ms)",
                        new Object[]{
                          handler.getApp(),
                          command.getSummary(),
                          (System.currentTimeMillis() - executeCommandStart),
                          LONG_DELAY_THRESHOLD});
      }
      // the last arguments here in the call below are instrumentation
      // that the client can use to determine LNS load
      ResponsePacket returnPacket = new ResponsePacket(
              commandPacket.getRequestID(),
              commandPacket.getServiceName(), returnValue, 0, 0,
              System.currentTimeMillis() - receiptTime);

      try {
        assert (returnPacket.getErrorCode() != null);
        ClientCommandProcessorConfig.getLogger().log(Level.FINE,
                "{0} handling command reply: {1}",
                new Object[]{handler.getApp(), returnPacket});
        // Possibly send the return value back to the client
        handleCommandReturnValuePacketForApp(commandPacket, returnPacket,
                doNotReplyToClient, app);
      } catch (IOException e) {
        ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
                "Problem replying to command: {0}", e);
      }

    } catch (JSONException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
              "{0}: problem  executing command: {1}",
              new Object[]{handler.getApp(), e});
      e.printStackTrace();
    }

    // reply to client is true, this means this is the active replica
    // that recvd the request from the gnsClient. So, let's check for
    // sending trigger to Context service here.
    if (Config.getGlobalBoolean(GNSConfig.GNSC.ENABLE_CNS)) {
      if (!doNotReplyToClient) {

        if (command.getClass().getSuperclass() == AbstractUpdate.class) {
          GNSConfig
                  .getLogger()
                  .log(Level.FINE,
                          "{0} sending trigger to context service for {1}:{2}",
                          new Object[]{handler.getApp(), command,
                            jsonFormattedCommand});

          app.getContextServiceGNSClient().sendTiggerOnGnsCommand(
                  jsonFormattedCommand, command, false);
        }
      }
    }

  }

  private static CommandPacket addMessageWithoutSignatureToCommand(
          CommandPacket commandPacket) throws JSONException {
    JSONObject command = PacketUtils.getCommand(commandPacket);
    CommandUtils.addMessageWithoutSignatureToJSON(command);
    return commandPacket;
  }

  /**
   * Execute the commandPacket.
   *
   * @param commandHandler
   * @param commandPacket
   * @param handler
   * @return Result of executing {@code commandPacket}.
   */
  public static CommandResponse executeCommand(AbstractCommand commandHandler,
          CommandPacket commandPacket, ClientRequestHandlerInterface handler) {
    try {
      if (commandHandler != null) {
        return commandHandler.execute(getInternalHeaderAfterEnforcingChecks(commandPacket,
                handler), commandPacket, handler);
      } else {
        return new CommandResponse(ResponseCode.OPERATION_NOT_SUPPORTED,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + GNSProtocol.OPERATION_NOT_SUPPORTED.toString() + " - Don't understand "
                + PacketUtils.getCommand(commandPacket));
      }
    } catch (JSONException e) {
      // e.printStackTrace();
      return new CommandResponse(ResponseCode.JSON_PARSE_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.JSON_PARSE_ERROR.toString() + " " + e
              + " while executing command.");
    } catch (NoSuchAlgorithmException | InvalidKeySpecException | ParseException | SignatureException | InvalidKeyException | UnsupportedEncodingException e) {
      return new CommandResponse(ResponseCode.QUERY_PROCESSING_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.QUERY_PROCESSING_ERROR.toString() + " " + e);
    } catch (InternalRequestException e) {
      return new CommandResponse(e.getCode(), GNSProtocol.BAD_RESPONSE.toString() + " "
              + ResponseCode.INTERNAL_REQUEST_EXCEPTION + " " + e);
    }
  }

  private static InternalRequestHeader getInternalHeaderAfterEnforcingChecks(
          CommandPacket commandPacket, ClientRequestHandlerInterface handler)
          throws InternalRequestException {
    InternalRequestHeader header = PacketUtils
            .getInternalRequestHeader(commandPacket);
    if (header == null) {
      return header;
    }
    // The checks below are unnecessary and are only expositionary.

    /* TTL expiration, but should never expire here as the sender would
		 * have not sent an expiring request in the first place.
     */
    if (header.getTTL() == 0) {
      throw new InternalRequestException(
              ResponseCode.INTERNAL_REQUEST_EXCEPTION, "TTL expired");
    }

    /**
     * Note: It is pointless to try to check whether a previous request in this
     * chain was already coordinated and this request is also coordinated
     * because if we are here, it is too late. This check must be done at
     * the sender side. Indeed most any reasonable check we can do to restrict
     * the capabilities of active code at the receiver might as well be
     * done at the sender. We can not detect node cycles at the sender
     * but it is unclear that we even care to prevent node cycles.
     *
     * It is unclear how to disallow targetGUID cycles unless we carry the
     * entire chain information, which seems like too much work given that
     * we already have TTLs to limit cycles.
     */

    // nothing suspicious detected
    return header;
  }

  private static long lastStatsTime = 0;

  /**
   * Called when a command return value packet is received by the app.
 * @param command 
   *
   * @param returnPacket
   * @param doNotReplyToClient
   *
   * @param app
   * @throws JSONException
   * @throws IOException
   */
  public static void handleCommandReturnValuePacketForApp(CommandPacket command, 
          ResponsePacket returnPacket, boolean doNotReplyToClient,
          GNSApplicationInterface<String> app) throws JSONException, IOException {
    if (!doNotReplyToClient) {
      app.sendToClient(command, returnPacket, returnPacket.toJSONObject());
    }

    // shows us stats every 100 commands, but not more than once every 5
    // seconds
    if (commandCount++ % 100 == 0
            && Config.getGlobalBoolean(RC.ENABLE_INSTRUMENTATION)) {
      if (System.currentTimeMillis() - lastStatsTime > 5000) {
        ClientCommandProcessorConfig.getLogger().log(Level.INFO,
                "{0} {1}",
                new Object[]{app, DelayProfiler.getStats()});
        lastStatsTime = System.currentTimeMillis();
      }
    }
  }
}
