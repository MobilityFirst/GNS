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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import static edu.umass.cs.gnscommon.GnsProtocol.BAD_RESPONSE;
import edu.umass.cs.gnsserver.httpserver.GnsHttpServer;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.GnsApp;
import edu.umass.cs.gnsserver.gnsApp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Handles sending and receiving of commands.
 *
 * @author westy
 */
public class CommandHandler {

  // handles command processing
  private static final CommandModule commandModule = new CommandModule();

  private static long commandCount = 0;

  private static final boolean USE_EXEC_POOL_TO_RUN_COMMANDS = true;

  private static final ExecutorService execPool
          = USE_EXEC_POOL_TO_RUN_COMMANDS == true ? Executors.newFixedThreadPool(100) : null;

  /**
   * Handles command packets coming in from the client.
   *
   * @param incomingJSON
   * @param handler
   * @throws JSONException
   * @throws UnknownHostException
   */
  private static void handlePacketCommandRequest(JSONObject incomingJSON, ClientRequestHandlerInterface handler,
          GnsApp app)
          throws JSONException, UnknownHostException {
    final Long receiptTime = System.currentTimeMillis(); // instrumentation
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info("<<<<<<<<<<<<<<<<< COMMAND PACKET RECEIVED: " + incomingJSON);
    }
    final CommandPacket packet = new CommandPacket(incomingJSON);
    // FIXME: Don't do this every time. 
    // Set the host field. Used by the help command and email module. 
    commandModule.setHTTPHost(handler.getNodeAddress().getHostString() + ":" + GnsHttpServer.getPort());
    final JSONObject jsonFormattedCommand = packet.getCommand();
    // Adds a field to the command to allow us to process the authentication of the signature
    addMessageWithoutSignatureToCommand(jsonFormattedCommand, handler);
    final GnsCommand command = commandModule.lookupCommand(jsonFormattedCommand);
    DelayProfiler.updateDelay("commandPreProc", receiptTime);
    final Long runCommandStart = System.currentTimeMillis(); // instrumentation
    if (USE_EXEC_POOL_TO_RUN_COMMANDS) {
      execPool.submit(new WorkerTask(jsonFormattedCommand, command, handler, packet, app, receiptTime));
    } else {
      runCommand(jsonFormattedCommand, command, handler, packet, app, receiptTime);
    }
    DelayProfiler.updateDelay("runCommand", runCommandStart);
  }

  private static void runCommand(JSONObject jsonFormattedCommand, GnsCommand command,
          ClientRequestHandlerInterface handler, CommandPacket packet, GnsApp app, long receiptTime) {
    try {
      final Long executeCommandStart = System.currentTimeMillis(); // instrumentation
      CommandResponse<String> returnValue = executeCommand(command, jsonFormattedCommand, handler);
      DelayProfiler.updateDelay("executeCommand", executeCommandStart);
      // the last arguments here in the call below are instrumentation that the client can use to determine LNS load
      CommandValueReturnPacket returnPacket = new CommandValueReturnPacket(packet.getClientRequestId(),
              packet.getLNSRequestId(),
              packet.getServiceName(), returnValue,
              handler.getReceivedRequests(), handler.getRequestsPerSecond(),
              System.currentTimeMillis() - receiptTime);

      try {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("HANDLING COMMAND REPLY : " + returnPacket.toString());
        }
        handleCommandReturnValuePacketForApp(returnPacket.toJSONObject(), app);
      } catch (IOException e) {
        GNS.getLogger().severe("Problem replying to command: " + e);
      }

    } catch (JSONException e) {
      GNS.getLogger().severe("Problem  executing command: " + e);
      e.printStackTrace();
    }
    DelayProfiler.updateDelay("handlePacketCommandRequest", receiptTime);
  }

  private static class WorkerTask implements Runnable {

    private final JSONObject jsonFormattedCommand;
    private final GnsCommand command;
    private final ClientRequestHandlerInterface handler;
    private final CommandPacket packet;
    private final GnsApp app;
    private final long receiptTime;

    public WorkerTask(JSONObject jsonFormattedCommand, GnsCommand command, ClientRequestHandlerInterface handler, CommandPacket packet, GnsApp app, long receiptTime) {
      this.jsonFormattedCommand = jsonFormattedCommand;
      this.command = command;
      this.handler = handler;
      this.packet = packet;
      this.app = app;
      this.receiptTime = receiptTime;
    }

    @Override
    public void run() {
      runCommand(jsonFormattedCommand, command, handler, packet, app, receiptTime);
    }
  }

  // this little dance is because we need to remove the signature to get the message that was signed
  // alternatively we could have the client do it but that just means a longer message
  // OR we could put the signature outside the command in the packet, but some packets don't need a signature
  private static void addMessageWithoutSignatureToCommand(JSONObject command, ClientRequestHandlerInterface handler) throws JSONException {
    if (command.has(SIGNATURE)) {
      String signature = command.getString(SIGNATURE);
      command.remove(SIGNATURE);
      String commandSansSignature = CanonicalJSON.getCanonicalForm(command);
      //String commandSansSignature = JSONUtils.getCanonicalJSONString(command);
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("########CANONICAL JSON: " + commandSansSignature);
      }
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
        //GNS.getLogger().info("Executing command: " + command.toString());
        GNS.getLogger().fine("Executing command: " + command.toString() + " with " + json);
        return command.execute(json, handler);
      } else {
        return new CommandResponse<String>(BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED + " - Don't understand " + json.toString());
      }
    } catch (JSONException e) {
      e.printStackTrace();
      return new CommandResponse<String>(BAD_RESPONSE + " " + JSON_PARSE_ERROR + " " + e + " while executing command.");
    } catch (NoSuchAlgorithmException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + QUERY_PROCESSING_ERROR + " " + e);
    } catch (InvalidKeySpecException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + QUERY_PROCESSING_ERROR + " " + e);
    } catch (SignatureException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + QUERY_PROCESSING_ERROR + " " + e);
    } catch (InvalidKeyException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + QUERY_PROCESSING_ERROR + " " + e);
    } catch (UnsupportedEncodingException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + QUERY_PROCESSING_ERROR + " " + e);
    }
  }

  //
  // Code for handling commands at the app
  //
  // public because the app uses this
  /**
   * Encapsulates the info needed for a command request.
   */
  public static class CommandRequestInfo {

    private final String host;
    private final int port;
    // For debugging
    private final String command;
    private final String guid;

    /**
     *
     * @param host
     * @param port
     * @param command
     * @param guid
     */
    public CommandRequestInfo(String host, int port, String command, String guid) {
      this.host = host;
      this.port = port;
      this.command = command;
      this.guid = guid;
    }

    /**
     * Returns the host.
     * 
     * @return a string
     */
    public String getHost() {
      return host;
    }

    /**
     * Returns the port.
     * 
     * @return an int
     */
    public int getPort() {
      return port;
    }

    /**
     * Returns the command.
     * 
     * @return a string
     */
    public String getCommand() {
      return command;
    }

    /**
     * Returns the guid.
     * 
     * @return a string
     */
    public String getGuid() {
      return guid;
    }

  }

  /**
   * Called when a command packet is received by the app. 
   * 
   * @param json
   * @param app
   * @throws JSONException
   * @throws IOException
   */
  public static void handleCommandPacketForApp(JSONObject json, GnsApp app) throws JSONException, IOException {
    CommandPacket packet = new CommandPacket(json);
    // Squirrel away the host and port so we know where to send the command return value
    // A little unnecessary hair for debugging... also peek inside the command.
    JSONObject command;
    String commandString = null;
    String guid = null;
    if ((command = packet.getCommand()) != null) {
      commandString = command.optString(COMMANDNAME, null);
      guid = command.optString(GUID, command.optString(NAME, null));
    }
    app.outStandingQueries.put(packet.getClientRequestId(),
            new CommandRequestInfo(packet.getSenderAddress(), packet.getSenderPort(),
                    commandString, guid));
    handlePacketCommandRequest(json, app.getClientCommandProcessor().getRequestHandler(), app);
  }

  private static long lastStatsTime = 0;

  /**
   * Called when a command return value packet is received by the app.
   * 
   * @param json
   * @param app
   * @throws JSONException
   * @throws IOException
   */
  public static void handleCommandReturnValuePacketForApp(JSONObject json, GnsApp app) throws JSONException, IOException {
    CommandValueReturnPacket returnPacket = new CommandValueReturnPacket(json);
    int id = returnPacket.getClientRequestId();
    CommandRequestInfo sentInfo;
    if ((sentInfo = app.outStandingQueries.get(id)) != null) {
      app.outStandingQueries.remove(id);
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("&&&&&&& For " + sentInfo.getCommand() + " | " + sentInfo.getGuid() + " APP IS SENDING VALUE BACK TO "
                + sentInfo.getHost() + "/" + sentInfo.getPort() + ": " + returnPacket.toString());
      }
      app.sendToClient(new InetSocketAddress(sentInfo.getHost(), sentInfo.getPort()), json);

      // shows us stats every 100 commands, but not more than once every 5 seconds
      if (commandCount++ % 100 == 0) {
        if (System.currentTimeMillis() - lastStatsTime > 5000) {
          System.out.println("8888888888888888888888888888>>>> " + DelayProfiler.getStats());
          lastStatsTime = System.currentTimeMillis();
        }
      }
    } else {
      GNS.getLogger().severe("Command packet info not found for " + id + ": " + json);
    }
  }
}
