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

import edu.umass.cs.gnsserver.httpserver.GnsHttpServer;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AbstractUpdate;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.gnsapp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandValueReturnPacket;
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

  private static final boolean USE_EXEC_POOL_TO_RUN_COMMANDS = false;

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
  private static void handlePacketCommandRequest(CommandPacket packet, boolean doNotReplyToClient,
          GNSApp app)
          throws JSONException, UnknownHostException {
    final Long receiptTime = System.currentTimeMillis(); // instrumentation
    ClientRequestHandlerInterface handler = app.getRequestHandler();
    if (handler.isDebugMode()) {
      GNSConfig.getLogger().log(Level.INFO, "Command packet received: {0}", new Object[]{packet.getSummary()});
    }
    final JSONObject jsonFormattedCommand = packet.getCommand();
    // Adds a field to the command to allow us to process the authentication of the signature
    addMessageWithoutSignatureToCommand(jsonFormattedCommand, handler);
    final GnsCommand command = commandModule.lookupCommand(jsonFormattedCommand);
    if (USE_EXEC_POOL_TO_RUN_COMMANDS) {
      execPool.submit(new WorkerTask(jsonFormattedCommand, command, handler, packet, doNotReplyToClient, app, receiptTime));
    } else {
      runCommand(jsonFormattedCommand, command, handler, packet, doNotReplyToClient, app, receiptTime);
    }
  }

  private static final long LONG_DELAY_THRESHOLD = 1;

  private static void runCommand(JSONObject jsonFormattedCommand, GnsCommand command,
          ClientRequestHandlerInterface handler, CommandPacket packet, boolean doNotReplyToClient, GNSApp app, long receiptTime) {
    try {
      final Long executeCommandStart = System.currentTimeMillis(); // instrumentation
      CommandResponse<String> returnValue = executeCommand(command, jsonFormattedCommand, handler);
      DelayProfiler.updateDelay("executeCommand", executeCommandStart);
      if (System.currentTimeMillis() - executeCommandStart > LONG_DELAY_THRESHOLD) {
        DelayProfiler.updateDelay(packet.getRequestType() + "."
                + command.getCommandName(), executeCommandStart);
      }
      if (System.currentTimeMillis() - executeCommandStart > LONG_DELAY_THRESHOLD) {
        GNSConfig
                .getLogger()
                .log(Level.INFO,
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
        if (handler.isDebugMode()) {
          GNSConfig.getLogger().log(Level.INFO,
                  "Handling command reply: {0}",
                  new Object[]{returnPacket});
        }
        handleCommandReturnValuePacketForApp(returnPacket, doNotReplyToClient, app);
      } catch (IOException e) {
        GNSConfig.getLogger().severe("Problem replying to command: " + e);
      }

    } catch (JSONException e) {
      GNSConfig.getLogger().severe("Problem  executing command: " + e);
      e.printStackTrace();
    }
    
    // reply to client is true, this means this is the active replica
    // that recvd the request from the gnsClient. So, let's check for sending trigger
    // to Context service here.
    if( AppReconfigurableNodeOptions.enableContextService )
    {
	    if( !doNotReplyToClient )
	    {
	    
	    	if(command.getClass().getSuperclass() == AbstractUpdate.class)
	    	{
	    		GNSConfig.getLogger().fine("Sending trigger to CS jsonFormattedCommand "
	    				+jsonFormattedCommand+" command "+command);
	    		
	    		app.getContextServiceGNSClient().sendTiggerOnGnsCommand(jsonFormattedCommand, command, false);
	    	}
	    }
    }
    //DelayProfiler.updateDelay("handlePacketCommandRequest", receiptTime);
  }

  private static class WorkerTask implements Runnable {

    private final JSONObject jsonFormattedCommand;
    private final GnsCommand command;
    private final ClientRequestHandlerInterface handler;
    private final CommandPacket packet;
    private final GNSApp app;
    private final long receiptTime;
    private final boolean doNotReplyToClient;

    public WorkerTask(JSONObject jsonFormattedCommand, GnsCommand command, ClientRequestHandlerInterface handler, CommandPacket packet, boolean doNotReplyToClient, GNSApp app, long receiptTime) {
      this.jsonFormattedCommand = jsonFormattedCommand;
      this.command = command;
      this.handler = handler;
      this.packet = packet;
      this.app = app;
      this.receiptTime = receiptTime;
      this.doNotReplyToClient = doNotReplyToClient;
    }

    @Override
    public void run() {
      runCommand(jsonFormattedCommand, command, handler, packet, doNotReplyToClient, app, receiptTime);
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
      if (handler.isDebugMode()) {
        GNSConfig.getLogger().log(Level.FINE,
                "########CANONICAL JSON: {0}",
                new Object[]{commandSansSignature});
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
        GNSConfig.getLogger().log(Level.FINE,
                "Executing command: {0} in packet {1}",
                new Object[]{command, json});
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

    // arun: Need this for correct receiver messaging
    private final InetSocketAddress myListeningAddress;

    /**
     *
     * @param host
     * @param port
     * @param command
     * @param guid
     */
    public CommandRequestInfo(String host, int port, String command, String guid, InetSocketAddress myListeningAddress) {
      this.host = host;
      this.port = port;
      this.command = command;
      this.guid = guid;
      this.myListeningAddress = myListeningAddress;
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
  public static void handleCommandPacketForApp(CommandPacket packet, boolean doNotReplyToClient, GNSApp app) throws JSONException, IOException {
    //CommandPacket packet = new CommandPacket(json);
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
                    commandString, guid, packet.getMyListeningAddress()));
    handlePacketCommandRequest(packet, doNotReplyToClient, app);
  }

  private static long lastStatsTime = 0;

  /**
   * Called when a command return value packet is received by the app.
   *
   * @param returnPacket
   *
   * @param app
   * @throws JSONException
   * @throws IOException
   */
  public static void handleCommandReturnValuePacketForApp(CommandValueReturnPacket returnPacket, boolean doNotReplyToClient, GNSApp app) throws JSONException, IOException {
    //CommandValueReturnPacket returnPacket = new CommandValueReturnPacket(json);
    long id = returnPacket.getClientRequestId();
    CommandRequestInfo sentInfo;
    if ((sentInfo = app.outStandingQueries.get(id)) != null) {
      app.outStandingQueries.remove(id);
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNSConfig.getLogger()
                .log(Level.INFO,
                        "{0}:{1} => {2} -> {3}",
                        new Object[]{
                          sentInfo.getCommand(),
                          sentInfo.getGuid(),
                          returnPacket.getSummary(),
                          sentInfo.getHost() + "/"
                          + sentInfo.getPort()});
      }
      if (!doNotReplyToClient) {
        app.sendToClient(new InetSocketAddress(sentInfo.getHost(), sentInfo.getPort()), returnPacket, returnPacket.toJSONObject(), sentInfo.myListeningAddress);
      }

      // shows us stats every 100 commands, but not more than once every 5 seconds
      if (commandCount++ % 100 == 0) {
        if (System.currentTimeMillis() - lastStatsTime > 5000) {
          GNSConfig.getLogger().log(Level.INFO, "{0}",
                  new Object[]{DelayProfiler.getStats()});
          lastStatsTime = System.currentTimeMillis();
        }
      }
    } else {
      GNSConfig.getLogger().severe("Command packet info not found for " + id + ": " + returnPacket.getSummary());
    }
  }
}
