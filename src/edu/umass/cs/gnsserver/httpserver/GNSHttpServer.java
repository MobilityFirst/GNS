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
package edu.umass.cs.gnsserver.httpserver;

/**
 *
 * @author westy
 */
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import edu.umass.cs.gnsclient.client.GNSClient;
import static edu.umass.cs.gnsclient.client.GNSCommand.createGNSCommandFromJSONObject;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.OPERATION_NOT_SUPPORTED;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.QUERY_PROCESSING_ERROR;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATUREFULLMESSAGE;
import edu.umass.cs.gnsserver.main.GNSConfig;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import edu.umass.cs.gnscommon.ResponseCode;
import static edu.umass.cs.gnsserver.httpserver.Defs.QUERYPREFIX;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.main.GNSConfig.GNSC;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.utils.Config;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 *
 * @author westy
 */
public class GNSHttpServer {

  /**
   *
   */
  protected static final String GNS_PATH = Config.getGlobalString(GNSConfig.GNSC.HTTP_SERVER_GNS_URL_PATH);
  private HttpServer httpServer = null;
  private int port;
  // handles command processing
  private final CommandModule commandModule;
  // newer handles command processing
  private GNSClient client = null;

  /**
   *
   */
  protected final ClientRequestHandlerInterface requestHandler;
  private final Date serverStartDate = new Date();

  private final static Logger LOGGER = Logger.getLogger(GNSHttpServer.class.getName());

  /**
   *
   * @param port
   * @param requestHandler
   */
  public GNSHttpServer(int port, ClientRequestHandlerInterface requestHandler) {
    this.commandModule = new CommandModule();
    this.requestHandler = requestHandler;
    if (!Config.getGlobalBoolean(GNSC.DISABLE_MULTI_SERVER_HTTP)) {
      try {
        this.client = new GNSClient();
      } catch (IOException e) {
        LOGGER.log(Level.SEVERE, "Unable to start GNS client:" + e);
      }
    }
    runServer(port);
  }

  /**
   * Start the server.
   *
   * @param startingPort
   */
  public final void runServer(int startingPort) {
    int cnt = 0;
    do {
      // Find the first port after starting port that actually works.
      // Usually if 8080 is busy we can get 8081.
      if (tryPort(startingPort + cnt)) {
        port = startingPort + cnt;
        break;
      }
    } while (cnt++ < 100);
  }

  /**
   * Stop everything.
   */
  public void stop() {
    if (httpServer != null) {
      httpServer.stop(0);
    }
  }

  /**
   * Try to start the http server at the port.
   *
   * @param port
   * @return true if it was started
   */
  public boolean tryPort(int port) {
    try {
      InetSocketAddress addr = new InetSocketAddress(port);
      httpServer = HttpServer.create(addr, 0);

      httpServer.createContext("/", new EchoHttpHandler());
      httpServer.createContext("/" + GNS_PATH, new DefaultHttpHandler());
      httpServer.setExecutor(Executors.newCachedThreadPool());
      httpServer.start();
      // Need to do this for the places where we expose the insecure http service to the user
      requestHandler.setHttpServerPort(port);
      LOGGER.log(Level.INFO,
              "HTTP server is listening on port {0}", port);
      return true;
    } catch (IOException e) {
      LOGGER.log(Level.FINE,
              "HTTP server failed to start on port {0} due to {1}",
              new Object[]{port, e});
      return false;
    }
  }

  /**
   * The default handler.
   */
  protected class DefaultHttpHandler implements HttpHandler {

    /**
     *
     * @param exchange
     */
    @Override
    public void handle(HttpExchange exchange) {
      try {
        String requestMethod = exchange.getRequestMethod();
        if (requestMethod.equalsIgnoreCase("GET")) {
          Headers requestHeaders = exchange.getRequestHeaders();
          String host = requestHeaders.getFirst("Host");
          Headers responseHeaders = exchange.getResponseHeaders();
          responseHeaders.set("Content-Type", "text/plain");
          exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);

          OutputStream responseBody = exchange.getResponseBody();

          URI uri = exchange.getRequestURI();
          LOGGER.log(Level.FINE,
                  "HTTP SERVER REQUEST FROM {0}: {1}", new Object[]{exchange.getRemoteAddress().getHostName(), uri.toString()});
          String path = uri.getPath();
          String query = uri.getQuery() != null ? uri.getQuery() : ""; // stupidly it returns null for empty query

          String commandName = path.replaceFirst("/" + GNS_PATH + "/", "");

          CommandResponse response;
          if (!commandName.isEmpty()) {
            LOGGER.log(Level.FINE, "Action: {0} Query:{1}", new Object[]{commandName, query});
            response = processQuery(host, commandName, query);
          } else {
            response = new CommandResponse(ResponseCode.OPERATION_NOT_SUPPORTED, BAD_RESPONSE
                    + " " + OPERATION_NOT_SUPPORTED + " Don't understand " + commandName + " " + query);
          }
          LOGGER.log(Level.FINER, "Response: " + response);
          // FIXME: This totally ignores the error code.
          responseBody.write(response.getReturnValue().getBytes());
          responseBody.close();
        }
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, "Error: " + e);
        e.printStackTrace();
        try {
          String response = BAD_RESPONSE + " " + QUERY_PROCESSING_ERROR + " " + e;
          OutputStream responseBody = exchange.getResponseBody();
          responseBody.write(response.getBytes());
          responseBody.close();
        } catch (Exception f) {
          // at this point screw it
        }
      }
    }
  }

  /**
   * Process queries for the http service. Converts the URI of e the HTTP query into
   * the JSON Object format that is used by the CommandModeule class, then finds
   * executes the matching command.
   */
  private CommandResponse processQuery(String host, String commandName, String queryString) {
    // Convert the URI into a JSONObject, stuffing in some extra relevant fields like
    // the signature, and the message signed.
    try {
      JSONObject jsonCommand = Util.parseURIQueryStringIntoJSONObject(queryString);
      // getCommandForHttp allows for "dump" as well as "Dump"
      CommandType commandType = CommandType.getCommandForHttp(commandName);
      if (commandType == null) {
        return new CommandResponse(ResponseCode.OPERATION_NOT_SUPPORTED,
                BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED
                + " Sorry, don't understand " + commandName + QUERYPREFIX + queryString);
      }
      // We need to stuff in the COMMAND_INT for the signature check and execution.
      jsonCommand.put(GNSCommandProtocol.COMMAND_INT, commandType.getInt());
      // Possibly do some work on the signature for later use.
      processSignature(jsonCommand);
      // Hair below is to handle some commands locally (creates, delets, selects, admin)
      // and the rest by invoking the GNS client and sending them out.
      // Client will be null if GNSC.DISABLE_MULTI_SERVER_HTTP (see above)
      // is true (or there was a problem).
      if (client == null || commandType.isLocallyHandled()) {
        AbstractCommand command;
        try {
          command = commandModule.lookupCommand(commandType);
          if (command != null) {
            return CommandHandler.executeCommand(command, jsonCommand, requestHandler);
          }
          LOGGER.log(Level.FINE, "lookupCommand returned null for {0}", commandName);
        } catch (IllegalArgumentException e) {
          LOGGER.log(Level.FINE, "lookupCommand failed for {0}", commandName);
        }
        return new CommandResponse(ResponseCode.OPERATION_NOT_SUPPORTED,
                BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED
                + " Sorry, don't understand " + commandName + QUERYPREFIX + queryString);
      } else {
        // Send the command remotely using a client
        try {
          CommandPacket commandResponsePacket
                  = getResponseUsingGNSClient(client, jsonCommand);
          return new CommandResponse(ResponseCode.NO_ERROR,
                  // some crap here to make single field reads return just the value for backward compatibility 
                  specialCaseSingleFieldRead(commandResponsePacket.getResultString(),
                          commandType, jsonCommand));
        } catch (IOException | ClientException e) {
          return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR, BAD_RESPONSE + " "
                  + GNSCommandProtocol.UNSPECIFIED_ERROR + " " + e.toString());
//      } catch (ClientException e) {
//        return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR,
//                BAD_RESPONSE + " " + OPERATION_NOT_SUPPORTED
//                + " Sorry, don't understand " + commandName + QUERYPREFIX + queryString);
        }
      }
    } catch (JSONException e) {
      return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR, BAD_RESPONSE + " "
              + GNSCommandProtocol.UNSPECIFIED_ERROR + " " + e.toString());
    }
  }

  private static void processSignature(JSONObject jsonCommand) throws JSONException {
    if (jsonCommand.has(GNSCommandProtocol.SIGNATURE)) {
      // Squirrel away the signature. Note that it is encoded as a hex string.
      String signature = jsonCommand.getString(GNSCommandProtocol.SIGNATURE);
      // Pull it out of the command because we don't want to have it there when we check the message.
      jsonCommand.remove(SIGNATURE);
      // FIXME: Remove this debugging aid at some point
      String originalMessage = null;
      if (jsonCommand.has("originalBase64")) {
        originalMessage = new String(Base64.decode(jsonCommand.getString("originalBase64")));
        jsonCommand.remove("originalBase64");
      }
      // Convert it to a conanical string (the message) that we can use later to check against the signature.
      String commandSansSignature = CanonicalJSON.getCanonicalForm(jsonCommand);
      // FIXME: Remove this debugging aid at some point
      if (originalMessage != null) {
        if (!originalMessage.equals(commandSansSignature)) {
          LOGGER.log(Level.SEVERE, "signature message mismatch! original: " + originalMessage
                  + " computed for signature: " + commandSansSignature);
        } else {
           LOGGER.log(Level.FINE, "######## original: " + originalMessage);
        }
      }
      // Put the decoded signature back as well as the message that we're going to
      // later compare the signature against.
      jsonCommand.put(SIGNATURE, signature).put(SIGNATUREFULLMESSAGE,
              commandSansSignature);

    }
  }

  //make single field reads return just the value for backward compatibility 
  private static String specialCaseSingleFieldRead(String response, CommandType commandType,
          JSONObject jsonFormattedArguments) {
    try {
      if (commandType.isRead() && jsonFormattedArguments.has(GNSCommandProtocol.FIELD)
              && !jsonFormattedArguments.getString(GNSCommandProtocol.FIELD).equals(GNSCommandProtocol.ENTIRE_RECORD)
              && JSONPacket.couldBeJSON(response) && response.startsWith("{")) {
        String key = jsonFormattedArguments.getString(GNSCommandProtocol.FIELD);
        JSONObject json = new JSONObject(response);
        return json.getString(key);
      }
    } catch (JSONException e) {
      LOGGER.log(Level.SEVERE, "Problem getting single key reponse for : " + e);
      // just return the response if there is some issue
    }
    return response;
  }

  private CommandPacket getResponseUsingGNSClient(GNSClient client,
          JSONObject jsonFormattedArguments) throws ClientException, IOException, JSONException {
    LOGGER.log(Level.FINE, "jsonFormattedCommand =" + jsonFormattedArguments.toString());
    CommandPacket outgoingPacket = createGNSCommandFromJSONObject(jsonFormattedArguments);
    LOGGER.log(Level.FINE, "outgoingPacket =" + outgoingPacket.toString());
    CommandPacket returnPacket = client.execute(outgoingPacket);
    LOGGER.log(Level.FINE, "returnPacket =" + returnPacket.toString());
    /**
     * Can also invoke getResponse(), getResponseString(), getResponseJSONObject()
     * etc. on {@link CommandPacket} as documented in {@link GNSCommand}.
     */
    return returnPacket;

  }

  /**
   * Returns info about the server.
   */
  protected class EchoHttpHandler implements HttpHandler {

    /**
     *
     * @param exchange
     * @throws IOException
     */
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String requestMethod = exchange.getRequestMethod();
      if (requestMethod.equalsIgnoreCase("GET")) {
        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set("Content-Type", "text/HTML");
        exchange.sendResponseHeaders(200, 0);

        OutputStream responseBody = exchange.getResponseBody();
        Headers requestHeaders = exchange.getRequestHeaders();
        Set<String> keySet = requestHeaders.keySet();
        Iterator<String> iter = keySet.iterator();

        String buildVersion = GNSConfig.readBuildVersion();
        String buildVersionInfo = "Build Version: Unable to lookup!";
        if (buildVersion != null) {
          buildVersionInfo = "Build Version: " + buildVersion;
        }
        String responsePreamble = "<html><head><title>GNS Server Status</title></head><body><p>";
        String responsePostamble = "</p></body></html>";
        String serverStartDateString = "Server start time: " + Format.formatDualDate(serverStartDate);
        String serverUpTimeString = "Server uptime: " + DurationFormatUtils.formatDurationWords(new Date().getTime() - serverStartDate.getTime(), true, true);
        String serverSSLMode = "Server SSL mode: " + ReconfigurationConfig.getServerSSLMode().toString();
        String clientSSLMode = "Client SSL mode: " + ReconfigurationConfig.getClientSSLMode().toString();
        String reconAddresses = "Recon addresses: " + ReconfigurationConfig.getReconfiguratorAddresses().toString();
        String numberOfNameServers = "Server count: " + requestHandler.getGnsNodeConfig().getNumberOfNodes();
        String recordsClass = "Records Class: " + GNSConfig.GNSC.getNoSqlRecordsClass();
        //StringBuilder resultString = new StringBuilder();
        // Servers
//        resultString.append("Servers:");
//        for (String topLevelNode : requestHandler.getGnsNodeConfig().getNodeIDs()) {
//          resultString.append("<br>&nbsp;&nbsp;");
//          resultString.append(topLevelNode);
//          resultString.append("&nbsp;=&gt;&nbsp;");
//          resultString.append(requestHandler.getGnsNodeConfig().getBindAddress(topLevelNode));
//          resultString.append("&nbsp;&nbspPublic IP:&nbsp;");
//          resultString.append(requestHandler.getGnsNodeConfig().getNodeAddress(topLevelNode));
//        }
//        String nodeAddressesString = resultString.toString();
        //Reconfigurators
//        resultString = new StringBuilder();
//        String prefix = "";
//        for (String recon : requestHandler.getGnsNodeConfig().getReconfigurators()) {
//          resultString.append("<br>&nbsp;&nbsp;");
//          //resultString.append(prefix);
//          resultString.append(recon);
//          resultString.append("&nbsp;=&gt;&nbsp;");
//          //resultString.append("(");
//          resultString.append(requestHandler.getGnsNodeConfig().getNodeAddress(recon).getHostName());
//          resultString.append(":");
//          resultString.append(requestHandler.getGnsNodeConfig().getNodePort(recon));
//          //resultString.append(")");
//          //prefix = ", ";
//        }
//        String reconfiguratorsString = "Reconfigurators: " + resultString.toString();
        // Replicas
//        resultString = new StringBuilder();
//        prefix = "";
//        for (String activeReplica : requestHandler.getGnsNodeConfig().getActiveReplicas()) {
//          resultString.append("<br>&nbsp;&nbsp;");
//          //resultString.append(prefix);
//          resultString.append(activeReplica);
//          resultString.append("&nbsp;=&gt;&nbsp;");
//          //resultString.append("(");
//          resultString.append(requestHandler.getGnsNodeConfig().getNodeAddress(activeReplica).getHostName());
//          resultString.append(":");
//          resultString.append(requestHandler.getGnsNodeConfig().getNodePort(activeReplica));
//          //resultString.append(")");
//          //prefix = ", ";
//        }
//        String activeReplicasString = "Active replicas: " + resultString.toString();
        // Build the response
        responseBody.write(responsePreamble.getBytes());
        responseBody.write(buildVersionInfo.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(serverStartDateString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(serverUpTimeString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(numberOfNameServers.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(reconAddresses.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(serverSSLMode.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(clientSSLMode.getBytes());
        responseBody.write("<br>".getBytes());

//        responseBody.write(nodeAddressesString.getBytes());
//        responseBody.write("<br>".getBytes());
//        responseBody.write(reconfiguratorsString.getBytes());
//        responseBody.write("<br>".getBytes());
//        responseBody.write(activeReplicasString.getBytes());
//        responseBody.write("<br>".getBytes());
        responseBody.write(recordsClass.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write("Request Headers:".getBytes());
        responseBody.write("<br>".getBytes());
        while (iter.hasNext()) {
          String key = iter.next();
          List<String> values = requestHeaders.get(key);
          String s = key + " = " + values.toString() + "\n";
          responseBody.write(s.getBytes());
          responseBody.write("<br>".getBytes());
        }
        responseBody.write(responsePostamble.getBytes());
        responseBody.close();
      }
    }
  }
}
