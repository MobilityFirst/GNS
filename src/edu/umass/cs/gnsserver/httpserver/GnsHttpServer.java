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
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccessSupport;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.umass.cs.gnsserver.main.GNS;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import static edu.umass.cs.gnsserver.httpserver.Defs.KEYSEP;
import static edu.umass.cs.gnsserver.httpserver.Defs.QUERYPREFIX;
import static edu.umass.cs.gnsserver.httpserver.Defs.VALSEP;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import java.util.Date;
import java.util.Map;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.json.JSONObject;

/**
 *
 *
 * @author westy
 */
public class GnsHttpServer {

  private static final String GNSPATH = GNS.GNS_URL_PATH;
  private static final int startingPort = 8080;
  private static int port;
  // handles command processing
  private static final CommandModule commandModule = new CommandModule();
  private static ClientRequestHandlerInterface requestHandler;
  private static Date serverStartDate = new Date();

  private static boolean debuggingEnabled = false;

  /**
   *
   * @param requestHandler
   */
  public static void runHttp(ClientRequestHandlerInterface requestHandler) {
    GnsHttpServer.requestHandler = requestHandler;
    runServer();
  }

  /**
   * Start the server.
   */
  public static void runServer() {
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
   * Try to start the http server at the port.
   * @param port
   * @return true if it was started
   */
  public static boolean tryPort(int port) {
    try {
      InetSocketAddress addr = new InetSocketAddress(port);
      HttpServer server = HttpServer.create(addr, 0);

      server.createContext("/", new EchoHandler());
      server.createContext("/" + GNSPATH, new DefaultHandler());
      server.setExecutor(Executors.newCachedThreadPool());
      server.start();
      GNS.getLogger().info("HTTP server is listening on port " + port);
      return true;
    } catch (IOException e) {
      GNS.getLogger().fine("HTTP server failed to start on port " + port + " due to " + e);
      return false;
    }
  }

  private static class DefaultHandler implements HttpHandler {

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
          if (debuggingEnabled) {
            GNS.getLogger().info("HTTP SERVER REQUEST FROM " + exchange.getRemoteAddress().getHostName() + ": " + uri.toString());
          }
          String path = uri.getPath();
          String query = uri.getQuery() != null ? uri.getQuery() : ""; // stupidly it returns null for empty query

          String action = path.replaceFirst("/" + GNSPATH + "/", "");

          String response;
          if (!action.isEmpty()) {
            if (debuggingEnabled) {
              GNS.getLogger().fine("Action: " + action + " Query:" + query);
            }
            response = processQuery(host, action, query);
          } else {
            response = BAD_RESPONSE + " " + NO_ACTION_FOUND;
          }
          if (debuggingEnabled) {
            GNS.getLogger().finer("Response: " + response);
          }
          responseBody.write(response.getBytes());
          responseBody.close();
        }
      } catch (Exception e) {
        GNS.getLogger().severe("Error: " + e);
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
  private static String processQuery(String host, String action, String queryString) {
    // Set the host field. Used by the help command. Find a better way to to do this?
    commandModule.setHTTPHost(host);
    // Convert the URI into a JSONObject, stuffing in some extra relevant fields like
    // the signature, and the message signed.
    String fullString = action + QUERYPREFIX + queryString; // for signature check
    Map<String, String> queryMap = Util.parseURIQueryString(queryString);
    //new command processing
    queryMap.put(COMMANDNAME, action);
    if (queryMap.keySet().contains(SIGNATURE)) {
      String signature = queryMap.get(SIGNATURE);
      String message = AccessSupport.removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature);
      queryMap.put(SIGNATUREFULLMESSAGE, message);
    }
    JSONObject jsonFormattedCommand = new JSONObject(queryMap);

    // Now we execute the command
    GnsCommand command = commandModule.lookupCommand(jsonFormattedCommand);
    return CommandHandler.executeCommand(command, jsonFormattedCommand, GnsHttpServer.requestHandler).getReturnValue();
  }

  /**
   * Returns info about the server.
   */
  private static class EchoHandler implements HttpHandler {

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

        String buildVersion = GNS.readBuildVersion();
        String buildVersionInfo = "Unknown";
        if (buildVersion != null) {
          buildVersionInfo = "Build Version: " + buildVersion;
        }
        String responsePreamble = "<html><head><title>GNS Server Status</title></head><body><p>";
        String responsePostamble = "</p></body></html>";
        String serverStartDateString = "Server start time: " + Format.formatDualDate(serverStartDate);
        String serverUpTimeString = "Server uptime: " + DurationFormatUtils.formatDurationWords(new Date().getTime() - serverStartDate.getTime(), true, true);
        String serverSSLMode = "Server SSL mode: " + ReconfigurationConfig.getServerSSLMode().toString();
        String clientSSLMode = "Client SSL mode: " + ReconfigurationConfig.getClientSSLMode().toString();
        String serverLocalNameServerID = "Local CCP address: " + GnsHttpServer.requestHandler.getNodeAddress();
        String numberOfNameServers = "Server count: " + GnsHttpServer.requestHandler.getGnsNodeConfig().getNumberOfNodes() + "\n";
        //String backingStoreClass = "Backing Store Class: " + Config.dataStore.getClassName() + "\n\n";
        String requestsReceivedString = "Client requests received: " + GnsHttpServer.requestHandler.getReceivedRequests();
        String requestsRateString = "Client requests rate: " + GnsHttpServer.requestHandler.getRequestsPerSecond();
        StringBuilder resultString = new StringBuilder();
        resultString.append("Servers:");
        for (String topLevelNode : GnsHttpServer.requestHandler.getGnsNodeConfig().getNodeIDs()) {
          resultString.append("<br>&nbsp;&nbsp;");
          resultString.append((String) topLevelNode);
          resultString.append("&nbsp;=&gt;&nbsp;");
          resultString.append(GnsHttpServer.requestHandler.getGnsNodeConfig().getBindAddress(topLevelNode));
          resultString.append("&nbsp;&nbspPublic IP:&nbsp;");
          resultString.append(GnsHttpServer.requestHandler.getGnsNodeConfig().getNodeAddress(topLevelNode));
        }
        String nodeAddressesString = resultString.toString();
        resultString = new StringBuilder();
        String prefix = "";
        for (Object recon : GnsHttpServer.requestHandler.getGnsNodeConfig().getReconfigurators()) {
          resultString.append(prefix);
          resultString.append((String) recon);
          prefix = ", ";
        }
        String reconfiguratorsString = "Reconfigurators: " + resultString.toString();
        resultString = new StringBuilder();
        prefix = "";
        for (Object activeReplica : GnsHttpServer.requestHandler.getGnsNodeConfig().getActiveReplicas()) {
          resultString.append(prefix);
          resultString.append((String) activeReplica);
          prefix = ", ";
        }
        String activeReplicasString = "Active replicas: " + resultString.toString();
        String consoleLogLevelString = "Console log level is " + GNS.getLogger().getLevel().getLocalizedName();
        String fileLogLevelString = "File log level is " + GNS.getLogger().getLevel().getLocalizedName();

        responseBody.write(responsePreamble.getBytes());
        responseBody.write(buildVersionInfo.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(serverStartDateString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(serverUpTimeString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(serverLocalNameServerID.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(serverSSLMode.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(clientSSLMode.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(numberOfNameServers.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(nodeAddressesString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(reconfiguratorsString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(activeReplicasString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(requestsReceivedString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(requestsRateString.getBytes());
        responseBody.write("<br>".getBytes());

        responseBody.write("Gigapaxos is enabled<br>".getBytes());
     
        responseBody.write("New app is enabled<br>".getBytes());

        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          responseBody.write("Server debug is true<br>".getBytes());
        }
        if (GnsHttpServer.requestHandler.getParameters().isDebugMode()) {
          responseBody.write("CCP debug is true<br>".getBytes());
        }
        responseBody.write(consoleLogLevelString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write(fileLogLevelString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write("<br>".getBytes());
        //responseBody.write(backingStoreClass.getBytes());
        while (iter.hasNext()) {
          String key = iter.next();
          List values = requestHeaders.get(key);
          String s = key + " = " + values.toString() + "\n";
          responseBody.write(s.getBytes());
          responseBody.write("<br>".getBytes());
        }
        responseBody.write(responsePostamble.getBytes());
        responseBody.close();
      }
    }
  }

  /**
   * Return the port.
   * 
   * @return an int
   */
  public static int getPort() {
    return port;
  }
  
}
