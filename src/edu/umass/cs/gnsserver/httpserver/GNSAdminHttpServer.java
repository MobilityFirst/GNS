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
import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import static edu.umass.cs.gnsserver.httpserver.Defs.KEYSEP;
import static edu.umass.cs.gnsserver.httpserver.Defs.QUERYPREFIX;
import static edu.umass.cs.gnsserver.httpserver.Defs.VALSEP;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import java.util.Date;
import java.util.Map;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.json.JSONObject;

/**
 *
 *
 * @author westy
 */
public class GNSAdminHttpServer {

  private static final String GNSPATH = GNSConfig.GNS_URL_PATH;
  private static final int STARTING_PORT = 8080;
  private int port;
  // handles command processing
  private final CommandModule commandModule;
  private final ClientRequestHandlerInterface requestHandler;
  private final Date serverStartDate = new Date();
  
  private final static Logger LOG = Logger.getLogger(GNSAdminHttpServer.class.getName());

  public GNSAdminHttpServer(ClientRequestHandlerInterface requestHandler) {
    this.commandModule = new CommandModule();
    this.requestHandler = requestHandler;
    runServer();
    requestHandler.setHttpServerPort(port);
  }

  /**
   * Start the server.
   */
  public final void runServer() {
    int cnt = 0;
    do {
      // Find the first port after starting port that actually works.
      // Usually if 8080 is busy we can get 8081.
      if (tryPort(STARTING_PORT + cnt)) {
        port = STARTING_PORT + cnt;
        break;
      }
    } while (cnt++ < 100);
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
      HttpServer server = HttpServer.create(addr, 0);

      server.createContext("/", new EchoHandler());
      server.createContext("/" + GNSPATH, new DefaultHandler());
      server.setExecutor(Executors.newCachedThreadPool());
      server.start();
      LOG.log(Level.INFO,
              "HTTP server is listening on port {0}", port);
      return true;
    } catch (IOException e) {
      LOG.log(Level.FINE,
              "HTTP server failed to start on port {0} due to {1}",
              new Object[]{port, e});
      return false;
    }
  }

  private class DefaultHandler implements HttpHandler {

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
          LOG.log(Level.FINE,
                  "HTTP SERVER REQUEST FROM {0}: {1}", new Object[]{exchange.getRemoteAddress().getHostName(), uri.toString()});
          String path = uri.getPath();
          String query = uri.getQuery() != null ? uri.getQuery() : ""; // stupidly it returns null for empty query

          String action = path.replaceFirst("/" + GNSPATH + "/", "");

          String response;
          if (!action.isEmpty()) {
            LOG.log(Level.FINE,
                    "Action: {0} Query:{1}", new Object[]{action, query});
            response = processQuery(host, action, query);
          } else {
            response = BAD_RESPONSE + " " + NO_ACTION_FOUND;
          }
          LOG.log(Level.FINER, "Response: {0}", response);
          responseBody.write(response.getBytes());
          responseBody.close();
        }
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Error: {0}", e);
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
  private String processQuery(String host, String action, String queryString) {
    // Set the host field. Used by the help command. Find a better way to to do this?
    //commandModule.setHTTPHost(host);
    // Convert the URI into a JSONObject, stuffing in some extra relevant fields like
    // the signature, and the message signed.
    String fullString = action + QUERYPREFIX + queryString; // for signature check
    Map<String, String> queryMap = Util.parseURIQueryString(queryString);
    //new command processing
    queryMap.put(COMMANDNAME, action);
    if (queryMap.keySet().contains(SIGNATURE)) {
      String signature = queryMap.get(SIGNATURE);
      String message = NSAccessSupport.removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature);
      queryMap.put(SIGNATUREFULLMESSAGE, message);
    }
    JSONObject jsonFormattedCommand = new JSONObject(queryMap);

    // Now we execute the command
    BasicCommand command = commandModule.lookupCommand(jsonFormattedCommand);
    return CommandHandler.executeCommand(command, jsonFormattedCommand, requestHandler).getReturnValue();
  }

  /**
   * Returns info about the server.
   */
  private class EchoHandler implements HttpHandler {

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
        String serverLocalNameServerID = "Local node address: " + requestHandler.getNodeAddress();
        String numberOfNameServers = "Server count: " + requestHandler.getGnsNodeConfig().getNumberOfNodes() + "\n";
        //String backingStoreClass = "Backing Store Class: " + Config.dataStore.getClassName() + "\n\n";
        //String requestsReceivedString = "Client requests received: " + requestHandler.getReceivedRequests();
        //String requestsRateString = "Client requests rate: " + requestHandler.getRequestsPerSecond();
        StringBuilder resultString = new StringBuilder();
        resultString.append("Servers:");
        for (String topLevelNode : requestHandler.getGnsNodeConfig().getNodeIDs()) {
          resultString.append("<br>&nbsp;&nbsp;");
          resultString.append(topLevelNode);
          resultString.append("&nbsp;=&gt;&nbsp;");
          resultString.append(requestHandler.getGnsNodeConfig().getBindAddress(topLevelNode));
          resultString.append("&nbsp;&nbspPublic IP:&nbsp;");
          resultString.append(requestHandler.getGnsNodeConfig().getNodeAddress(topLevelNode));
        }
        String nodeAddressesString = resultString.toString();
        resultString = new StringBuilder();
        String prefix = "";
        for (String recon : requestHandler.getGnsNodeConfig().getReconfigurators()) {
          resultString.append("<br>&nbsp;&nbsp;");
          //resultString.append(prefix);
          resultString.append(recon);
          resultString.append("&nbsp;=&gt;&nbsp;");
          //resultString.append("(");
          resultString.append(requestHandler.getGnsNodeConfig().getNodeAddress(recon).getHostName());
          resultString.append(":");
          resultString.append(requestHandler.getGnsNodeConfig().getNodePort(recon));
          //resultString.append(")");
          //prefix = ", ";
        }
        String reconfiguratorsString = "Reconfigurators: " + resultString.toString();
        resultString = new StringBuilder();
        prefix = "";
        for (String activeReplica : requestHandler.getGnsNodeConfig().getActiveReplicas()) {
          resultString.append("<br>&nbsp;&nbsp;");
          //resultString.append(prefix);
          resultString.append(activeReplica);
          resultString.append("&nbsp;=&gt;&nbsp;");
          //resultString.append("(");
          resultString.append(requestHandler.getGnsNodeConfig().getNodeAddress(activeReplica).getHostName());
          resultString.append(":");
          resultString.append(requestHandler.getGnsNodeConfig().getNodePort(activeReplica));
          //resultString.append(")");
          //prefix = ", ";
        }
        String activeReplicasString = "Active replicas: " + resultString.toString();
        String consoleLogLevelString = "Console log level is " + GNSConfig.getLogger().getLevel().getLocalizedName();
        String fileLogLevelString = "File log level is " + GNSConfig.getLogger().getLevel().getLocalizedName();

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
//        responseBody.write("<br>".getBytes());
//        responseBody.write(requestsReceivedString.getBytes());
//        responseBody.write("<br>".getBytes());
//        responseBody.write(requestsRateString.getBytes());
        responseBody.write("<br>".getBytes());

        responseBody.write("Gigapaxos is enabled<br>".getBytes());

        responseBody.write("New app is enabled<br>".getBytes());
        responseBody.write("Console logging level is ".getBytes());
        responseBody.write(consoleLogLevelString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write("File logging level is ".getBytes());
        responseBody.write(fileLogLevelString.getBytes());
        responseBody.write("<br>".getBytes());
        responseBody.write("<br>".getBytes());
        //responseBody.write(backingStoreClass.getBytes());
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

  /**
   * Return the port.
   *
   * @return an int
   */
  public int getPort() {
    return port;
  }

}
