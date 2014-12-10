package edu.umass.cs.gns.localnameserver.httpserver;

/**
 *
 * @author westy
 */
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.umass.cs.gns.clientsupport.*;
import edu.umass.cs.gns.clientsupport.Defs;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.paxos.PaxosReplica;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.localnameserver.httpserver.Defs.KEYSEP;
import static edu.umass.cs.gns.localnameserver.httpserver.Defs.QUERYPREFIX;
import static edu.umass.cs.gns.localnameserver.httpserver.Defs.VALSEP;
import edu.umass.cs.gns.clientsupport.CommandRequest;
import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.Util;
import java.util.Map;
import org.json.JSONObject;

/**
 *
 * 
 * @author westy
 */
public class GnsHttpServer {
  
  private static final String GNSPATH = GNS.GNS_URL_PATH;
  private static final int port = 8080;
  //private static int localNameServerID;
  
  // handles command processing
  private static final CommandModule commandModule = new CommandModule();
  private static ClientRequestHandlerInterface requestHandler;

  public static void runHttp(ClientRequestHandlerInterface requestHandler) {
    GnsHttpServer.requestHandler = requestHandler;
    runServer();
  }

  public static void runServer() {
    int cnt = 0;
    do {
      if (tryPort(port + cnt)) {
        break;
      }
    } while (cnt++ < 100);
  }

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
          GNS.getLogger().info("HTTP SERVER REEQUEST FROM " + exchange.getRemoteAddress().getHostName() + ": " + uri.toString());
          String path = uri.getPath();
          String query = uri.getQuery() != null ? uri.getQuery() : ""; // stupidly it returns null for empty query

          String action = path.replaceFirst("/" + GNSPATH + "/", "");

          String response;
          if (!action.isEmpty()) {
            GNS.getLogger().fine("Action: " + action + " Query:" + query);
            response = processQuery(host, action, query);
          } else {
            response = Defs.BADRESPONSE + " " + Defs.NOACTIONFOUND;
          }
          GNS.getLogger().finer("Response: " + response);
          responseBody.write(response.getBytes());
          responseBody.close();
        }
      } catch (Exception e) {
        GNS.getLogger().severe("Error: " + e);
        e.printStackTrace();
        try {
          String response = Defs.BADRESPONSE + " " + Defs.QUERYPROCESSINGERROR + " " + e;
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
    return CommandRequest.executeCommand(command, jsonFormattedCommand, GnsHttpServer.requestHandler).getReturnValue();
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
        responseHeaders.set("Content-Type", "text/plain");
        exchange.sendResponseHeaders(200, 0);

        OutputStream responseBody = exchange.getResponseBody();
        Headers requestHeaders = exchange.getRequestHeaders();
        Set<String> keySet = requestHeaders.keySet();
        Iterator<String> iter = keySet.iterator();

        String buildVersion = GNS.readBuildVersion();
        String buildVersionInfo = "Unknown";
        if (buildVersion != null) {
          buildVersionInfo = "Build Version: " + buildVersion + "\n";
        }
        //
        String serverVersionInfo =
                "Server Version: "
                + Version.replaceFirst(Matcher.quoteReplacement("$Revision:"), "").replaceFirst(Matcher.quoteReplacement("$"), "") + "\n";
        String recordVersionInfo =
                "Field Access Version: "
                + FieldAccess.Version.replaceFirst(Matcher.quoteReplacement("$Revision:"), "").replaceFirst(Matcher.quoteReplacement("$"), "") + "\n";
        String accountVersionInfo =
                "Account Access Version: "
                + AccountAccess.Version.replaceFirst(Matcher.quoteReplacement("$Revision:"), "").replaceFirst(Matcher.quoteReplacement("$"), "") + "\n";
        String fieldMetadataVersionInfo =
                "Field Metadata Version: "
                + FieldMetaData.Version.replaceFirst(Matcher.quoteReplacement("$Revision:"), "").replaceFirst(Matcher.quoteReplacement("$"), "") + "\n";
        String groupsVersionInfo =
                "Groups Version: "
                + GroupAccess.Version.replaceFirst(Matcher.quoteReplacement("$Revision:"), "").replaceFirst(Matcher.quoteReplacement("$"), "") + "\n";
        String selectVersionInfo =
                "Select Version: "
                + SelectHandler.Version.replaceFirst(Matcher.quoteReplacement("$Revision:"), "").replaceFirst(Matcher.quoteReplacement("$"), "") + "\n";
        String mongoRecordsVersionInfo =
                "Mongo Records Version: "
                + MongoRecords.Version.replaceFirst(Matcher.quoteReplacement("$Revision:"), "").replaceFirst(Matcher.quoteReplacement("$"), "") + "\n";
        String paxosVersionInfo =
                "Paxos Replica Version: "
                + PaxosReplica.Version.replaceFirst(Matcher.quoteReplacement("$Revision:"), "").replaceFirst(Matcher.quoteReplacement("$"), "") + "\n";

        String serverLocalNameServerID = "\nLocal Name Server Address: " + GnsHttpServer.requestHandler.getNodeAddress() + "\n";
        String numberOfNameServers = "Name Server Count: " + GnsHttpServer.requestHandler.getGnsNodeConfig().getNumberOfNodes() + "\n";
        //String backingStoreClass = "Backing Store Class: " + Config.dataStore.getClassName() + "\n\n";

        responseBody.write(buildVersionInfo.getBytes());
        responseBody.write(serverVersionInfo.getBytes());
        responseBody.write(recordVersionInfo.getBytes());
        responseBody.write(accountVersionInfo.getBytes());
        responseBody.write(fieldMetadataVersionInfo.getBytes());
        responseBody.write(groupsVersionInfo.getBytes());
        responseBody.write(selectVersionInfo.getBytes());
        responseBody.write(mongoRecordsVersionInfo.getBytes());
        responseBody.write(paxosVersionInfo.getBytes());
        responseBody.write(serverLocalNameServerID.getBytes());
        responseBody.write(numberOfNameServers.getBytes());
        //responseBody.write(backingStoreClass.getBytes());
        while (iter.hasNext()) {
          String key = iter.next();
          List values = requestHeaders.get(key);
          String s = key + " = " + values.toString() + "\n";
          responseBody.write(s.getBytes());
        }
        responseBody.close();
      }
    }
  }
  

  public static String Version = "$Revision$";
}