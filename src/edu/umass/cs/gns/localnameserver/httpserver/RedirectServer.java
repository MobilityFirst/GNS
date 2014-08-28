package edu.umass.cs.gns.localnameserver.httpserver;

/**
 *
 * @author westy
 */
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.umass.cs.gns.main.GNS;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;

/**
 *
 * 
 * @author westy
 */
public class RedirectServer {

  public static final int port = 80;
  public static final String newURl = "http://gnrs.name";

  public static void runServer() {
    try {
      InetSocketAddress addr = new InetSocketAddress(port);
      HttpServer server = HttpServer.create(addr, 0);
      server.createContext("/", new RedirectHandler());
      server.setExecutor(Executors.newCachedThreadPool());
      server.start();
      GNS.getLogger().info("HTTP redirect server is listening on port " + port);
    } catch (IOException e) {
      GNS.getLogger().info("HTTP redirect server failed to start on port " + port + " due to " + e);
    }
  }

  private static class RedirectHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) {
      try {
        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set("Location", newURl);
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_MOVED_PERM, 0);
        OutputStream os = exchange.getResponseBody();
        os.write("".getBytes());
        os.close();
      } catch (Exception e) {
        GNS.getLogger().info("HTTP redirect server had problem redirecting: " + e);
      }
    }
  }

  private static class EchoRequestHeadersHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange xchg) throws IOException {
      Headers headers = xchg.getRequestHeaders();
      Set<Map.Entry<String, List<String>>> entries = headers.entrySet();

      StringBuilder response = new StringBuilder();
      for (Map.Entry<String, List<String>> entry : entries) {
        response.append(entry.toString() + "\n");
      }

      xchg.sendResponseHeaders(200, response.length());
      OutputStream os = xchg.getResponseBody();
      os.write(response.toString().getBytes());
      os.close();
    }
  }

  // Typical use: java -cp GNS.jar edu.umass.cs.gns.httpserver.RedirectServer
  public static void main(String[] args) throws IOException {
    runServer();
  }
  public static String Version = "$Revision$";
}