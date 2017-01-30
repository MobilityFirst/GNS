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
import edu.umass.cs.gnscommon.CommandType;
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

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
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

import edu.umass.cs.gnscommon.GNSProtocol;

import java.io.UnsupportedEncodingException;

/**
 *
 *
 * @author Brendan
 * Created by refactoring Westy's GNSHttpServer into a parent class, so most of the code was
 * written by him for that class.
 */
public class GNSHttpProxy {

	/**
	 *
	 */
	protected static final String GNS_PATH = Config.getGlobalString(GNSConfig.GNSC.HTTP_SERVER_GNS_URL_PATH);
	protected HttpServer httpServer = null;
	protected int port;
	// handles command processing
	protected final CommandModule commandModule;
	// newer handles command processing
	protected GNSClient client = null;

	/**
	 *
	 */
	final protected Date serverStartDate = new Date();

	private final static Logger LOGGER = Logger.getLogger(GNSHttpProxy.class.getName());

	/**
	 * This does not call runServer on construction because GNSHttpServer may 
	 * set a requestHandler first. runServer must be explicitly called later!
	 * @param requestHandler
	 */
	public GNSHttpProxy() {
		this.commandModule = new CommandModule();
		try {
			this.client = new GNSClient() {
				public String getLabel() {
					return GNSHttpProxy.class.getSimpleName();
				}
			};
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Unable to start GNS client:" + e);
		}
	}
	
	/**
	 * Creates and runs a GNSHttpProxy that receives commands over HTTP
	 * and executed them using a GNSClient.
	 */
	public static void main(String[] args){
		GNSHttpProxy proxy = new GNSHttpProxy();
		proxy.runServer(8080);
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
			edu.umass.cs.utils.Util.suicide(GNSConfig.getLogger(), "Unable to start GNS HTTP server; exiting");
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
			httpServer.createContext("/" + GNS_PATH, new ProxyHttpHandler());
			httpServer.setExecutor(Executors.newCachedThreadPool());
			httpServer.start();

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
	protected class ProxyHttpHandler implements HttpHandler {

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
						response = new CommandResponse(ResponseCode.OPERATION_NOT_SUPPORTED, GNSProtocol.BAD_RESPONSE.toString()
								+ " " + GNSProtocol.OPERATION_NOT_SUPPORTED.toString() + " Don't understand " + commandName + " " + query);
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
					String response = GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.QUERY_PROCESSING_ERROR.toString() + " " + e;
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
	 *
	 * @throws InternalRequestException
	 */
	protected CommandResponse processQuery(String host, String commandName, String queryString) throws InternalRequestException {
		// Convert the URI into a JSONObject, stuffing in some extra relevant fields like
		// the signature, and the message signed.
		try {
			// Note that the commandName is not part of the queryString string here so
			// it doesn't end up in the jsonCommand. Also see below where we put the 
			// command integer into the jsonCommand.
			JSONObject jsonCommand = Util.parseURIQueryStringIntoJSONObject(queryString);
			// If the signature exists it is Base64 encoded so decode it now.
			if (jsonCommand.has(GNSProtocol.SIGNATURE.toString())) {
				jsonCommand.put(GNSProtocol.SIGNATURE.toString(),
						new String(Base64.decode(jsonCommand.getString(GNSProtocol.SIGNATURE.toString())),
								GNSProtocol.CHARSET.toString()));
			}
			// getCommandForHttp allows for "dump" as well as "Dump"
			CommandType commandType = CommandType.getCommandForHttp(commandName);
			if (commandType == null) {
				return new CommandResponse(ResponseCode.OPERATION_NOT_SUPPORTED,
						GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.OPERATION_NOT_SUPPORTED.toString()
						+ " Sorry, don't understand " + commandName + QUERYPREFIX + queryString);
			}
			// The client currently just uses the command name (which is not part of the
			// query string above) so we need to stuff 
			// in the Command integer for the signature check and execution.
			jsonCommand.put(GNSProtocol.COMMAND_INT.toString(), commandType.getInt());
			// Optionally does some sanity checking on the message if that was enabled at the client.
			// This makes necessary changes to the jsonCommand so don't remove this call
			// unless you know what you're doing and also change the code in the HTTP client.
			sanityCheckMessage(jsonCommand);
			// This proxy invokes the GNS client to send commands out.


			// Send the command remotely using a client
			try {
				LOGGER.log(Level.FINE, "Sending command out to a remote server: " + jsonCommand);
				CommandPacket commandResponsePacket = getResponseUsingGNSClient(client, jsonCommand);
				return new CommandResponse(ResponseCode.NO_ERROR,
						// Some crap here to make single field reads return just the value for backward compatibility 
						// There is similar code to this other places.
						specialCaseSingleFieldRead(commandResponsePacket.getResultString(),
								commandType, jsonCommand));
			} catch (IOException | ClientException e) {
				return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " "
						+ GNSProtocol.UNSPECIFIED_ERROR.toString() + " " + e.toString());
				//      } catch (ClientException e) {
				//        return new CommandResponse(ResponseCode.GNSProtocol.UNSPECIFIED_ERROR.toString(),
				//                GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.OPERATION_NOT_SUPPORTED.toString()
				//                + " Sorry, don't understand " + commandName + QUERYPREFIX + queryString);
			}

		} catch (JSONException | UnsupportedEncodingException e) {
			return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " "
					+ GNSProtocol.UNSPECIFIED_ERROR.toString() + " " + e.toString());
		}
	}

	protected static void sanityCheckMessage(JSONObject jsonCommand) throws JSONException,
	UnsupportedEncodingException {
		if (jsonCommand.has("originalMessageBase64")) {
			String originalMessage = new String(Base64.decode(jsonCommand.getString("originalMessageBase64")),
					GNSProtocol.CHARSET.toString());
			jsonCommand.remove("originalMessageBase64");
			String commandSansSignature = CanonicalJSON.getCanonicalForm(jsonCommand);
			if (!originalMessage.equals(commandSansSignature)) {
				LOGGER.log(Level.SEVERE, "signature message mismatch! original: " + originalMessage
						+ " computed for signature: " + commandSansSignature);
			} else {
				LOGGER.log(Level.FINE, "######## original: " + originalMessage);
			}
		}
	}


	//make single field reads return just the value for backward compatibility 
	protected static String specialCaseSingleFieldRead(String response, CommandType commandType,
			JSONObject jsonFormattedArguments) {
		try {
			if (commandType.isRead() && jsonFormattedArguments.has(GNSProtocol.FIELD.toString())
					&& !jsonFormattedArguments.getString(GNSProtocol.FIELD.toString()).equals(GNSProtocol.ENTIRE_RECORD.toString())
					&& JSONPacket.couldBeJSON(response) && response.startsWith("{")) {
				String key = jsonFormattedArguments.getString(GNSProtocol.FIELD.toString());
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

		CommandPacket outgoingPacket = new CommandPacket((long) (Math.random() * Long.MAX_VALUE), jsonFormattedArguments, false);
		//GNSCommand.createGNSCommandFromJSONObject(jsonFormattedArguments);

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

				String recordsClass = "Records Class: " + GNSConfig.GNSC.getNoSqlRecordsClass();

				// Build the response
				responseBody.write(responsePreamble.getBytes());
				responseBody.write(buildVersionInfo.getBytes());
				responseBody.write("<br>".getBytes());
				responseBody.write(serverStartDateString.getBytes());
				responseBody.write("<br>".getBytes());
				responseBody.write(serverUpTimeString.getBytes());
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
