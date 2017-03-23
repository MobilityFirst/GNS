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
import com.sun.net.httpserver.HttpsExchange;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig.GNSCC;
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

import java.util.ArrayList;
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
	protected DefaultHttpHandler httpHandler;
	protected EchoHttpProxyHandler echoHandler;
	//Does this proxy allow commands to be executed locally? (Should be false in this class, and true in GNSHttpServer.)
	protected boolean localExecution;

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
		this.localExecution = false;
		this.commandModule = new CommandModule();
		this.httpHandler = new DefaultHttpHandler();
		this.echoHandler = new EchoHttpProxyHandler();
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
		int port = Config.getGlobalInt(GNSCC.HTTP_PROXY_PORT);
		String hostname = Config.getGlobalString(GNSCC.HTTP_PROXY_INCOMING_HOSTNAME);
		

		GNSHttpProxy proxy = new GNSHttpProxy();
		proxy.runServer(port, hostname);
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
			if (tryPort(startingPort + cnt, null)) {
				port = startingPort + cnt;
				break;
			}
			edu.umass.cs.utils.Util.suicide(GNSConfig.getLogger(), "Unable to start GNS HTTP server; exiting");
		} while (cnt++ < 100);
	}
	
	/**
	 * Start the server.
	 *
	 * @param startingPort
	 */
	public final void runServer(int startingPort, String hostname) {
		int cnt = 0;
		do {
			// Find the first port after starting port that actually works.
			// Usually if 8080 is busy we can get 8081.
			if (tryPort(startingPort + cnt, hostname)) {
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
	public boolean tryPort(int port, String hostname) {
		try {
			InetSocketAddress addr;
			if (hostname == null){
				addr = new InetSocketAddress(port);
			}
			else{
				addr = new InetSocketAddress(hostname, port);
			}
			httpServer = HttpServer.create(addr, 0);

			httpServer.createContext("/", echoHandler);
			httpServer.createContext("/" + GNS_PATH, httpHandler);
			httpServer.setExecutor(Executors.newCachedThreadPool());
			httpServer.start();

			LOGGER.log(Level.INFO,
					"HTTP server is listening on port {0} for {1}", new Object[]{port, hostname});
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

					try (OutputStream responseBody = exchange.getResponseBody()) {
						URI uri = exchange.getRequestURI();
						LOGGER.log(Level.FINE,
								"HTTP SERVER REQUEST FROM {0}: {1}",
								new Object[]{exchange.getRemoteAddress().getHostName(), uri.toString()});
						String path = uri.getPath();
						String query = uri.getQuery() != null ? uri.getQuery() : ""; // stupidly it returns null for empty query

						String commandName = path.replaceFirst("/" + GNS_PATH + "/", "");

						CommandResponse response;
						if (!commandName.isEmpty()) {
							LOGGER.log(Level.FINE, "Action: {0} Query:{1}", new Object[]{commandName, query});
							boolean secureServer = exchange instanceof HttpsExchange;
							response = processQuery(host, commandName, query, secureServer);
						} else {
							response = new CommandResponse(ResponseCode.OPERATION_NOT_SUPPORTED, GNSProtocol.BAD_RESPONSE.toString()
									+ " " + GNSProtocol.OPERATION_NOT_SUPPORTED.toString() + " Don't understand " + commandName + " " + query);
						}
						LOGGER.log(Level.FINER, "Response: {0}", response);
						// FIXME: This totally ignores the error code.
						responseBody.write(response.getReturnValue().getBytes());
					}
				}
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, "Error: {0}", e.getMessage());
				e.printStackTrace();
				try {
					String response = GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.QUERY_PROCESSING_ERROR.toString() + " " + e;
					try (OutputStream responseBody = exchange.getResponseBody()) {
						responseBody.write(response.getBytes());
					}
				} catch (Exception f) {
					// at this point screw it
				}
			}
		}
	}
	
	/**
	 * In GNSHttpProxy this does nothing since it does not handle local commands.
	 * However, children of this class (such as GNSHttpServer)
	 * may override this in order to execute commands locally.
	 * 
	 */
	protected CommandResponse handleCommandLocally(JSONObject jsonCommand, 
			CommandType commandType, String commandName, String queryString) throws JSONException{
		throw new UnsupportedOperationException("GNSHttpProxy cannot execute commands locally.");
	}

	/**
	 * Process queries for the http service. Converts the URI of e the HTTP query into
	 * the JSON Object format that is used by the CommandModeule class, then finds
	 * executes the matching command.
	 *
	 * @throws InternalRequestException
	 */
	protected CommandResponse processQuery(String host, String commandName, String queryString, boolean secureServer) throws InternalRequestException {

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

			//Only allow mutual auth commands if we're on a secure (HTTPS) server
			if (commandType.isMutualAuth() && !secureServer) {
				return new CommandResponse(ResponseCode.OPERATION_NOT_SUPPORTED,
						GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.OPERATION_NOT_SUPPORTED.toString()
						+ " Not authorized to execute " + commandName + QUERYPREFIX + queryString);
			}

			// The client currently just uses the command name (which is not part of the
			// query string above) so we need to stuff 
			// in the Command integer for the signature check and execution.
			jsonCommand.put(GNSProtocol.COMMAND_INT.toString(), commandType.getInt());
			// Optionally does some sanity checking on the message if that was enabled at the client.
			// This makes necessary changes to the jsonCommand so don't remove this call
			// unless you know what you're doing and also change the code in the HTTP client.
			sanityCheckMessage(jsonCommand);
			// Hair below is to handle some commands locally (creates, delets, selects, admin)
			// and the rest by invoking the GNS client and sending them out.
			// Client will be null if GNSC.DISABLE_MULTI_SERVER_HTTP (see above)
			// is true (or there was a problem).
			if (client == null || (localExecution && commandType.isLocallyHandled())) {
				// EXECUTE IT LOCALLY
				return handleCommandLocally(jsonCommand, commandType, commandName, queryString);
			} else {
				// Send the command remotely using a client
				try {
					LOGGER.log(Level.FINE, "Sending command out to a remote server: {0}", jsonCommand);
					CommandPacket commandResponsePacket = getResponseUsingGNSClient(client, jsonCommand);
					return new CommandResponse(ResponseCode.NO_ERROR,
							// Some crap here to make single field reads return just the value for backward compatibility 
							// There is similar code to this other places.
							specialCaseSingleFieldRead(commandResponsePacket.getResultString(),
									commandType, jsonCommand));
				} catch (IOException | ClientException e) {
					return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " "
							+ GNSProtocol.UNSPECIFIED_ERROR.toString() + " " + e.toString());
				}
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

	protected CommandPacket getResponseUsingGNSClient(GNSClient client,
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
	
	protected static void processSignature(JSONObject jsonCommand) throws JSONException {
		if (jsonCommand.has(GNSProtocol.SIGNATURE.toString())) {
			// Squirrel away the signature.
			String signature = jsonCommand.getString(GNSProtocol.SIGNATURE.toString());
			// Pull it out of the command because we don't want to have it there when we check the message.
			jsonCommand.remove(GNSProtocol.SIGNATURE.toString());
			// Convert it to a conanical string (the message) that we can use later to check against the signature.
			String commandSansSignature = CanonicalJSON.getCanonicalForm(jsonCommand);
			// Put the decoded signature back as well as the message that we're going to
			// later compare the signature against.
			jsonCommand.put(GNSProtocol.SIGNATURE.toString(), signature).put(GNSProtocol.SIGNATUREFULLMESSAGE.toString(),
					commandSansSignature);

		}
	}

	/**
	 * Returns info about the server.
	 */
	protected class EchoHttpProxyHandler implements HttpHandler {
		
		/**
		 * Any strings in the list returned by this method will be printed in the echo response.
		 * This method may be overridden in children classes that wish to add more information to
		 * the response body.
		 * @return
		 */
		protected List<String> getAdditionalInformation(){
			List<String> info = new ArrayList<String>(0);
			
			//Put additional information here, or override this method in a subclass.
			
			return info;
		}

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

				try (OutputStream responseBody = exchange.getResponseBody()) {
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
					String secureString = exchange instanceof HttpsExchange ? "Security: Secure" : "Security: Open";
					// Build the response
					responseBody.write(responsePreamble.getBytes());
					responseBody.write(buildVersionInfo.getBytes());
					responseBody.write("<br>".getBytes());
					responseBody.write(serverStartDateString.getBytes());
					responseBody.write("<br>".getBytes());
					responseBody.write(serverUpTimeString.getBytes());
					responseBody.write("<br>".getBytes());
					
					//Get any additional (child class) information
					List<String> additonalInformation = getAdditionalInformation();
					
					for (String info : additonalInformation){
						responseBody.write(info.getBytes());
						responseBody.write("<br>".getBytes());
					}
					
					responseBody.write(reconAddresses.getBytes());
					responseBody.write("<br>".getBytes());
					responseBody.write(serverSSLMode.getBytes());
					responseBody.write("<br>".getBytes());
					responseBody.write(clientSSLMode.getBytes());
					responseBody.write("<br>".getBytes());
					responseBody.write(secureString.getBytes());
					responseBody.write("<br>".getBytes());
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

				}
			}
		}
	}
}
