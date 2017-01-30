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
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

import edu.umass.cs.gnscommon.ResponseCode;
import static edu.umass.cs.gnsserver.httpserver.Defs.QUERYPREFIX;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.main.GNSConfig.GNSC;
import edu.umass.cs.gnsserver.utils.Util;
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
 * @author westy
 */
public class GNSHttpServer extends GNSHttpProxy{


	/**
	 *
	 */
	protected final ClientRequestHandlerInterface requestHandler;

	private final static Logger LOGGER = Logger.getLogger(GNSHttpServer.class.getName());

	/**
	 *
	 * @param port
	 * @param requestHandler
	 */
	public GNSHttpServer(int port, ClientRequestHandlerInterface requestHandler) {
		super();
		this.requestHandler = requestHandler;
		if (Config.getGlobalBoolean(GNSC.DISABLE_MULTI_SERVER_HTTP)) {
			client.close();
			client = null;
		}
		runServer(port);
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
	protected class DefaultHttpHandler extends ProxyHttpHandler {


		/**
		 * Process queries for the http service. Converts the URI of e the HTTP query into
		 * the JSON Object format that is used by the CommandModeule class, then finds
		 * executes the matching command.
		 *
		 * @throws InternalRequestException
		 */
		@SuppressWarnings("unused") //This is called in the parent class (ProxyHttpHandler) in the handle method.
		private CommandResponse processQuery(String host, String commandName, String queryString) throws InternalRequestException {
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
				// Hair below is to handle some commands locally (creates, delets, selects, admin)
				// and the rest by invoking the GNS client and sending them out.
				// Client will be null if GNSC.DISABLE_MULTI_SERVER_HTTP (see above)
				// is true (or there was a problem).
				if (client == null || commandType.isLocallyHandled()) {
					// EXECUTE IT LOCALLY
					AbstractCommand command;
					try {
						command = commandModule.lookupCommand(commandType);
						// Do some work to get the signature and message into the command for
						// signature checking that happens later on. 
						// This only happens for local execution because remote handling (in the 
						// other side of the if) already does this.
						processSignature(jsonCommand);
						if (command != null) {
							return CommandHandler.executeCommand(command,
									new CommandPacket((long) (Math.random() * Long.MAX_VALUE), jsonCommand, false),
									requestHandler);
						}
						LOGGER.log(Level.FINE, "lookupCommand returned null for {0}", commandName);
					} catch (IllegalArgumentException e) {
						LOGGER.log(Level.FINE, "lookupCommand failed for {0}", commandName);
					}
					return new CommandResponse(ResponseCode.OPERATION_NOT_SUPPORTED,
							GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.OPERATION_NOT_SUPPORTED.toString()
							+ " Sorry, don't understand " + commandName + QUERYPREFIX + queryString);
				} else {
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
				}
			} catch (JSONException | UnsupportedEncodingException e) {
				return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " "
						+ GNSProtocol.UNSPECIFIED_ERROR.toString() + " " + e.toString());
			}
		}

		private void processSignature(JSONObject jsonCommand) throws JSONException {
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
}
