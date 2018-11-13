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
package edu.umass.cs.gnsclient.client.http;

/**
 * @author arun
 */

import com.sun.net.httpserver.*;
import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.EnvUtils;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport
	.CommandResponse;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.utils.Config;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.umass.cs.gnsserver.httpserver.Defs.QUERYPREFIX;

public class GNSHTTPProxy {

	/**
	 *
	 */
	protected static final String GNS_PATH = Config.getGlobalString(GNSConfig
		.GNSC.HTTP_SERVER_GNS_URL_PATH);
	private final boolean isProxyClient;
	private final HttpServer httpServer;
	// handles command processing
	private final GNSClient client;

	/**
	 *
	 */
	private final Date serverStartDate = new Date();

	private final static Logger LOGGER = Logger.getLogger(GNSHTTPProxy.class
		.getName());

	/**
	 * @param port
	 */
	public GNSHTTPProxy(int port, boolean isProxyClient, String
		clientKeyDBDir) throws IOException {
		if (isProxyClient) {
			ensureDirExists(clientKeyDBDir);
		} this.isProxyClient = isProxyClient; this.client = new GNSClient() {
			@Override
			public String getLabel() {
				return GNSHTTPProxy.class.getSimpleName();
			}
		}; this.httpServer = tryPort(port);
	}

	public GNSHTTPProxy(int port, boolean isProxyClient) throws IOException {
		this(port, isProxyClient, EnvUtils.GNS_HOME);
	}

	public GNSHTTPProxy(int port) throws IOException {
		this(port, false);
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
	public HttpServer tryPort(int port) throws IOException {
		try {
			InetSocketAddress addr = new InetSocketAddress(port);
			HttpServer server = HttpServer.create(addr, 0);

			server.createContext("/", new EchoHttpHandler());
			server.createContext("/" + GNS_PATH, new DefaultHttpHandler());
			server.setExecutor(Executors.newCachedThreadPool()); server
				.start();
			LOGGER.log(Level.INFO, "HTTP server is listening on port {0}",
				port);
			return server;
		} catch (IOException e) {
			LOGGER.log(Level.WARNING, "HTTP server failed to start on port " +
				"{0}" + " " + "due to {1}", new Object[]{port, e.getMessage
				()});
			throw e;
		}
	}

	/**
	 * The default handler.
	 */
	protected class DefaultHttpHandler implements HttpHandler {

		/**
		 * @param exchange
		 */
		@Override
		public void handle(HttpExchange exchange) {
			CommandResponse response = null;

			try {
				String requestMethod = exchange.getRequestMethod();
				if (requestMethod.equalsIgnoreCase("GET")) {
					Headers requestHeaders = exchange.getRequestHeaders();
					String host = requestHeaders.getFirst("Host");
					Headers responseHeaders = exchange.getResponseHeaders();
					responseHeaders.set("Content-Type", "text/plain");
					if (Config.getGlobalBoolean(GNSClientConfig.GNSCC
						.ENABLE_CROSS_ORIGIN_REQUESTS)) {
						responseHeaders.set("Access-Control-Allow-Origin",
							"*");
					}

					int statusCode = HttpURLConnection.HTTP_OK; //default

					try (OutputStream responseBody = exchange.getResponseBody
						()) {
						URI uri = exchange.getRequestURI();
						LOGGER.log(Level.FINE, "HTTP SERVER REQUEST FROM {0}: " +
							"" + "" + "" + "" + "" + "" + "" + "" + "{1}", new
							Object[]{exchange.getRemoteAddress().getHostName()
							, uri.toString()});
						String path = uri.getPath();
						String query = uri.getQuery() != null ? uri.getQuery()
							: "";
						String commandName = path.replaceFirst("/" + GNS_PATH
							+ "/", "");


						if (!commandName.isEmpty()) {
							LOGGER.log(Level.FINE, "Action: {0} Query:{1}",
								new Object[]{commandName, query});

							// process commandName query
							response = processQuery(host, commandName, query,
								exchange instanceof HttpsExchange);

							if (response.getExceptionOrErrorCode() !=
								ResponseCode.NO_ERROR)
								statusCode = translateToHTTPStatusCode
									(response.getExceptionOrErrorCode());
						}
						else {
							statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
							response = new CommandResponse(ResponseCode
								.OPERATION_NOT_SUPPORTED, GNSProtocol
								.BAD_RESPONSE.toString() + " " + GNSProtocol
								.OPERATION_NOT_SUPPORTED.toString() + " Don't " +
								"" + "" + "" + "" + "" + "" + "understand" + "" +
								" " + commandName + "" + " " + query);
						} LOGGER.log(Level.FINER, "Response: {0}", response);

						exchange.sendResponseHeaders(statusCode, 0);
						responseBody.write(response.getReturnValue().getBytes
							());
					}
				}
			} catch (IOException e) {
				LOGGER.log(Level.WARNING, "IOException: {0}", e.getMessage());
				e.printStackTrace();

				// FIXME: unclear why retrying write makes sense
				try {
					String defaultResponse = GNSProtocol.BAD_RESPONSE.toString
						() + " " + GNSProtocol.QUERY_PROCESSING_ERROR.toString
						() + " " + e;
					OutputStream responseBody = exchange.getResponseBody();
					responseBody.write(response != null ? response
						.getReturnValue().getBytes() : defaultResponse
						.getBytes());
				} catch (Exception f) {
					// at this point screw it
				}
			}
		}
	}

	private static int translateToHTTPStatusCode(ResponseCode code) {
		// FIXME: systematically distinguish between client and server errors
		return HttpURLConnection.HTTP_BAD_REQUEST;
	}

	/*
	 * Process queries for the http service. Converts the URI of the HTTP
	 * query into
	 * the JSON Object format that is used by the CommandModeule class, then
	 * finds
	 * executes the matching command.
	 *
	 * @throws InternalRequestException
	 */
	private CommandResponse processQuery(String host, String commandName,
										 String queryString, boolean
											 secureServer) {

		// Convert the URI into a JSONObject, stuffing in some extra relevant
		// fields like
		// the signature, and the message signed.
		try {
			// Note that the commandName is not part of the queryString string
			// here so
			// it doesn't end up in the jsonCommand. Also see below where we
			// put the
			// command integer into the jsonCommand.
			JSONObject jsonCommand = Util.parseURIQueryStringIntoJSONObject
				(queryString);
			// If the signature exists it is Base64 encoded so decode it now.
			if (jsonCommand.has(GNSProtocol.SIGNATURE.toString())) {
				jsonCommand.put(GNSProtocol.SIGNATURE.toString(), new String
					(Base64.decode(jsonCommand.getString(GNSProtocol.SIGNATURE
						.toString())), GNSProtocol.CHARSET.toString()));
			}
			// getCommandForHttp allows for "dump" as well as "Dump"
			CommandType commandType = CommandType.getCommandForHttp
				(commandName);
			if (commandType == null) {
				return new CommandResponse(ResponseCode
					.OPERATION_NOT_SUPPORTED, GNSProtocol.BAD_RESPONSE
					.toString() + " " + GNSProtocol.OPERATION_NOT_SUPPORTED
					.toString() + " Sorry, don't " + "understand " +
					commandName + QUERYPREFIX + queryString);
			}

			//Only allow mutual auth commands if we're on a secure (HTTPS)
			// server
			if (commandType.isMutualAuth() && !secureServer) {
				return new CommandResponse(ResponseCode
					.OPERATION_NOT_SUPPORTED, GNSProtocol.BAD_RESPONSE
					.toString() + " " + GNSProtocol.OPERATION_NOT_SUPPORTED
					.toString() + " Not " + "authorized to execute " +
					commandName + QUERYPREFIX + queryString);
			}

			// The client currently just uses the command name (which is not
			// part of the
			// query string above) so we need to stuff
			// in the Command integer for the signature check and execution.
			jsonCommand.put(GNSProtocol.COMMAND_INT.toString(), commandType
				.getInt()).put(GNSProtocol.COMMANDNAME.toString(), commandType
				.toString());
			// Optionally does some sanity checking on the message if that was
			// enabled at the client.
			// This makes necessary changes to the jsonCommand so don't remove
			// this call
			// unless you know what you're doing and also change the code in
			// the HTTP client.
			sanityCheckMessage(jsonCommand);
			// Hair below is to handle some commands locally (creates, delets,
			// selects, admin)
			// and the rest by invoking the GNS client and sending them out.
			// Client will be null if GNSC.DISABLE_MULTI_SERVER_HTTP (see
			// above)
			// is true (or there was a problem).
			return defaultHandleCommand(host, commandName, queryString,
				jsonCommand, secureServer);
		}
		// FIXME: translate to appropriate HTTP + GNS response code
		catch (JSONException | UnsupportedEncodingException |
			NoSuchAlgorithmException e) {
			return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR,
				GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol
					.UNSPECIFIED_ERROR.toString() + " " + e.toString());
		}
	}

	/**
	 * Implementations may extend this method.
	 *
	 * @param host
	 * @param commandName
	 * @param queryString
	 * @param jsonCommand
	 * @param secureServer
	 * @return
	 * @throws JSONException
	 */
	protected CommandResponse defaultHandleCommand(String host, String
		commandName, String queryString, JSONObject jsonCommand, boolean
		secureServer) throws JSONException, NoSuchAlgorithmException {
		// Send the command remotely using a client
		try {
			LOGGER.log(Level.FINE, "Sending command out to a remote " +
				"server: {0}", jsonCommand);
			CommandPacket commandResponsePacket = getResponseUsingGNSClient
				(client, jsonCommand);
			return new CommandResponse(ResponseCode.NO_ERROR,
				// Some crap here to make single field reads
				// return just the value for backward compatibility
				// There is similar code to this other places.
				specialCaseSingleFieldRead(commandResponsePacket
					.getResultString(), CommandType.getCommandType(jsonCommand
					.getInt(GNSProtocol.COMMAND_INT.toString())),
					jsonCommand));
		} catch (IOException | ClientException e) {
			return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR,
				GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol
					.UNSPECIFIED_ERROR.toString() + " " + e.toString());
		}
	}

	private static void sanityCheckMessage(JSONObject jsonCommand) throws
		JSONException, UnsupportedEncodingException {
		if (jsonCommand.has(GNSProtocol.ORIGINAL_MESSAGE_BASE64.toString())) {
			String originalMessage = new String(Base64.decode(jsonCommand
				.getString(GNSProtocol.ORIGINAL_MESSAGE_BASE64.toString())), GNSProtocol.CHARSET
				.toString());
			jsonCommand.remove(GNSProtocol.ORIGINAL_MESSAGE_BASE64.toString());
			String commandSansSignature = CanonicalJSON.getCanonicalForm
				(jsonCommand);
			if (!originalMessage.equals(commandSansSignature)) {
				LOGGER.log(Level.SEVERE, "signature message mismatch! " +
					"original: {0} computed for signature: {1}", new
					Object[]{originalMessage, commandSansSignature});
			}
			else {
				LOGGER.log(Level.FINE, "######## original: {0}",
					originalMessage);
			}
		}
	}

	private static void processSignature(JSONObject jsonCommand) throws
		JSONException {
		if (jsonCommand.has(GNSProtocol.SIGNATURE.toString())) {
			// Squirrel away the signature.
			String signature = jsonCommand.getString(GNSProtocol.SIGNATURE
				.toString());
			// Pull it out of the command because we don't want to have it
			// there when we check the message.
			jsonCommand.remove(GNSProtocol.SIGNATURE.toString());
			// Convert it to a conanical string (the message) that we can use
			// later to check against the signature.
			String commandSansSignature = CanonicalJSON.getCanonicalForm
				(jsonCommand);
			// Put the decoded signature back as well as the message that
			// we're going to
			// later compare the signature against.
			jsonCommand.put(GNSProtocol.SIGNATURE.toString(), signature).put
				(GNSProtocol.SIGNATUREFULLMESSAGE.toString(),
					commandSansSignature);

		}
	}

	//make single field reads return just the value for backward compatibility
	private static String specialCaseSingleFieldRead(String response,
													 CommandType commandType,
													 JSONObject
														 jsonFormattedArguments) {
		try {
			if (commandType.isRead() && jsonFormattedArguments.has(GNSProtocol
				.FIELD.toString()) && !jsonFormattedArguments.getString
				(GNSProtocol.FIELD.toString()).equals(GNSProtocol
				.ENTIRE_RECORD.toString()) && JSONPacket.couldBeJSON(response)
				&& response.startsWith("{")) {
				String key = jsonFormattedArguments.getString(GNSProtocol
					.FIELD.toString());
				JSONObject json = new JSONObject(response);
				return json.getString(key);
			}
		} catch (JSONException e) {
			LOGGER.log(Level.WARNING, "Problem getting single key reponse for " +
				"" + "" + ":" + " {0}", e.getMessage());
			// just return the response if there is some issue
		} return response;
	}

	private CommandPacket getResponseUsingGNSClient(GNSClient client,
													JSONObject
														jsonFormattedArguments) throws ClientException, IOException, JSONException, NoSuchAlgorithmException {
		CommandType type = CommandType.getCommandType(jsonFormattedArguments
			.getInt(GNSProtocol.COMMAND_INT.toString()));
		GuidEntry querier = getQuerier(client, type, jsonFormattedArguments);

		CommandPacket outgoingPacket = this.isProxyClient ?
			// sign command
			GNSCommand.getCommand(type, querier, getKeysAndValuesAsArray
				(addMissingParams(client, querier, type,
					jsonFormattedArguments)))

			:

			GNSCommand.getCommand(type, querier, jsonFormattedArguments);


		LOGGER.log(Level.FINE, "{0} sending request {1}", new Object[]{this,
			outgoingPacket});
		CommandPacket returnPacket = client.execute(outgoingPacket);
		LOGGER.log(Level.FINE, "{0} received response {1}", new
			Object[]{this, returnPacket});
		/**
		 * Can also invoke getResponse(), getResponseString(),
		 * getResponseJSONObject()
		 * etc. on {@link CommandPacket} as documented in {@link GNSCommand}.
		 */
		return returnPacket;

	}

	public String toString() {
		return this.getClass().getSimpleName();
	}

	private static JSONObject addMissingParams(GNSClient client, GuidEntry
		querier, CommandType type, JSONObject jsonFormattedArguments) throws JSONException, ClientException, NoSuchAlgorithmException {
		//if (querier == null) return jsonFormattedArguments;

		/**
		 * 	Add missing arguments like public key for account or GUID
		 * 	creation; default field for read, etc.
		 */
		switch (type) {
			case RegisterAccount:
			case RegisterAccountSecured:
				jsonFormattedArguments.put(GNSProtocol.PUBLIC_KEY.toString(),
					querier.getPublicKeyString());
				break;
			case AddMultipleGuids:
				JSONArray names = jsonFormattedArguments.getJSONArray
					(GNSProtocol.NAMES.toString());
				ArrayList<String> publicKeys = new ArrayList<String>();
				for (int i = 0; i < names.length(); i++) {
					GuidEntry entry = GuidUtils.lookupOrCreateGuidEntry(
						(String) names.get(i), client.getGNSProvider());
					publicKeys.add(entry.getPublicKeyString());
				}
				jsonFormattedArguments.put(GNSProtocol.PUBLIC_KEYS.toString(),
					publicKeys);
				break;
			case Read:
				if(!jsonFormattedArguments.has(GNSProtocol.FIELD.toString()))
					jsonFormattedArguments.put(GNSProtocol.FIELD.toString(),
						GNSProtocol.ENTIRE_RECORD.toString());
			default:
		}

		// if command requires and lacks signature, insert signature
		if (type.requiresSignature() && !jsonFormattedArguments.has
			(GNSProtocol.SIGNATURE.toString()) && querier != null) {
			if (type.isRead() || type.isSelect())
				jsonFormattedArguments.put(GNSProtocol.READER.toString(),
					querier.getGuid());
			else if(type.isUpdate())
				jsonFormattedArguments.put(GNSProtocol.WRITER.toString(),
					querier.getGuid());
		}

		return jsonFormattedArguments;
	}


	private static boolean allRequiredParamsPresent(CommandType type,
													JSONObject
														jsonFormattedArguments) throws JSONException {
		for (String param : type.getCommandRequiredParameters())
			if (!jsonFormattedArguments.has(param)) return false; return true;
	}

	private static CommandType getCommandType(JSONObject json) throws
		JSONException {
		return CommandType.getCommandType(json.getInt(GNSProtocol.COMMAND_INT
			.toString()));
	}

	private static GuidEntry getImplicitSigner(GNSClient client, CommandType
		type, JSONObject jsonFormattedArguments) throws JSONException,
		EncryptionException, NoSuchAlgorithmException {
		GuidEntry querierGUIDEntry = null;
		if (querierGUIDEntry==null)
			querierGUIDEntry = getGUIDEntry(client,
				jsonFormattedArguments, GNSProtocol.READER.toString());
		if (querierGUIDEntry==null)
			querierGUIDEntry = getGUIDEntry(client,
				jsonFormattedArguments, GNSProtocol.WRITER.toString());

//		if (type.isCreateDelete())
			switch (type) {
				case RegisterAccount:
				case RegisterAccountSecured:
				case RegisterAccountWithCertificate:
					// keypair creation necessary
					querierGUIDEntry = GuidUtils.lookupOrCreateGuidEntry
						(jsonFormattedArguments.getString(GNSProtocol.NAME
							.toString()), client.getGNSProvider());
					break;

				default:
					querierGUIDEntry = getGUIDEntry(client,
					 jsonFormattedArguments, GNSProtocol.GUID
							.toString());
			}
		return querierGUIDEntry;
	}

	private static GuidEntry getGUIDEntry(GNSClient client,
														  JSONObject json,
														  String querierKey)
		throws JSONException {
		if (!json.has(querierKey)) return null;
		// else
		String name = KeyPairUtils.getName(client.getGNSProvider
			(), json.getString(querierKey));
		GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, name);
		return entry;
	}


	private static GuidEntry getQuerier(GNSClient client, CommandType type,
										JSONObject jsonFormattedArguments)
		throws JSONException, EncryptionException, NoSuchAlgorithmException {
		String querier = null;
		// no-op if all required arguments are present
		return allRequiredParamsPresent(type, jsonFormattedArguments) ? null :
			// else implicit signer GUID
			getImplicitSigner(client, type, jsonFormattedArguments);
	}

	/**
	 * Converts a JSONObject to an array of key-value pairs wherein every
	 * odd element is a key and the successive even element is the
	 * corresponding value.
	 *
	 * @param json
	 * @return
	 * @throws JSONException
	 */
	public static Object[] getKeysAndValuesAsArray(JSONObject json) throws
		JSONException {
		ArrayList<Object> arrayList = new ArrayList<Object>();
		JSONArray jsonArray = json.names();
		for (int i = 0; i < jsonArray.length(); i++) {
			String key = (String) jsonArray.get(i); arrayList.add(key);
			arrayList.add(json.get(key));
		} return arrayList.toArray();
	}

	/**
	 * Returns info about the server.
	 */
	protected class EchoHttpHandler implements HttpHandler {

		/**
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
					String buildVersionInfo = "Build Version: Unable to " +
						"lookup!";
					if (buildVersion != null) {
						buildVersionInfo = "Build Version: " + buildVersion;
					}
					String responsePreamble = "<html><head><title>GNS Server "
						+ "Status</title></head><body><p>";
					String responsePostamble = "</p></body></html>";
					String serverStartDateString = "Server start time: " +
						Format.formatDualDate(serverStartDate);
					String serverUpTimeString = "Server uptime: " +
						DurationFormatUtils.formatDurationWords(new Date()
							.getTime() - serverStartDate.getTime(), true,
							true);
					String serverSSLMode = "Server SSL mode: " +
						ReconfigurationConfig.getServerSSLMode().toString();
					String clientSSLMode = "Client SSL mode: " +
						ReconfigurationConfig.getClientSSLMode().toString();
					String reconAddresses = "Recon addresses: " +
						ReconfigurationConfig.getReconfiguratorAddresses()
							.toString();
//          String numberOfNameServers = "Server count: " + requestHandler
// .getGnsNodeConfig().getNumberOfNodes();
					String recordsClass = "Records Class: " + GNSConfig.GNSC
						.getNoSqlRecordsClass();
					String secureString = exchange instanceof HttpsExchange ?
						"Security: Secure" : "Security: Open";
					// Build the response
					responseBody.write(responsePreamble.getBytes());
					responseBody.write(buildVersionInfo.getBytes());
					responseBody.write("<br>".getBytes());
					responseBody.write(serverStartDateString.getBytes());
					responseBody.write("<br>".getBytes());
					responseBody.write(serverUpTimeString.getBytes());
					responseBody.write("<br>".getBytes());
//          responseBody.write(numberOfNameServers.getBytes());
					responseBody.write("<br>".getBytes());
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
					} responseBody.write(responsePostamble.getBytes());
				}
			}
		}
	}

	private static String ensureDirExists(String dir) {
		new File(dir).mkdirs(); return dir;
	}

	private static void processArgs(String[] args) {

	}

	public static void main(String[] args) throws IOException {
		new GNSHTTPProxy(5678, true);
	}
}
