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

import edu.umass.cs.gnscommon.CommandType;

import java.io.IOException;
import java.util.List;

import edu.umass.cs.gnscommon.ResponseCode;
import static edu.umass.cs.gnsserver.httpserver.Defs.QUERYPREFIX;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.main.GNSConfig.GNSC;
import edu.umass.cs.utils.Config;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;


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
		echoHandler = new EchoHttpServerHandler();
		this.requestHandler = requestHandler;
		if (Config.getGlobalBoolean(GNSC.DISABLE_MULTI_SERVER_HTTP)) {
			client.close();
			client = null;
		}
		runServer(port);
	}




/**
 * Executes the command with the given information locally.
 * @param jsonCommand The JSONObject containing the command.
 * @param commandType The CommandType of the command.
 * @param commandName The name of the command.
 * @return
 * @throws JSONException 
 */
	protected CommandResponse handleCommandLocally(JSONObject jsonCommand, 
			CommandType commandType, String commandName, String queryString) throws JSONException{
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
	}



	/**
	 * Returns info about the server. This overrides the getAdditionalInformation method
	 * of its parent echoHttpProxyHandler in order to print the number of name servers
	 * in echo http replies.
	 */
	protected class EchoHttpServerHandler extends EchoHttpProxyHandler {

		/**
		 *
		 * @param exchange
		 * @throws IOException
		 */
		@Override
		protected List<String> getAdditionalInformation(){
			List<String> info = new ArrayList<String>();
			
			String numberOfNameServers = "Server count: " + requestHandler.getGnsNodeConfig().getNumberOfNodes();
			info.add(numberOfNameServers);
			
			return info;
		}
	
	}
}
