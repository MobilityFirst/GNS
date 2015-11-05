/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */

package edu.umass.cs.gigapaxos;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;

/**
 * @author arun
 *
 */
public class PaxosServer {
	private final SSLMessenger<String, JSONObject> messenger;

	PaxosServer(String myID, NodeConfig<String> nodeConfig)
			throws IOException {
		this.messenger = (new JSONMessenger<String>(
				(new MessageNIOTransport<String, JSONObject>(myID, nodeConfig,
						ReconfigurationConfig.getServerSSLMode()))));
		Replicable app = this.createApp();
		PaxosManager<String> pm = startPaxosManager(this.messenger, app,
				new InetSocketAddress(nodeConfig.getNodeAddress(myID),
						nodeConfig.getNodePort(myID)));

		// create default paxos group with same name as app class
		pm.createPaxosInstance(app.getClass().getSimpleName() + "0",
				nodeConfig.getNodeIDs(), null);
	}

	private PaxosManager<String> startPaxosManager(
			Messenger<String, JSONObject> messenger,
			Replicable app, InetSocketAddress myAddress) {
		return new PaxosManager<String>(messenger.getMyID(),
				PaxosConfig.getDefaultNodeConfig(), this.messenger, app, null,
				true).initClientMessenger(myAddress);
	}

	protected static Set<InetSocketAddress> getDefaultServers() {
		Set<InetSocketAddress> servers = new HashSet<InetSocketAddress>();
		NodeConfig<String> nodeConfig = PaxosConfig
				.getDefaultNodeConfig();
		for (String id : nodeConfig.getNodeIDs())
			servers.add(new InetSocketAddress(nodeConfig.getNodeAddress(id),
					nodeConfig.getNodePort(id)));
		return servers;
	}

	private Replicable createApp() {
		Replicable curApp = null;
		if (PaxosConfig.application != null) {
			try {

				curApp = (Replicable) PaxosConfig.application
						.getConstructor().newInstance();
			} catch (InstantiationException | IllegalAccessException
					| IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e) {
				PaxosManager.getLogger().severe(
						"App must support a constructor with no arguments");
				System.exit(1);
			}
		} else {
			PaxosManager.getLogger().severe(
					"Node" + messenger.getMyID()
							+ " unable to create paxos application replica");
		}
		if (curApp instanceof ClientMessenger) {
			((ClientMessenger) curApp).setClientMessenger(messenger);
		} 
		return curApp;
	}

	private static Set<String> processArgs(String[] args,
			NodeConfig<String> nodeConfig) {
		// -c options => start with clean DB
		for (String arg : args)
			if (arg.trim().equals("-c"))
				PaxosManager.startWithCleanDB(true);
		// server names must be at the end of args
		Set<String> servers = new HashSet<String>();
		for (int i = args.length - 1; i >= 0; i--)
			if (nodeConfig.nodeExists(args[i]))
				servers.add(args[i]);
		return servers;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if (args.length == 0)
			throw new RuntimeException(
					"At least one node ID must be specified as a command-line argument for starting "
							+ PaxosServer.class.getSimpleName());
		PaxosConfig.setConsoleHandler();
		NodeConfig<String> nodeConfig = PaxosConfig
				.getDefaultNodeConfig();
		System.out.print("Starting paxos servers [ ");
		for (String server : processArgs(args, nodeConfig)) {
			new PaxosServer(server, nodeConfig);
			System.out.print(server + " ");
		}
		System.out.println("] done");
	}
}
