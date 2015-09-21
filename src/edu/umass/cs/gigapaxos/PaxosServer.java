package edu.umass.cs.gigapaxos;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONObject;

import edu.umass.cs.nio.InterfaceMessenger;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.InterfaceSSLMessenger;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;

/**
 * @author arun
 *
 */
public class PaxosServer {
	private final InterfaceSSLMessenger<String, JSONObject> messenger;

	PaxosServer(String myID, InterfaceNodeConfig<String> nodeConfig)
			throws IOException {
		this.messenger = (new JSONMessenger<String>(
				(new MessageNIOTransport<String, JSONObject>(myID, nodeConfig,
						ReconfigurationConfig.getServerSSLMode()))));
		InterfaceReplicable app = this.createApp();
		PaxosManager<String> pm = startPaxosManager(this.messenger, app,
				new InetSocketAddress(nodeConfig.getNodeAddress(myID),
						nodeConfig.getNodePort(myID)));

		// create default paxos group with same name as app class
		pm.createPaxosInstance(app.getClass().getSimpleName() + "0",
				nodeConfig.getNodeIDs(), null);
	}

	private PaxosManager<String> startPaxosManager(
			InterfaceMessenger<String, JSONObject> messenger,
			InterfaceReplicable app, InetSocketAddress myAddress) {
		return new PaxosManager<String>(messenger.getMyID(),
				PaxosConfig.getDefaultNodeConfig(), this.messenger, app, null,
				true).initClientMessenger(myAddress);
	}

	protected static Set<InetSocketAddress> getDefaultServers() {
		Set<InetSocketAddress> servers = new HashSet<InetSocketAddress>();
		InterfaceNodeConfig<String> nodeConfig = PaxosConfig
				.getDefaultNodeConfig();
		for (String id : nodeConfig.getNodeIDs())
			servers.add(new InetSocketAddress(nodeConfig.getNodeAddress(id),
					nodeConfig.getNodePort(id)));
		return servers;
	}

	private InterfaceReplicable createApp() {
		InterfaceReplicable curApp = null;
		if (PaxosConfig.application != null) {
			try {

				curApp = (InterfaceReplicable) PaxosConfig.application
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
		if (curApp instanceof InterfaceClientMessenger) {
			((InterfaceClientMessenger) curApp).setClientMessenger(messenger);
		} 
		return curApp;
	}

	private static Set<String> processArgs(String[] args,
			InterfaceNodeConfig<String> nodeConfig) {
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
		InterfaceNodeConfig<String> nodeConfig = PaxosConfig
				.getDefaultNodeConfig();
		System.out.print("Starting paxos servers [ ");
		for (String server : processArgs(args, nodeConfig)) {
			new PaxosServer(server, nodeConfig);
			System.out.print(server + " ");
		}
		System.out.println("] done");
	}
}
