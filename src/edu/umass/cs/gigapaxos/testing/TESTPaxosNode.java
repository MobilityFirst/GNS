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
package edu.umass.cs.gigapaxos.testing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.deprecated.ReplicableDeprecated;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         The main server node class for gigapaxos testing.
 */
public class TESTPaxosNode {

	private final int myID;
	private PaxosManager<Integer> pm = null;
	private TESTPaxosApp app = null;

	// A server must have an id
	TESTPaxosNode(int id, NodeConfig<Integer> nc, boolean local) throws IOException {
		this.myID = id;
		pm = startPaxosManagerAndApp(id, nc, local);
		assert (pm != null);
	}

	/**
	 * @param id
	 * @throws IOException 
	 */
	public TESTPaxosNode(int id) throws IOException {
		this(id, TESTPaxosConfig.getNodeConfig(), true);
	}

	private PaxosManager<Integer> startPaxosManagerAndApp(int id, NodeConfig<Integer> nc, boolean local) {
		try {
			// shared between app and paxos manager only for testing
			JSONNIOTransport<Integer> niot = null;
			this.pm = new PaxosManager<Integer>(id, nc,
					(niot = new JSONNIOTransport<Integer>(id, nc,
							new PacketDemultiplexerDefault(), true)),
					(this.app = new TESTPaxosApp(niot)), null, true);
				pm.initClientMessenger(new InetSocketAddress(nc
						.getNodeAddress(myID), nc.getNodePort(myID)));
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return pm;
	}

	/**
	 * 
	 */
	public void close() {
		this.pm.close();
	}

	protected PaxosManager<Integer> getPaxosManager() {
		return pm;
	}

	protected String getAppState(String paxosID) {
		return app.checkpoint(paxosID);
	}

	public String toString() {
		String s = "[id=";
		s += this.myID + ", pm=" + pm + ", app=" + app;
		return s;
	}

	// Creates the default MAX_CONFIG_GROUP groups
	protected void createDefaultGroupInstances() {
		System.out.println("\nNode " + this.myID
				+ " initiating creation of default paxos groups:");
		for (String groupID : TESTPaxosConfig.getGroups()) {
			for (int id : TESTPaxosConfig.getGroup(groupID)) {
				boolean created = false;
				if (myID == id) {
					Set<Integer> group = Util.arrayToIntSet(TESTPaxosConfig
							.getGroup(groupID));
					System.out.print(groupID + ":" + group + " ");
					created = this.getPaxosManager().createPaxosInstance(
							groupID, 0, group, (ReplicableDeprecated) null,
							"default_initial_state_string");
					if (!created)
						System.out
								.println(":  not created (probably coz it is pre-existing)");
					else
						System.out.println("Created group " + groupID
								+ " with members " + group);
				}
			}
		}
	}

	// Creates groups if needed more than MAX_CONFIG_GROUPS
	protected void createNonDefaultGroupInstanes(int numGroups) {
		int j = 1;
		if (numGroups > Config.getGlobalInt(TC.PRE_CONFIGURED_GROUPS))
			System.out.println("\nNode " + this.myID
					+ " initiating creation of non-default groups:");
		// Creating groups beyond default configured groups (if numGroups >
		// MAX_CONFIG_GROUPS)
		for (int i = Config.getGlobalInt(TC.PRE_CONFIGURED_GROUPS); i < numGroups; i++) {
			String groupID = Config.getGlobalString(TC.TEST_GUID_PREFIX) + i;
			for (int id : TESTPaxosConfig.getDefaultGroup()) {
				Set<Integer> group = Util.arrayToIntSet(TESTPaxosConfig
						.getGroup(groupID));
				if (id == myID)
					this.getPaxosManager().createPaxosInstance(groupID, 0,
							group, (ReplicableDeprecated) null, null);
			}
			if (i % j == 0 && ((j *= 2) > 1) || (i % 100000 == 0)) {
				System.out.print(i + " ");
			}
		}
	}

	private static int processArgs(String[] args) {
		// last arg is always node ID
		int myID = -1;
		try {
			myID = (args != null && args.length > 0 ? Integer.parseInt(args[args.length-1])
					: -1);
		} catch (NumberFormatException nfe) {
		}
		assert (myID != -1) : "Need an integer node ID as the last argument";

		// first arg starting with "-D" will be interpreted as confDir
		TESTPaxosConfig.setDistribtedTest(TESTPaxosConfig.getConfDirArg(args));
				//Arrays.copyOfRange(args, 1, args.length)));

		// if -c is in args
		PaxosManager.startWithCleanDB(TESTPaxosConfig.shouldCleanDB(args));

		return myID;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			TESTPaxosConfig.setConsoleHandler();
			NIOTransport.setUseSenderTask(Config.getGlobalBoolean(PC.USE_NIO_SENDER_TASK));
			int myID = processArgs(args);
			TESTPaxosNode me = new TESTPaxosNode(myID, TESTPaxosConfig.getFromPaxosConfig(), false);

			// Creating default groups
			int numGroups = Config.getGlobalInt(TC.NUM_GROUPS);
			System.out
					.println("Creating "
							+ Config.getGlobalInt(TC.PRE_CONFIGURED_GROUPS)
							+ " default groups");
			me.createDefaultGroupInstances();
			System.out.println("Creating "
					+ (numGroups - Config.getGlobalInt(TC.PRE_CONFIGURED_GROUPS))
					+ " additional non-default groups");
			me.createNonDefaultGroupInstanes(numGroups);

			System.out.println("\n\nFinished creating all groups\n\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
