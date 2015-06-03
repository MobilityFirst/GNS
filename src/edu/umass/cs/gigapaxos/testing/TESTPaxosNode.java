package edu.umass.cs.gigapaxos.testing;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.deprecated.Replicable;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 */
public class TESTPaxosNode {
	/*
	 * Nodes may create more groups than TESTPaxosConfig.MAX_CONFIG_GROUPS for testing scalability.
	 */

	private final int myID;
	private PaxosManager<Integer> pm = null;
	private TESTPaxosReplicable app = null;

	// A server must have an id
	TESTPaxosNode(int id) throws IOException {
		this.myID = id;
		pm = startPaxosManagerAndApp(id);
		assert (pm != null);
	}

	private PaxosManager<Integer> startPaxosManagerAndApp(int id) {
		try {
			// shared between app and paxos manager only for testing
			JSONNIOTransport<Integer> niot = null;
			this.pm = new PaxosManager<Integer>(id,
					TESTPaxosConfig.getNodeConfig(),
					(niot = new JSONNIOTransport<Integer>(id, TESTPaxosConfig
							.getNodeConfig(), new PacketDemultiplexerDefault(),
							true)),
					(this.app = new TESTPaxosReplicable(niot)), null);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return pm;
	}

	public void close() {
		this.pm.close();
	}

	protected Replicable getApp() {
		return app;
	}

	protected PaxosManager<Integer> getPaxosManager() {
		return pm;
	}

	protected String getAppState(String paxosID) {
		return app.getState(paxosID);
	}

	protected void crash() {
		this.pm.resetAll();
		this.app.shutdown();
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
							groupID, (short) 0, group, (Replicable)null, "default_initial_state_string");
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
		if (numGroups > TESTPaxosConfig.PRE_CONFIGURED_GROUPS)
			System.out.println("\nNode " + this.myID
					+ " initiating creation of non-default groups:");
		// Creating groups beyond default configured groups (if numGroups > MAX_CONFIG_GROUPS)
		for (int i = TESTPaxosConfig.PRE_CONFIGURED_GROUPS; i < numGroups; i++) {
			String groupID = TESTPaxosConfig.TEST_GUID_PREFIX + i;
			for (int id : TESTPaxosConfig.getDefaultGroup()) {
				Set<Integer> group = Util.arrayToIntSet(TESTPaxosConfig
						.getGroup(groupID));
				if (id == myID)
					this.getPaxosManager().createPaxosInstance(groupID,
							(short) 0, group, (Replicable)null);
			}
			if (i % j == 0 && ((j *= 2) > 1) || (i % 100000 == 0)) {
				System.out.print(i + " ");
			}
		}
	}

	private static int processArgs(String[] args) {
		// first arg is always node ID
		int myID = -1;
		try {
			myID = (args != null && args.length > 0 ? Integer.parseInt(args[0])
					: -1);
		} catch (NumberFormatException nfe) {
		}
		assert (myID != -1) : "Need an integer node ID as the first argument";

		// args[1] or beyond other than "-c" will be interpreted as confDir
		TESTPaxosConfig.setDistribtedTest(TESTPaxosConfig.getConfDirArg(Arrays
				.copyOfRange(args, 1, args.length)));

		// if -c is in args
		TESTPaxosConfig.setCleanDB(args);

		return myID;
	}

	public static void main(String[] args) {
		try {
			int myID = processArgs(args);
			TESTPaxosNode me = new TESTPaxosNode(myID);

			// Creating default groups
			int numGroups = TESTPaxosConfig.getNumGroups();
			System.out
					.println("Creating "
							+ TESTPaxosConfig.PRE_CONFIGURED_GROUPS
							+ " default groups");
			me.createDefaultGroupInstances();
			System.out.println("Creating "
					+ (numGroups - TESTPaxosConfig.PRE_CONFIGURED_GROUPS)
					+ " additional non-default groups");
			me.createNonDefaultGroupInstanes(numGroups);

			System.out.println("\n\nFinished creating all groups\n\n"); // no stdout to print here
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
