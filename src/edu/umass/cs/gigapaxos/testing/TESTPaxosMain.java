package edu.umass.cs.gigapaxos.testing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import edu.umass.cs.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.utils.DelayProfiler;

/**
 * @author V. Arun
 */
public class TESTPaxosMain {
	private HashMap<Integer, TESTPaxosNode> nodes = new HashMap<Integer, TESTPaxosNode>();
	ScheduledExecutorService execpool = Executors.newScheduledThreadPool(5);

	TESTPaxosMain() throws IOException {
		Set<Integer> nodeIDs = TESTPaxosConfig.getNodes();
		for (int id : nodeIDs) {
			TESTPaxosNode node = null;
			try {
				node = new TESTPaxosNode(id);
			} catch (Exception e) {
				e.printStackTrace();
			}
			assert (node != null) : "Failed to create node " + id;
			nodes.put(id, node);
		}
	}

	protected void assertRSMInvariant(String paxosID) {
		String state = null, prevState = null;
		for (int id : TESTPaxosConfig.getGroup(paxosID)) {
			if (TESTPaxosConfig.isCrashed(id))
				continue;
			if (state == null)
				state = nodes.get(id).getAppState(paxosID);
			else
				assert (state.equals(prevState));
			prevState = state;
		}
	}

	private static String getAggregateOutput(int numReqs, long t1, long t2) {
		return TESTPaxosClient.getAggregateOutput(numReqs, t2 - t1) + "\n  "
				+ DelayProfiler.getStats() + "\n  "
				+ NIOInstrumenter.getJSONStats();
	}

	/*
	 * This method tests single-node paxos and exits gracefully at the end by closing all nodes and
	 * associated paxos managers. Calling this method again with testRecovery=true will test
	 * recovery mode.
	 */
	public static void testPaxos() {
		try {
			/*************** Setting up servers below ***************************/

			TESTPaxosMain tpMain = null;
			tpMain = new TESTPaxosMain(); // creates all nodes, each with its paxos manager and app

			// no-op if recovery enabled coz we need consistent groups across runs
			TESTPaxosConfig
					.setRandomGroups(TESTPaxosConfig.PRE_CONFIGURED_GROUPS);

			// creates paxos groups (may not create if recovering)
			for (int id : tpMain.nodes.keySet()) {
				tpMain.nodes.get(id).createDefaultGroupInstances();
				tpMain.nodes.get(id).createNonDefaultGroupInstanes(
						TESTPaxosConfig.getNumGroups());
			}

			/*************** End of server setup ***************************/

			/*************** Client requests/responses below ****************/

			TESTPaxosClient[] clients = TESTPaxosClient.setupClients();
			TESTPaxosShutdownThread.register(clients);
			int numReqs = TESTPaxosConfig.getNumRequestsPerClient();

			// begin first run
			long t1 = System.currentTimeMillis();
			TESTPaxosClient.sendTestRequests(numReqs, clients);
			TESTPaxosClient.waitForResponses(clients);
			long t2 = System.currentTimeMillis();
			System.out
					.println("\n[run1]" + getAggregateOutput(numReqs, t1, t2));
			// end first run

			assert(TESTPaxosClient.noOutstanding(clients));
			TESTPaxosClient.resetLatencyComputation();
			Thread.sleep(2000);

			// begin second run
			t1 = System.currentTimeMillis();
			TESTPaxosClient.sendTestRequests(numReqs, clients);
			TESTPaxosClient.waitForResponses(clients);
			t2 = System.currentTimeMillis();
			TESTPaxosClient.printOutput(clients);
			System.out.println("[run2]" + getAggregateOutput(numReqs, t1, t2));
			// end second run

			for (TESTPaxosNode node : tpMain.nodes.values()) {
				node.close();
			}
			for (TESTPaxosClient client : clients)
				client.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void processArgs(String[] args) {
		// Only useful for local testing
		TESTPaxosConfig.setSingleNodeTest(TESTPaxosConfig.getConfDirArg(args));
		TESTPaxosConfig.setCleanDB(args);
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		processArgs(args);

		System.out
				.println("\nThis is a single-node test. For distributed testing, "
						+ "use TESTPaxosNode and TESTPaxosClient with the appropriate "
						+ "configuration file.\nInitiating single-node test...\n");
		TESTPaxosConfig.setAssertRSMInvariant(true);
		testPaxos();

		Thread.sleep(1000);

		System.out
				.println("\n############### Testing with recovery ################\n");
		TESTPaxosConfig.setCleanDB(false);
		testPaxos();
	}

}
