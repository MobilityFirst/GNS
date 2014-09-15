package edu.umass.cs.gns.replicaCoordination.multipaxos;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.PaxosInstrumenter;
import edu.umass.cs.gns.util.Util;
import java.util.Random;

/**
@author V. Arun
 */
public class TESTPaxosMain {
	private HashMap<NodeId<String>, TESTPaxosNode> nodes=new HashMap<NodeId<String>, TESTPaxosNode>();
	ScheduledExecutorService execpool = Executors.newScheduledThreadPool(5);

	TESTPaxosMain() throws IOException {
		Set<NodeId<String>> nodeIDs = TESTPaxosConfig.getNodes();
		for(NodeId<String> id : nodeIDs) {
			TESTPaxosNode node = null;
			try {
				node = new TESTPaxosNode(id);
			} catch(Exception e) {e.printStackTrace();}
			assert(node!=null) : "Failed to create node " + id;
			nodes.put(id, node); 
		}
	}
	protected void assertRSMInvariant(String paxosID) {
		String state = null, prevState = null;
		for(NodeId<String> id : TESTPaxosConfig.getGroup(paxosID)) {
			if(TESTPaxosConfig.isCrashed(id)) continue;
			if(state==null) state = nodes.get(id).getAppState(paxosID);
			else assert(state.equals(prevState));
			prevState = state;
		}
	}

	private static NodeId<String> getRandomNodeID() {
                Random random = new Random();
		Set<NodeId<String>> allNodes = TESTPaxosConfig.getNodes();
		NodeId<String>[] array = Util.setToNodeIdArray(allNodes);
		return array[random.nextInt(allNodes.size())];
	}

	/* Will create a random group for testing. Will be a no-op
	 * if recovery is enabled.
	 */
	private static void createRandomGroup(String groupID) {
		int size = 0;
		while(size<3) size = (int)(Math.random()*10);
		TreeSet<NodeId<String>> group = new TreeSet<NodeId<String>>();
		while(group.size() < size) {
			group.add(getRandomNodeID());
		}
		NodeId<String>[] members = new NodeId[group.size()];
		int i=0; for(NodeId<String> id : group) members[i++] = id;
		TESTPaxosConfig.createGroup(groupID, TESTPaxosConfig.TEST_WITH_RECOVERY ? 
				TESTPaxosConfig.getDefaultGroup() : members);
	}

	private static void createRandomGroups() {
		for(String groupID : TESTPaxosConfig.getGroups()) {
			if(groupID.equals(TESTPaxosConfig.getGroup(0))) continue; // first group is always default
			createRandomGroup(groupID);
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Only useful for local testing
		System.out.println("\nThis is a single-node test. For distributed testing, use TESTPaxosNode " +
				"and TESTPaxosClient with the appropriate configuration file.\nInitiating single-node test...\n");
		try {
			/*************** Setting up servers below ***************************/

			if(!TESTPaxosConfig.TEST_WITH_RECOVERY) TESTPaxosConfig.setCleanDB(true);
			NodeId<String> myID = (args!=null && args.length>0 ? new NodeId<String>(Integer.parseInt(args[0]))
                                : GNSNodeConfig.INVALID_NAME_SERVER_ID);
			assert(myID==GNSNodeConfig.INVALID_NAME_SERVER_ID) : "Cannot specify node ID for local test with TESTPaxosMain";

			TESTPaxosMain tpMain=null;
			tpMain = new TESTPaxosMain(); // creates all nodes, each with its paxos manager and app

			createRandomGroups(); // no effect if recovery is enabled coz we need consistent groups across runs

			// creates paxos groups (may not create if recovering)
			for(NodeId<String> id : tpMain.nodes.keySet()) {
				tpMain.nodes.get(id).createDefaultGroupInstances();
				tpMain.nodes.get(id).createNonDefaultGroupInstanes(TESTPaxosConfig.NUM_GROUPS);
			}

			/*************** End of server setup ***************************/

			/*************** Client requests/responses below ****************/

			TESTPaxosClient[] clients = TESTPaxosClient.setupClients();
			int numReqs = TESTPaxosConfig.NUM_REQUESTS_PER_CLIENT;
			long t1=System.currentTimeMillis();
			TESTPaxosClient.sendTestRequests(numReqs, clients);
			TESTPaxosClient.waitForResponses(clients);
			Thread.sleep(1000);
			TESTPaxosClient.sendTestRequests(numReqs, clients);
			TESTPaxosClient.waitForResponses(clients);


			long t2=System.currentTimeMillis();

			TESTPaxosClient.printOutput(clients);
			System.out.println("Average throughput (req/sec) = " + 
					Util.df(numReqs*TESTPaxosConfig.NUM_CLIENTS*1000.0/(t2-t1)));
			for(int i=0; i<3; i++) {
				PaxosManager.waitToFinishAll();
				AbstractPaxosLogger.waitToFinishAll();
				Thread.sleep(1000);
			}
			for(TESTPaxosNode node : tpMain.nodes.values()) node.close(); // can only close after all nodes have finished
			System.out.println(PaxosInstrumenter.getStats());
			System.exit(1);
		} catch(Exception e) {e.printStackTrace();System.exit(1);}
	}
}
