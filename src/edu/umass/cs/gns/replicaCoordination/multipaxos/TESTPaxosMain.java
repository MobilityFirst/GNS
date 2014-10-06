package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.PaxosInstrumenter;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class TESTPaxosMain {
	private HashMap<Integer,TESTPaxosNode> nodes=new HashMap<Integer,TESTPaxosNode>();
	ScheduledExecutorService execpool = Executors.newScheduledThreadPool(5);

	TESTPaxosMain() throws IOException {
		Set<Integer> nodeIDs = TESTPaxosConfig.getNodes();
		for(int id : nodeIDs) {
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
		for(int id : TESTPaxosConfig.getGroup(paxosID)) {
			if(TESTPaxosConfig.isCrashed(id)) continue;
			if(state==null) state = nodes.get(id).getAppState(paxosID);
			else assert(state.equals(prevState));
			prevState = state;
		}
	}

	private static int getRandomNodeID() {
		Set<Integer> allNodes = TESTPaxosConfig.getNodes();
		Object[] array = allNodes.toArray();
		int index = (int)(Math.random()*allNodes.size());
		return (Integer)array[index];
	}

	/* Will create a random group for testing. Will be a no-op
	 * if recovery is enabled.
	 */
	private static void createRandomGroup(String groupID) {
		int size = 0;
		while(size<3) size = (int)(Math.random()*10);
		TreeSet<Integer> group = new TreeSet<Integer>();
		while(group.size() < size) {
			group.add(getRandomNodeID());
		}
		int[] members = new int[group.size()];
		int i=0; for(int id : group) members[i++] = id;
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
			int myID = (args!=null && args.length>0 ? Integer.parseInt(args[0]) : -1);
			assert(myID==-1) : "Cannot specify node ID for local test with TESTPaxosMain";

			TESTPaxosMain tpMain=null;
			tpMain = new TESTPaxosMain(); // creates all nodes, each with its paxos manager and app

			createRandomGroups(); // no effect if recovery is enabled coz we need consistent groups across runs

			// creates paxos groups (may not create if recovering)
			for(int id : tpMain.nodes.keySet()) {
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
			System.out.println("Average response time of first run = " + TESTPaxosClient.getAvgLatency());
			TESTPaxosClient.resetLatencyComputation();
			TESTPaxosClient.sendTestRequests(numReqs, clients);
			TESTPaxosClient.waitForResponses(clients);


			long t2=System.currentTimeMillis();

			TESTPaxosClient.printOutput(clients);	
			System.out.println("Average throughput (overall) (req/sec) = " + 
					Util.df(numReqs*TESTPaxosConfig.NUM_CLIENTS*1000.0/(t2-t1)) + "\n" +
					"Total no-op count (overall) = " + TESTPaxosClient.getTotalNoopCount() +"\n" +
					"Average response time of just the second run (not overall) = " + TESTPaxosClient.getAvgLatency());
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
