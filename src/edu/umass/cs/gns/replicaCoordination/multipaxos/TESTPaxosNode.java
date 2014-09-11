package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;

import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class TESTPaxosNode {
	/* Nodes may create more groups than TESTPaxosConfig.MAX_CONFIG_GROUPS
	 * for testing scalability.
	 */
	//public static final int NUM_GROUPS = TESTPaxosConfig.MAX_CONFIG_GROUPS;

	private final NodeId<String> myID;
	private PaxosManager pm=null;
	private TESTPaxosReplicable app=null;

	// A server must have an id
	TESTPaxosNode(int id) throws IOException {
		this.myID = id;
		app = new TESTPaxosReplicable();
		pm = startPaxosManager(id, app);
		// only for testing so app can send back response; in general, app should have its own NIO
		app.setNIOTransport(pm.getNIOTransport()); 
	}
	public PaxosManager startPaxosManager(int id, Replicable app) {
		try {
			this.pm = new PaxosManager(id, TESTPaxosConfig.getNodeConfig(), 
					new JSONNIOTransport(id, TESTPaxosConfig.getNodeConfig(), 
							new JSONMessageExtractor(new PacketDemultiplexerDefault())), app, null);
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return pm;
	}
	public void close() {this.pm.close();}

	protected Replicable getApp() {return app;}
	protected PaxosManager getPaxosManager() {return pm;}
	protected String getAppState(String paxosID) {return app.getState(paxosID);}

	protected void crash() {
		this.pm.resetAll();
		this.app.shutdown();
	}
	public String toString() {
		String s="[id=";
		s+=this.myID + ", pm="+pm + ", app="+app;
		return s;
	}

	// Creates the default MAX_CONFIG_GROUP groups
	protected void createDefaultGroupInstances() {
		System.out.println("\nNode " + this.myID + " initiating creation of default paxos groups:");
		for(String groupID : TESTPaxosConfig.getGroups()) {
			for(int id: TESTPaxosConfig.getGroup(groupID)) {
				boolean created = false;
				if(myID==id) {
					System.out.print(groupID + ":" + Util.arrayToIntSet(TESTPaxosConfig.getGroup(groupID)) + " ");
					created = this.getPaxosManager().createPaxosInstance(groupID, (short)0, 
							Util.arrayToIntSet(TESTPaxosConfig.getGroup(groupID)), null);
					if(!created) System.out.println(":  not created (probably coz it is pre-existing)");
				}
			}
		}	
	}
	// Creates groups if needed more than MAX_CONFIG_GROUPS
	protected void createNonDefaultGroupInstanes(int numGroups) {
		int j=1;
		if(numGroups > TESTPaxosConfig.MAX_CONFIG_GROUPS) 
			System.out.println("\nNode "+this.myID+" initiating creation of non-default groups:");
		// Creating groups beyond default configured groups (if numGroups > MAX_CONFIG_GROUPS)
		for(int i=TESTPaxosConfig.MAX_CONFIG_GROUPS; i<numGroups; i++) {
			String groupID = TESTPaxosConfig.TEST_GUID_PREFIX+i;
			for(int id: TESTPaxosConfig.getDefaultGroup()) {
				if(id==myID) this.getPaxosManager().createPaxosInstance(groupID, (short)0, 
						Util.arrayToIntSet(TESTPaxosConfig.getGroup(groupID)), null);
			}
			if(i%j==0 && ((j*=2)>1) || (i%100000==0)) {
				System.out.print(i+" ");
			}
		}
	}


	public static void main(String[] args) {
		try {
			if(!TESTPaxosConfig.TEST_WITH_RECOVERY) TESTPaxosConfig.setCleanDB(true);
			NodeId<String> myID = (args!=null && args.length>0 ? Integer.parseInt(args[0]) : -1);
			assert(myID!=-1) : "Need a node ID argument"; 
			
			int numGroups = TESTPaxosConfig.NUM_GROUPS;
			if (args!=null && args.length>1) numGroups =  Integer.parseInt(args[1]);

			if(TESTPaxosConfig.findMyIP(myID))  {
				TESTPaxosConfig.setDistributedServers();
				TESTPaxosConfig.setDistributedClients();
			}
			TESTPaxosNode me = new TESTPaxosNode(myID);

			// Creating default groups
			System.out.println("Creating " + TESTPaxosConfig.MAX_CONFIG_GROUPS + " default groups");
			me.createDefaultGroupInstances();
			System.out.println("Creating " + (numGroups - TESTPaxosConfig.MAX_CONFIG_GROUPS) + " additional non-default groups");
			me.createNonDefaultGroupInstanes(numGroups);

			System.out.println("\n\nFinished creating all groups\n\n"); // no output to print here except logs
		} catch(Exception e) {e.printStackTrace();}
	}
}
