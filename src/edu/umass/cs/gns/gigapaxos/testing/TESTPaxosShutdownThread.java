package edu.umass.cs.gns.gigapaxos.testing;

import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.gigapaxos.paxosutil.RequestInstrumenter;

/* This class is for printing testing/debug information when
 * a paxos test run is killed ungracefully (using Ctrl-C).
 */
public class TESTPaxosShutdownThread extends Thread {

	static {
		Runtime.getRuntime().addShutdownHook(new Thread(new TESTPaxosShutdownThread()));
	}
	private static Set<TESTPaxosClient> testClients = new HashSet<TESTPaxosClient>();
	private static Set<TESTPaxosReplicable> apps = new HashSet<TESTPaxosReplicable>();
	
	protected static void register(TESTPaxosClient[] clients) {
		for(TESTPaxosClient client : clients) {
			testClients.add(client);
		}
	}
	protected static void register(TESTPaxosReplicable app) {
		apps.add(app);
	}
	
	public void run() {
		System.out.println("![TESTPaxosShutdownThread invoked]!");
		Set<RequestPacket> missing = TESTPaxosClient.getMissingRequests(testClients.toArray(new TESTPaxosClient[0]));
		if(RequestInstrumenter.DEBUG) {
			String sep="-------\n";
			for(RequestPacket req : missing) {
				System.out.print(sep + (RequestInstrumenter.getLog(req.requestID)));
			}
		}
		System.out.println("\n#missing="+missing.size());
	}
	public static void main(String[] args) {
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
