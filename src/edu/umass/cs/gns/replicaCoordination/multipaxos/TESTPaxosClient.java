package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.DefaultPacketDemultiplexer;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.BasicPacketDemultiplexer;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class TESTPaxosClient {
	private static final long INITIAL_WARMUP_DELAY = 1000;
	private static final long createTime = System.currentTimeMillis();
	private static final int random = (int)(Math.random()*TESTPaxosConfig.NUM_GROUPS);

	private final GNSNIOTransport niot;
	private final int myID;
	private int reqCount=0;
	private int replyCount=0;
	private int noopCount=0;
	private int executedCount=0;
	private int preRecoveryExecutedCount=0;

	private ConcurrentHashMap<Integer,RequestPacket> requests = new ConcurrentHashMap<Integer,RequestPacket>();

	private static Logger log = Logger.getLogger(TESTPaxosClient.class.getName()); // GNS.getLogger();

	private synchronized int incrReplyCount() {return this.replyCount++;}
	private synchronized int incrReqCount() {return this.reqCount++;}
	private synchronized int incrNoopCount() {return this.noopCount++;}

	private synchronized int getReplyCount() {return this.replyCount;}
	private synchronized int getExecutedCount() {return this.executedCount-this.preRecoveryExecutedCount;}
	private synchronized int getRequestCount() {return this.reqCount;}
	private synchronized int getNoopCount() {return this.noopCount;}
	private synchronized void setExecutedCount(int ec) {if(ec > this.executedCount) this.executedCount=ec;}
	private synchronized void setPreRecoveryCount(int prc) {if(this.executedCount==0) this.preRecoveryExecutedCount=prc;}
	private synchronized int getPreRecoveryCount() {return this.preRecoveryExecutedCount;}

	private class ClientPacketDemultiplexer extends BasicPacketDemultiplexer {
		private final TESTPaxosClient client;
		private ClientPacketDemultiplexer(TESTPaxosClient tpc) {this.client=tpc;}
		public synchronized boolean handleJSONObject(JSONObject msg) {
			try {
				ProposalPacket proposal = new ProposalPacket(msg);
				long latency = System.currentTimeMillis() - proposal.getCreateTime();
				if(requests.containsKey(proposal.requestID)) {
					log.info("Client " + client.myID + " received response #" + client.incrReplyCount() + 
							" with latency " + latency+ proposal.getDebugInfo()+" : " + msg);
				}
				if(proposal.isNoop()) client.incrNoopCount();
				client.setPreRecoveryCount(proposal.slot);
				client.setExecutedCount(proposal.slot+1);
				requests.remove(proposal.requestID);
			} catch(JSONException je) {je.printStackTrace();}
			return true;
		}
	}

	protected TESTPaxosClient(int id) throws IOException {
		this.myID = id;
		niot = new GNSNIOTransport(myID, TESTPaxosConfig.getNodeConfig(), 
				new JSONMessageExtractor(new DefaultPacketDemultiplexer()));
		niot.addPacketDemultiplexer(new ClientPacketDemultiplexer(this));
		new Thread(niot).start();
	}

	protected void sendRequest(RequestPacket req) throws IOException, JSONException {
		int[] group = TESTPaxosConfig.getGroup(req.getPaxosID());
		int index=-1;
		while(index<0 || index>group.length || TESTPaxosConfig.isCrashed(group[index])) {
			index = (int)(Math.random()*group.length); if(index==group.length) index--;
		}
		this.sendRequest(group[index], req);
	}
	protected void sendRequest(int id, RequestPacket req) throws IOException, JSONException {
		log.info("Sending request to node " + id + ": " + req);
		this.requests.put(req.requestID, req);
		this.niot.sendToID(id, req.toJSONObject());
	}
	protected RequestPacket makeRequest() {
		int reqID = this.incrReqCount();
		RequestPacket req = new RequestPacket(this.myID, reqID, // only place where req count is incremented
				"[Sample request numbered " + ((int)(Math.random()*Integer.MAX_VALUE)) + "]", false);
		req.putPaxosID(TESTPaxosConfig.TEST_GUID, (short)0);
		return req;
	}
	protected RequestPacket makeRequest(String paxosID) {
		RequestPacket req = this.makeRequest();
		req.putPaxosID(paxosID!=null ? paxosID : TESTPaxosConfig.TEST_GUID, (short)0);
		req.setReplyToClient(true);
		return req;
	}
	protected RequestPacket makeAndSendRequest(String paxosID) throws JSONException, IOException {
		//assert(TESTPaxosConfig.getGroups().contains(paxosID));
		RequestPacket req = this.makeRequest(paxosID);
		this.sendRequest(req);
		return req;
	}

	protected static TESTPaxosClient[] setupClients() {
		System.out.println("\n\nInitiating paxos clients setup");
		TESTPaxosClient[] clients = new TESTPaxosClient[TESTPaxosConfig.NUM_CLIENTS];
		for(int i=0; i<TESTPaxosConfig.NUM_CLIENTS; i++) {
			try {
				clients[i] = new TESTPaxosClient(TESTPaxosConfig.TEST_CLIENT_ID+i);
			} catch(Exception e) {
				e.printStackTrace(); 
				System.exit(1);
			}
		}
		System.out.println("Completed initiating " + TESTPaxosConfig.NUM_CLIENTS + " clients");
		return clients;
	}
	protected static void sendTestRequests(int numReqs, TESTPaxosClient[] clients) throws JSONException, IOException{
		System.out.println("\nInitiating test sending " + numReqs*TESTPaxosConfig.NUM_CLIENTS + 
				" requests using " + TESTPaxosConfig.NUM_CLIENTS + " clients at an aggregate load of "+
				TESTPaxosConfig.TOTAL_LOAD + " reqs/sec");
		double interRequestInterval = 0;
		for(int i=0; i<numReqs; i++) {
			try {
				for(int j=0; j<TESTPaxosConfig.NUM_CLIENTS; j++) {
					/* Note: test will fail if a client sends requests to more than one paxosID
					 * as the test is currently set up to count executed requests using slot
					 * numbers in each paxos instance. Changing that to matching reply count
					 * against request count is not enough as clients may not receive all
					 * replies if there is checkpoint transfer.
					 */
					clients[j].makeAndSendRequest("paxos"+((random+j+i*TESTPaxosConfig.NUM_CLIENTS)%TESTPaxosConfig.NUM_GROUPS)); 
					if(j + i*TESTPaxosConfig.NUM_CLIENTS == TESTPaxosConfig.NUM_GROUPS-1) Thread.sleep(INITIAL_WARMUP_DELAY);
					interRequestInterval += 1000/TESTPaxosConfig.TOTAL_LOAD;
					if(interRequestInterval > 10) {
						Thread.sleep((long)interRequestInterval);
						interRequestInterval = 0;
					}
				}
			} catch(InterruptedException e) {e.printStackTrace();}
		}
	}

	protected static void waitForResponses(TESTPaxosClient[] clients) {
		//waitForResponses(clients, true);
		for(int i=0; i<TESTPaxosConfig.NUM_CLIENTS; i++) {
			while(clients[i].requests.size()>0) {
				if(clients[i].getReplyCount()%1==0) {
					System.out.println("Client " + clients[i].myID + " recieved execution confirmation up to slot " + 
							clients[i].getExecutedCount() + " after sending " + clients[i].getRequestCount() + " requests" +
							"; requests turned to no-ops = " + clients[i].getNoopCount()+
							"; responses received = " + clients[i].getReplyCount() + "; preRecoveryExecutedCount = " +
							clients[i].getPreRecoveryCount() + "; current_throughput = "+ Util.df(getTotalThroughput(clients))+
							"; requests waiting = " + getWaiting(clients[i]) + "\n");
				}
				try {
					Thread.sleep(2*INITIAL_WARMUP_DELAY);
				} catch(InterruptedException e) {e.printStackTrace();}
				System.out.println("Current aggregate throughput = "+ Util.df(getTotalThroughput(clients)));
			}
			//assert(clients[i].requests.isEmpty());
		}
	}
	private static String getWaiting(TESTPaxosClient client) {
		String s="[";
		for(RequestPacket req : client.requests.values()) {
			s += req.getPaxosID()+":"+req.requestID + " , ";
		}
		s+="]";
		return s;
	}

	private static double getTotalThroughput(TESTPaxosClient[] clients) {
		int totalExecd = 0;
		for(int i=0; i<clients.length; i++) {
			totalExecd += clients[i].getReplyCount();
		}
		return totalExecd*1000.0/(System.currentTimeMillis() - createTime);
	}

	protected static void printOutput(TESTPaxosClient[] clients) {
		for(int i=0; i<TESTPaxosConfig.NUM_CLIENTS; i++) {
			//if(clients[i].getRequestCount()<=clients[i].getExecutedCount()) {
			if(clients[i].requests.isEmpty()) {
				System.out.println("\n\nSUCCESS! Exection count = "+clients[i].getExecutedCount() + "; requests issued = "
						+clients[i].getRequestCount() + "; requests turned to no-ops = " + clients[i].getNoopCount()+
						"; responses received = " + clients[i].getReplyCount() + "; preRecoveryExecutedCount = " +
						clients[i].getPreRecoveryCount() + "\n");
			}
			else System.out.println("\nFAILURE: Exection count = " +clients[i].getExecutedCount() + "; requests issued = "
					+clients[i].getRequestCount() +"; requests turned to no-ops = " + clients[i].getNoopCount()+ 
					"; responses received = " + clients[i].getReplyCount() + "\n");
		}
	}

	public static void main(String[] args) {
		try {
			int myID = (args!=null && args.length>0 ? Integer.parseInt(args[0]) : -1);
			assert(myID!=-1) : "Need a node ID argument";

			if(TESTPaxosConfig.findMyIP(myID))  {
				TESTPaxosConfig.setDistributedServers();
				TESTPaxosConfig.setDistributedClients();
			}
			TESTPaxosClient[] clients = TESTPaxosClient.setupClients();
			Thread.sleep(2000);

			long t1=System.currentTimeMillis();

			int numReqs = TESTPaxosConfig.NUM_REQUESTS_PER_CLIENT;
			sendTestRequests(numReqs, clients);
			waitForResponses(clients);
			Thread.sleep(1000);
			sendTestRequests(numReqs, clients);
			waitForResponses(clients);

			long t2 = System.currentTimeMillis();

			printOutput(clients);
			System.out.println("Average throughput (req/sec) = " + 
					Util.df(numReqs*TESTPaxosConfig.NUM_CLIENTS*1000.0/(t2-t1)));
			System.exit(1);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
