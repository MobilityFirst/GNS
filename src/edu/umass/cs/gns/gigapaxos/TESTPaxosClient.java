package edu.umass.cs.gns.gigapaxos;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.util.Util;

/**
 * @author V. Arun
 */
public class TESTPaxosClient {
	private static final boolean DEBUG = PaxosManager.DEBUG;
	private static long minSleepInterval = 10;

	private static final long createTime = System.currentTimeMillis();
	private static final int random = (int) (Math.random() * TESTPaxosConfig.NUM_GROUPS);

	private static int totalNoopCount = 0;
	private static int numRequests = 0;
	private static long totalLatency = 0;

	private static synchronized void incrTotalLatency(long ms) {
		totalLatency += ms;
		numRequests++;
	}

	protected static synchronized double getAvgLatency() {
		return totalLatency * 1.0 / numRequests;
	}

	protected synchronized static void resetLatencyComputation() {
		totalLatency = 0;
		numRequests = 0;
	}

	private final JSONNIOTransport<Integer> niot;
	private final int myID;
	private int reqCount = 0;
	private int replyCount = 0;
	private int noopCount = 0;
	private int executedCount = 0;
	private int preRecoveryExecutedCount = 0;

	private ConcurrentHashMap<Integer, RequestPacket> requests = new ConcurrentHashMap<Integer, RequestPacket>();

	private static Logger log = Logger.getLogger(TESTPaxosClient.class
			.getName()); // GNS.getLogger();

	private synchronized int incrReplyCount() {
		return this.replyCount++;
	}

	private synchronized int incrReqCount() {
		this.reqCount++;
		return (int) (Math.random() * Integer.MAX_VALUE);
	}

	private synchronized int incrNoopCount() {
		incrTotalNoopCount();
		return this.noopCount++;
	}

	private synchronized static int incrTotalNoopCount() {
		return totalNoopCount++;
	}

	protected synchronized static int getTotalNoopCount() {
		return totalNoopCount;
	}

	private synchronized int getReplyCount() {
		return this.replyCount;
	}

	private synchronized int getExecutedCount() {
		return this.executedCount - this.preRecoveryExecutedCount;
	}

	private synchronized int getRequestCount() {
		return this.reqCount;
	}

	private synchronized int getNoopCount() {
		return this.noopCount;
	}

	private synchronized void setExecutedCount(int ec) {
		if (ec > this.executedCount)
			this.executedCount = ec;
	}

	private synchronized void setPreRecoveryCount(int prc) {
		if (this.executedCount == 0)
			this.preRecoveryExecutedCount = prc;
	}

	private synchronized int getPreRecoveryCount() {
		return this.preRecoveryExecutedCount;
	}

	synchronized void close() {
		this.niot.stop();
	}

	private class ClientPacketDemultiplexer extends AbstractPacketDemultiplexer {
		private final TESTPaxosClient client;

		private ClientPacketDemultiplexer(TESTPaxosClient tpc) {
			this.client = tpc;
			this.register(Packet.PacketType.PAXOS_PACKET);
		}

		public synchronized boolean handleJSONObject(JSONObject msg) {
			try {
				ProposalPacket proposal = new ProposalPacket(msg);
				long latency = System.currentTimeMillis()
						- proposal.getCreateTime();
				if (requests.containsKey(proposal.requestID)) {
					client.incrReplyCount();
					if(DEBUG) log.info("Client " + client.myID + " received response #"
							+ client.getReplyCount() + " with latency "
							+ latency + proposal.getDebugInfo() + " : " + msg);
					incrTotalLatency(latency);
					synchronized (client) {
						client.notify();
					}
				} else {
					if (DEBUG)
						log.info("Client " + client.myID
								+ " received PHANTOM response #"
								+ client.getReplyCount() + " with latency "
								+ latency + proposal.getDebugInfo() + " : "
								+ msg);
				}
				if (proposal.isNoop())
					client.incrNoopCount();
				client.setPreRecoveryCount(proposal.slot);
				client.setExecutedCount(proposal.slot + 1);
				requests.remove(proposal.requestID);
			} catch (JSONException je) {
				je.printStackTrace();
			}
			return true;
		}
	}

	protected TESTPaxosClient(int id) throws IOException {
		this.myID = id;
		niot = new JSONNIOTransport<Integer>(myID,
				TESTPaxosConfig.getNodeConfig(), new JSONMessageExtractor(
						new PacketDemultiplexerDefault()));
		niot.addPacketDemultiplexer(new ClientPacketDemultiplexer(this));
		new Thread(niot).start();
	}

	protected void sendRequest(RequestPacket req) throws IOException,
			JSONException {
		int[] group = TESTPaxosConfig.getGroup(req.getPaxosID());
		int index = -1;
		while (index < 0 || index > group.length
				|| TESTPaxosConfig.isCrashed(group[index])) {
			index = (int) (Math.random() * group.length);
			if (index == group.length)
				index--;
		}
		this.sendRequest(group[index], req);
	}

	protected void sendRequest(int id, RequestPacket req) throws IOException,
			JSONException {
		if(DEBUG) log.info("Sending request to node " + id + ": " + req);
		this.requests.put(req.requestID, req);
		this.niot.sendToID(id, req.toJSONObject());
	}

	protected RequestPacket makeRequest() {
		int reqID = this.incrReqCount();
		RequestPacket req = new RequestPacket(this.myID,
				reqID, // only place where req count is incremented
				"[Sample request numbered "
						+ ((int) (Math.random() * Integer.MAX_VALUE)) + "]",
				false);
		req.putPaxosID(TESTPaxosConfig.TEST_GUID, (short) 0);
		return req;
	}

	protected RequestPacket makeRequest(String paxosID) {
		RequestPacket req = this.makeRequest();
		req.putPaxosID(paxosID != null ? paxosID : TESTPaxosConfig.TEST_GUID,
				(short) 0);
		req.setReplyToClient(true);
		return req;
	}

	protected RequestPacket makeAndSendRequest(String paxosID)
			throws JSONException, IOException {
		// assert(TESTPaxosConfig.getGroups().contains(paxosID));
		RequestPacket req = this.makeRequest(paxosID);
		this.sendRequest(req);
		return req;
	}

	protected static TESTPaxosClient[] setupClients() {
		System.out.println("\n\nInitiating paxos clients setup");
		TESTPaxosClient[] clients = new TESTPaxosClient[TESTPaxosConfig.NUM_CLIENTS];
		for (int i = 0; i < TESTPaxosConfig.NUM_CLIENTS; i++) {
			try {
				clients[i] = new TESTPaxosClient(TESTPaxosConfig.TEST_CLIENT_ID
						+ i);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		System.out.println("Completed initiating "
				+ TESTPaxosConfig.NUM_CLIENTS + " clients");
		return clients;
	}

	protected static void sendTestRequests(int numReqsPerClient,
			TESTPaxosClient[] clients) throws JSONException, IOException {
		System.out.print("\nInitiating test sending " + numReqsPerClient
				* TESTPaxosConfig.NUM_CLIENTS + " requests using "
				+ TESTPaxosConfig.NUM_CLIENTS
				+ " clients at an aggregate load of "
				+ TESTPaxosConfig.TOTAL_LOAD + " reqs/sec...");
		long initTime = System.currentTimeMillis();
		for (int i = 0; i < numReqsPerClient; i++) {
			for (int j = 0; j < TESTPaxosConfig.NUM_CLIENTS; j++) {
				int curTotalReqs = j + i * TESTPaxosConfig.NUM_CLIENTS;
				clients[j]
						.makeAndSendRequest("paxos"
								+ ((random + curTotalReqs) % TESTPaxosConfig.NUM_GROUPS));
				long accumulatedTime = System.currentTimeMillis() - initTime;
				if ((curTotalReqs) / TESTPaxosConfig.TOTAL_LOAD * 1000
						- accumulatedTime > minSleepInterval) {
					try {
						Thread.sleep(minSleepInterval);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
		System.out.println("done sending requests in "
				+ Util.df((System.currentTimeMillis() - initTime) / 1000.0)
				+ " secs");
	}

	protected static void waitForResponses(TESTPaxosClient[] clients) {
		for (int i = 0; i < TESTPaxosConfig.NUM_CLIENTS; i++) {
			while (clients[i].requests.size() > 0) {
				synchronized (clients[i]) {
					try {
						clients[i].wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				System.out.println("Current aggregate throughput = "
						+ Util.df(getTotalThroughput(clients)) + " reqs/sec");
				System.out.println(getWaiting(clients));
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static String getWaiting(TESTPaxosClient[] clients) {
		String s = "Unfinished requests: [ ";
		for (int i = 0; i < clients.length; i++) {
			s += "C" + i + ":" + clients[i].requests.size() + " ";
		}
		s += "]";
		return s;
	}

	private static double getTotalThroughput(TESTPaxosClient[] clients) {
		int totalExecd = 0;
		for (int i = 0; i < clients.length; i++) {
			totalExecd += clients[i].getReplyCount();
		}
		return totalExecd * 1000.0 / (System.currentTimeMillis() - createTime);
	}

	protected static void printOutput(TESTPaxosClient[] clients) {
		for (int i = 0; i < TESTPaxosConfig.NUM_CLIENTS; i++) {
			if (clients[i].requests.isEmpty()) {
				System.out.println("\n\nSUCCESS! requests issued = "
						+ clients[i].getRequestCount()
						+ "; requests turned to no-ops = "
						+ clients[i].getNoopCount() + "; responses received = "
						+ clients[i].getReplyCount()
						+ "; preRecoveryExecutedCount = "
						+ clients[i].getPreRecoveryCount() + "\n");
			} else
				System.out.println("\nFAILURE: Exection count = "
						+ clients[i].getExecutedCount()
						+ "; requests issued = " + clients[i].getRequestCount()
						+ "; requests turned to no-ops = "
						+ clients[i].getNoopCount() + "; responses received = "
						+ clients[i].getReplyCount() + "\n");
		}
	}
	
	protected static String getAggregateOutput(int numReqs, long delay) {
		return "\n  average_throughput = "
				+ Util.df(numReqs * TESTPaxosConfig.NUM_CLIENTS
						* 1000.0 / delay)
				+ "/s"
				+ "\n  noop_count = "
				+ TESTPaxosClient.getTotalNoopCount()
				+ "\n  average_response_time = "
				+ Util.df(TESTPaxosClient.getAvgLatency()) + "ms";
	}

	public static void main(String[] args) {
		try {
			int myID = (args != null && args.length > 0 ? Integer
					.parseInt(args[0]) : -1);
			assert (myID != -1) : "Need a node ID argument";

			if (TESTPaxosConfig.findMyIP(myID)) {
				TESTPaxosConfig.setDistributedServers();
				TESTPaxosConfig.setDistributedClients();
			}
			TESTPaxosClient[] clients = TESTPaxosClient.setupClients();
			int numReqs = TESTPaxosConfig.NUM_REQUESTS_PER_CLIENT;

			// begin first run
			long t1 = System.currentTimeMillis();
			sendTestRequests(numReqs, clients);
			waitForResponses(clients);
			long t2 = System.currentTimeMillis();
			System.out.println("\n[run1]"
					+ getAggregateOutput(numReqs, t2-t1));
			// end first run

			resetLatencyComputation();
			Thread.sleep(1000);
			
			// begin second run
			t1 = System.currentTimeMillis();
			sendTestRequests(numReqs, clients);
			waitForResponses(clients);
			t2 = System.currentTimeMillis();
			printOutput(clients); // printed only after second
			System.out.println("\n[run2] "
							+ getAggregateOutput(numReqs, t2-t1));
			// end second run
			
			for (TESTPaxosClient client : clients) {
				client.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
