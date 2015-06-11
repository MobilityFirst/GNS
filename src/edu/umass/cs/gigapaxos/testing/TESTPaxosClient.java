package edu.umass.cs.gigapaxos.testing;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.ProposalPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 */
public class TESTPaxosClient {
	private static long minSleepInterval = 10;

	private static final long createTime = System.currentTimeMillis();
	private static final int RANDOM_REPLAY = (int) (Math.random() * TESTPaxosConfig
			.getNumGroups());

	private static int totalNoopCount = 0;

	private static int numRequests = 0; // used only for latency
	private static long totalLatency = 0;
	private static double movingAvgLatency = 0;
	private static double movingAvgDeviation = 0;
	private static int numRtxReqs = 0;
	private static int rtxCount = 0;

	private static synchronized void incrTotalLatency(long ms) {
		totalLatency += ms;
		numRequests++;
	}

	private static synchronized void updateMovingAvgLatency(long ms) {
		movingAvgLatency = Util.movingAverage(ms, movingAvgLatency);
		movingAvgDeviation = Util.movingAverage(ms, movingAvgDeviation);
	}

	private static synchronized void updateLatency(long ms) {
		incrTotalLatency(ms);
		updateMovingAvgLatency(ms);
	}

	private static synchronized double getTimeout() {
		return Math.max(movingAvgLatency + 4 * movingAvgDeviation,
				TESTPaxosConfig.CLIENT_REQ_RTX_TIMEOUT);
	}

	private static synchronized double getAvgLatency() {
		return totalLatency * 1.0 / numRequests;
	}

	private static synchronized void incrRtxCount() {
		rtxCount++;
	}

	private static synchronized void incrNumRtxReqs() {
		numRtxReqs++;
	}

	private static synchronized int getRtxCount() {
		return rtxCount;
	}

	private static synchronized int getNumRtxReqs() {
		return numRtxReqs;
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

	private final ConcurrentHashMap<Integer, RequestPacket> requests = new ConcurrentHashMap<Integer, RequestPacket>();
	private final Timer timer = new Timer(); // for retransmission

	private static Logger log = PaxosManager.getLogger();

	private synchronized int incrReplyCount() {
		return this.replyCount++;
	}

	private synchronized int incrReqCount() {
		return ++this.reqCount;
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
		this.timer.cancel();
		this.niot.stop();
	}

	synchronized boolean noOutstanding() {
		return this.requests.isEmpty();
	}

	/******** Start of ClientPacketDemultiplexer ******************/
	private class ClientPacketDemultiplexer extends
			AbstractJSONPacketDemultiplexer {
		private final TESTPaxosClient client;

		private ClientPacketDemultiplexer(TESTPaxosClient tpc) {
			this.client = tpc;
			this.register(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
		}

		public synchronized boolean handleMessage(JSONObject msg) {
			try {
				ProposalPacket proposal = new ProposalPacket(msg);
				long latency = System.currentTimeMillis()
						- proposal.getCreateTime();
				if (requests.containsKey(proposal.requestID)) {
					client.incrReplyCount();
					log.log(Level.FINE,
							"{0}{1}{2}{3}{4}{5}{6}{7}{8}",
							new Object[] { "Client ", client.myID,
									" received response #",
									client.getReplyCount(), " with latency ",
									latency, proposal.getDebugInfo(), " : ",
									msg });
					updateLatency(latency);
					synchronized (client) {
						client.notify();
					}
				} else {
					log.log(Level.FINE,
							"{0}{1}{2}{3}{4}{5}{6}{7}{8}",
							new Object[] { "Client ", client.myID,
									" received PHANTOM response #",
									client.getReplyCount(), " with latency ",
									latency, proposal.getDebugInfo(), " : ",
									msg });
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

	/******** End of ClientPacketDemultiplexer ******************/

	private class Retransmitter extends TimerTask {
		final int id;
		final RequestPacket req;
		final double timeout;
		boolean first;

		Retransmitter(int id, RequestPacket req) {
			this(id, req, getTimeout());
			first = true;
		}

		Retransmitter(int id, RequestPacket req, double timeout) {
			this.id = id;
			this.req = req;
			this.timeout = timeout;
			first = false;
		}

		public void run() {
			try {
				// checks parent queue
				if (requests.containsKey(req.requestID)) {
					incrRtxCount();
					if (first)
						incrNumRtxReqs();
					log.log(Level.INFO, "{0}{1}{2}{3}{4}{5}", new Object[] {
							"Retransmitting request ", "" + req.requestID,
							" to node ", id, ": ", req });
					niot.sendToID(id, req.toJSONObject());
					timer.schedule(new Retransmitter(id, req, timeout * 2),
							(long) (timeout * 2));
				}
			} catch (IOException | JSONException e) {
				e.printStackTrace();
			}
		}
	}

	protected TESTPaxosClient(int id) throws IOException {
		this.myID = id;
		niot = (new JSONNIOTransport<Integer>(id,
				TESTPaxosConfig.getNodeConfig(),
				(new PacketDemultiplexerDefault()), false));
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
		log.log(Level.FINE, "{0}{1}{2}{3}", new Object[] {
				"Sending request to node ", id, ": ", req });
		this.requests.put(req.requestID, req);
		this.niot.sendToID(id, req.toJSONObject());
		if (TESTPaxosConfig.ENABLE_CLIENT_REQ_RTX)
			this.timer
					.schedule(new Retransmitter(id, req), (long) getTimeout());
	}

	// to control request size, min request size is still > 350B
	static String GIBBERISH = "01234567890123456789012345678901";
	static int REQ_BAGGAGE_SIZE = 0;
	static {
		if (GIBBERISH.length() < REQ_BAGGAGE_SIZE)
			GIBBERISH = GIBBERISH.substring(0, REQ_BAGGAGE_SIZE);
		else
			while (GIBBERISH.length() < REQ_BAGGAGE_SIZE)
				GIBBERISH += GIBBERISH;
	}

	protected RequestPacket makeRequest() {
		int reqID = ((int) (Math.random() * Integer.MAX_VALUE));
		RequestPacket req = new RequestPacket(
				this.myID,
				reqID, // only place where req count is
						// incremented
				"[Sample request numbered " + incrReqCount() + "]" + GIBBERISH,
				false);
		req.putPaxosID(TESTPaxosConfig.TEST_GUID, 0);
		return req;
	}

	protected RequestPacket makeRequest(String paxosID) {
		RequestPacket req = this.makeRequest();
		req.putPaxosID(paxosID != null ? paxosID : TESTPaxosConfig.TEST_GUID,
				 0);
		return req;
	}

	protected RequestPacket makeAndSendRequest(String paxosID)
			throws JSONException, IOException {
		RequestPacket req = this.makeRequest(paxosID);
		this.sendRequest(req);
		return req;
	}

	protected static TESTPaxosClient[] setupClients() {
		System.out.println("\n\nInitiating paxos clients setup");
		TESTPaxosClient[] clients = new TESTPaxosClient[TESTPaxosConfig
				.getNumClients()];
		for (int i = 0; i < TESTPaxosConfig.getNumClients(); i++) {
			try {
				clients[i] = new TESTPaxosClient(TESTPaxosConfig.TEST_CLIENT_ID
						+ i);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		System.out.println("Completed initiating "
				+ TESTPaxosConfig.getNumClients() + " clients");
		return clients;
	}

	protected static void sendTestRequests(int numReqsPerClient,
			TESTPaxosClient[] clients) throws JSONException, IOException {
		System.out.print("\nInitiating test sending " + numReqsPerClient
				* TESTPaxosConfig.getNumClients() + " requests using "
				+ TESTPaxosConfig.getNumClients()
				+ " clients at an aggregate load of "
				+ TESTPaxosConfig.getTotalLoad() + " reqs/sec...");
		long initTime = System.currentTimeMillis();
		for (int i = 0; i < numReqsPerClient; i++) {
			for (int j = 0; j < TESTPaxosConfig.getNumClients(); j++) {
				int curTotalReqs = j + i * TESTPaxosConfig.getNumClients();
				clients[j].makeAndSendRequest("paxos"
						+ ((RANDOM_REPLAY + curTotalReqs) % TESTPaxosConfig
								.getNumGroups()));
				long leadTime = (long) ((curTotalReqs)
						/ TESTPaxosConfig.getTotalLoad() * 1000)
						- (System.currentTimeMillis() - initTime);
				if (leadTime > minSleepInterval) {
					try {
						Thread.sleep(leadTime);
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
		for (int i = 0; i < TESTPaxosConfig.getNumClients(); i++) {
			while (clients[i].requests.size() > 0) {
				synchronized (clients[i]) {
					try {
						System.out.println(getWaiting(clients)
								+ "; #num_total_retransmissions = "
								+ getRtxCount()
								+ "; num_retransmitted_requests = "
								+ getNumRtxReqs());
						clients[i].wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				System.out.println("Cumulative response rate = "
						+ Util.df(getTotalThroughput(clients)) + " reqs/sec");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	protected static boolean noOutstanding(TESTPaxosClient[] clients) {
		boolean noOutstanding = true;
		for (TESTPaxosClient client : clients)
			noOutstanding = noOutstanding && client.noOutstanding();
		return noOutstanding;
	}

	protected static Set<RequestPacket> getMissingRequests(
			TESTPaxosClient[] clients) {
		Set<RequestPacket> missing = new HashSet<RequestPacket>();
		for (int i = 0; i < clients.length; i++) {
			missing.addAll(clients[i].requests.values());
		}
		return missing;
	}

	private static String getWaiting(TESTPaxosClient[] clients) {
		int total = 0;
		String s = " unfinished requests: [ ";
		for (int i = 0; i < clients.length; i++) {
			s += "C" + i + ":" + clients[i].requests.size() + " ";
			total += clients[i].requests.size();
		}
		s += "]";
		return total + s;
	}

	private static double getTotalThroughput(TESTPaxosClient[] clients) {
		int totalExecd = 0;
		for (int i = 0; i < clients.length; i++) {
			totalExecd += clients[i].getReplyCount();
		}
		return totalExecd * 1000.0 / (System.currentTimeMillis() - createTime);
	}

	protected static void printOutput(TESTPaxosClient[] clients) {
		for (int i = 0; i < TESTPaxosConfig.getNumClients(); i++) {
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
				+ Util.df(numReqs * TESTPaxosConfig.getNumClients() * 1000.0
						/ delay) + "/s" + "\n  noop_count = "
				+ TESTPaxosClient.getTotalNoopCount()
				+ "\n  average_response_time = "
				+ Util.df(TESTPaxosClient.getAvgLatency()) + "ms";
	}

	static class Main {
		public static void main(String[] args) {
			try {
				TESTPaxosConfig.setDistribtedTest(args.length > 0 ? args[0]
						: null);

				TESTPaxosClient[] clients = TESTPaxosClient.setupClients();
				int numReqs = TESTPaxosConfig.getNumRequestsPerClient();

				// begin first run
				long t1 = System.currentTimeMillis();
				sendTestRequests(numReqs, clients);
				waitForResponses(clients);
				long t2 = System.currentTimeMillis();
				System.out.println("\n[run1]"
						+ getAggregateOutput(numReqs, t2 - t1));
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
						+ getAggregateOutput(numReqs, t2 - t1));
				// end second run

				for (TESTPaxosClient client : clients) {
					client.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
