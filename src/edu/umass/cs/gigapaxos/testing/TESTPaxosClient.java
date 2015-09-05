/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
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

import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 */
public class TESTPaxosClient {
	static {
		TESTPaxosConfig.load();
	}

	// private static final long createTime = System.currentTimeMillis();
	private static final int RANDOM_REPLAY = (int) (Math.random() * Config
			.getGlobalInt(TC.NUM_GROUPS));

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

	protected synchronized static void resetLatencyComputation(
			TESTPaxosClient[] clients) {
		totalLatency = 0;
		numRequests = 0;
		for (TESTPaxosClient client : clients)
			client.runReplyCount = 0;
	}

	private final JSONNIOTransport<Integer> niot;
	private final int myID;
	private int totReqCount = 0;
	private int totReplyCount = 0;
	private int runReplyCount = 0;
	private int noopCount = 0;

	private final ConcurrentHashMap<Long, RequestPacket> requests = new ConcurrentHashMap<Long, RequestPacket>();
	private final ConcurrentHashMap<Long, Long> requestCreateTimes = new ConcurrentHashMap<Long, Long>();
	private final Timer timer; // for retransmission

	private static Logger log = Logger.getLogger(TESTPaxosClient.class
			.getName());

	// PaxosManager.getLogger();

	private synchronized int incrReplyCount() {
		this.runReplyCount++;
		return this.totReplyCount++;
	}

	private synchronized int incrReqCount() {
		return ++this.totReqCount;
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

	private synchronized int getTotalReplyCount() {
		return this.totReplyCount;
	}

	private synchronized int getRunReplyCount() {
		return this.runReplyCount;
	}

	private synchronized int getTotalRequestCount() {
		return this.totReqCount;
	}

	private synchronized int getNoopCount() {
		return this.noopCount;
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
			this.setThreadName("" + tpc.myID);
		}

		public boolean handleMessage(JSONObject msg) {
			try {
				RequestPacket request = new RequestPacket(msg);
				if (requests.containsKey(request.requestID)) {
					client.incrReplyCount();
					assert (requestCreateTimes.containsKey(request.requestID)) : request;
					long latency = System.currentTimeMillis()
							- requestCreateTimes.get(request.requestID);
					log.log(Level.FINE,
							"Client {0} received response #{1} with latency {2} [{3}] : {4}",
							new Object[] { client.myID,
									client.getTotalReplyCount(), latency,
									request.getDebugInfo(),
									request.getSummary() });
					updateLatency(latency);
					synchronized (client) {
						client.notify();
					}
				} else {
					log.log(Level.FINE,
							"Client {0} received PHANTOM response #{1} [{2}] for request {3} : {4}",
							new Object[] { client.myID,
									client.getTotalReplyCount(),
									request.getDebugInfo(), request.requestID,
									request.getSummary() });
				}
				if (request.isNoop())
					client.incrNoopCount();
				requests.remove(request.requestID);
				requestCreateTimes.remove(request.requestID);
			} catch (JSONException je) {
				log.severe(this + " incurred JSONException while processing "
						+ msg);
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

	protected TESTPaxosClient(int id, InterfaceNodeConfig<Integer> nc) throws IOException {
		this.myID = id;
		niot = (new JSONNIOTransport<Integer>(id,
				nc==null ? TESTPaxosConfig.getNodeConfig() : nc,
				//TESTPaxosConfig.getNodeConfig(),
				(new ClientPacketDemultiplexer(this)), true));
		this.timer = new Timer(TESTPaxosClient.class.getSimpleName() + myID);
	}
	

	private boolean sendRequest(RequestPacket req) throws IOException,
			JSONException {
		int[] group = TESTPaxosConfig.getGroup(req.getPaxosID());
		int index = (int)(req.requestID % group.length);
		while (index < 0 || index >= group.length
				|| TESTPaxosConfig.isCrashed(group[index]))
			index = (int) (Math.random() * group.length);
		return this.sendRequest(group[index], req);
	}

	protected boolean sendRequest(int id, RequestPacket req)
			throws IOException, JSONException {
		log.log(Level.FINE, "Sending request to node {0}: {1}", new Object[] {
				id, req.getSummary() });
		if (this.requests.put(req.requestID, req) != null
				|| this.requestCreateTimes.put(req.requestID,
						System.currentTimeMillis()) != null)
			return false; // collision in integer space
		this.incrReqCount();
		// no retransmission send
		assert (this.niot.sendToID(id, req.toJSONObject()) > 0);
		if (TESTPaxosConfig.ENABLE_CLIENT_REQ_RTX)
			this.timer
					.schedule(new Retransmitter(id, req), (long) getTimeout());
		return true;
	}

	/**
	 * to control request size; min request size is still at least ~350B
	 */
	public static final String GIBBERISH = "47343289u230798sd7f69sf79s8fs9fys9nlk,.nesd5623mfds87wekldsoi;,DS"
			+ "NFSDHGFBLLLAS7238485734934MNFD Z|47343289u23094322|94322"
			+ "FHKSDF74JKFHDSOLRW0-3NCML,VFDYUP9045YMRJBJ;ASSKJBKNL7R3498 lc"
			+ "fdslfmsf90sdjwe;dj8934r436854398uewnljscnzxjkcfairw"
			+ "6943rhewldscnsodfuyoilkndsyrupornwekitcew9or;nwcigblw819ei23-t05860";;
	static String gibberish = GIBBERISH;
	static {
		int baggageSize = Config.getGlobalInt(TC.REQUEST_BAGGAGE_SIZE);
		if (gibberish.length() > baggageSize)
			gibberish = gibberish.substring(0, baggageSize);
		else
			while (gibberish.length() < baggageSize)
				gibberish += (baggageSize > 2 * gibberish.length() ? gibberish
						: gibberish.substring(0,
								baggageSize - gibberish.length()));
		Util.assertAssertionsEnabled();
		assert (gibberish.length() == baggageSize);
	}

	/**
	 * @return Literally gibberish.
	 */
	public static String getGibberish() {
		return gibberish;
	}

	private RequestPacket makeRequest() {
		int reqID = ((int) (Math.random() * Integer.MAX_VALUE));
		RequestPacket req = new RequestPacket(reqID,
				"Sample request numbered " + getTotalRequestCount() + ":"
						+ gibberish, false);
		return req;
	}

	private static final String TEST_GUID = Config
			.getGlobalString(TC.TEST_GUID);

	private RequestPacket makeRequest(String paxosID) {
		RequestPacket req = this.makeRequest();
		req.putPaxosID(paxosID != null ? paxosID : TEST_GUID, 0);
		return req;
	}

	protected boolean makeAndSendRequest(String paxosID) throws JSONException,
			IOException {
		RequestPacket req = this.makeRequest(paxosID);
		return this.sendRequest(req);
	}

	protected static TESTPaxosClient[] setupClients(InterfaceNodeConfig<Integer> nc) {
		System.out.println("\n\nInitiating paxos clients setup");
		TESTPaxosClient[] clients = new TESTPaxosClient[Config
				.getGlobalInt(TC.NUM_CLIENTS)];
		for (int i = 0; i < Config.getGlobalInt(TC.NUM_CLIENTS); i++) {
			try {
				clients[i] = new TESTPaxosClient(
						Config.getGlobalInt(TC.TEST_CLIENT_ID) + i, nc);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		System.out.println("Completed initiating "
				+ Config.getGlobalInt(TC.NUM_CLIENTS) + " clients");
		return clients;
	}

	private static double TOTAL_LOAD = Config.getGlobalDouble(TC.TOTAL_LOAD);
	private static int NUM_GROUPS = Config.getGlobalInt(TC.NUM_GROUPS);
	private static final String TEST_GUID_PREFIX = Config.getGlobalString(TC.TEST_GUID_PREFIX);

	private static double mostRecentSentRate = 0;
	protected static void sendTestRequests(int numReqsPerClient,
			TESTPaxosClient[] clients) throws JSONException, IOException {
		System.out.print("\nInitiating test sending " + numReqsPerClient
				* clients.length + " requests using " + clients.length
				+ " clients at an aggregate load of " + (TOTAL_LOAD)
				+ " reqs/sec...");
		RateLimiter r = new RateLimiter(TOTAL_LOAD);
		long initTime = System.currentTimeMillis();
		for (int i = 0; i < numReqsPerClient; i++) {
			for (int j = 0; j < clients.length; j++) {
				int curTotalReqs = j + i * clients.length;
				while (!clients[j].makeAndSendRequest(TEST_GUID_PREFIX
						+ ((RANDOM_REPLAY + curTotalReqs) % (NUM_GROUPS))))
					;
				r.record();
			}
		}
		
		mostRecentSentRate = numReqsPerClient * clients.length * 1000.0
				/ (System.currentTimeMillis() - initTime);

		System.out.println("done sending requests in "
				+ Util.df((System.currentTimeMillis() - initTime) / 1000.0)
				+ " secs; actual sending rate = "
				+ Util.df(mostRecentSentRate));
	}

	protected static void waitForResponses(TESTPaxosClient[] clients,
			long startTime) {
		for (int i = 0; i < Config.getGlobalInt(TC.NUM_CLIENTS); i++) {
			while (clients[i].requests.size() > 0) {
				synchronized (clients[i]) {
					if (clients[i].requests.size() > 0)
						try {
							clients[i].wait(4000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
				}
				System.out
						.println(getWaiting(clients)
								+ (getRtxCount() > 0 ? "; #num_total_retransmissions = "
								+ getRtxCount() : "")
								+ (getRtxCount() > 0 ? "; num_retransmitted_requests = "
										+ getNumRtxReqs()
										: "")
								+ "; aggregate response rate = "
								+ Util.df(getTotalThroughput(clients, startTime))
								+ " reqs/sec");
				if (clients[i].requests.size() > 0)
					try {
						Thread.sleep(1000);
						//System.out.println(DelayProfiler.getStats());
					} catch (Exception e) {
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

	private static double getTotalThroughput(TESTPaxosClient[] clients,
			long startTime) {
		int totalExecd = 0;
		for (int i = 0; i < clients.length; i++) {
			totalExecd += clients[i].getRunReplyCount();
		}

		return totalExecd * 1000.0 / (System.currentTimeMillis() - startTime);
	}

	protected static void printOutput(TESTPaxosClient[] clients) {
		for (int i = 0; i < Config.getGlobalInt(TC.NUM_CLIENTS); i++) {
			if (clients[i].requests.isEmpty()) {
				System.out.println("\n\nSUCCESS! requests issued = "
						+ clients[i].getTotalRequestCount()
						+ "; requests turned to no-ops = "
						+ clients[i].getNoopCount() + "; responses received = "
						+ clients[i].getTotalReplyCount() + "\n");
			} else
				System.out.println("\nFAILURE: Requests issued = "
						+ clients[i].getTotalRequestCount()
						+ "; requests turned to no-ops = "
						+ clients[i].getNoopCount() + "; responses received = "
						+ clients[i].getTotalReplyCount() + "\n");
		}
	}

	protected static String getAggregateOutput(int numReqs, long delay) {
		return "\n  average_sent_rate = " + Util.df(mostRecentSentRate) +"/s"
				+ "\n  average_response_time = "
				+ Util.df(TESTPaxosClient.getAvgLatency()) + "ms"
				+ "\n  noop_count = " + TESTPaxosClient.getTotalNoopCount();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			TESTPaxosConfig.setConsoleHandler();
			TESTPaxosConfig.setDistribtedTest(args.length > 0 ? args[0] : null);

			TESTPaxosClient[] clients = TESTPaxosClient.setupClients(TESTPaxosConfig.getFromPaxosConfig());
			int numReqs = Config.getGlobalInt(TC.NUM_REQUESTS)
					/ Config.getGlobalInt(TC.NUM_CLIENTS);

			// begin first run
			long t1 = System.currentTimeMillis();
			sendTestRequests(numReqs, clients);
			waitForResponses(clients, t1);
			long t2 = System.currentTimeMillis();
			System.out.println("\n[run1]"
					+ getAggregateOutput(numReqs, t2 - t1));
			// end first run

			resetLatencyComputation(clients);
			Thread.sleep(1000);

			// begin second run
			t1 = System.currentTimeMillis();
			sendTestRequests(numReqs, clients);
			waitForResponses(clients, t1);
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
