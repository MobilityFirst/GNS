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
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
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

	private  JSONNIOTransport<Integer> niot;
	private final NodeConfig<Integer> nc;
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
					long latency = System.currentTimeMillis()
							- requestCreateTimes.get(request.requestID);
					client.incrReplyCount();
					//assert (requestCreateTimes.containsKey(request.requestID)) : request;
					TESTPaxosClient.log.log(Level.FINE,
							"Client {0} received response #{1} with latency {2} [{3}] : {4}",
							new Object[] { client.myID,
									client.getTotalReplyCount(), latency,
									request.getDebugInfo(),
									request.getSummary() });
					
					DelayProfiler.updateInterArrivalTime("response_rate", 1, 100);
					//DelayProfiler.updateRate("response_rate2", 1000, 10);

					if (Util.oneIn(NUM_REQUESTS*1000)) // disabled
						System.out.println("Instantaneous response_rate1 = "
								+ Util.df(DelayProfiler
										.getThroughput("response_rate1"))
								+ "/s; response_rate2 = "
								+ Util.df(DelayProfiler
										.getRate("response_rate2"))
								+ "/s");
					updateLatency(latency);
					synchronized (client) {
						client.notify();
					}
				} else {
					TESTPaxosClient.log.log(Level.FINE,
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
					sendToID(id, req.toJSONObject());
					timer.schedule(new Retransmitter(id, req, timeout * 2),
							(long) (timeout * 2));
				}
			} catch (IOException | JSONException e) {
				e.printStackTrace();
			}
		}
	}

	protected TESTPaxosClient(int id, NodeConfig<Integer> nc) throws IOException {
		this.myID = id;
		this.nc = (nc==null ? TESTPaxosConfig.getNodeConfig() : nc);
		niot = (new JSONNIOTransport<Integer>(id, this.nc,
				(new ClientPacketDemultiplexer(this)), true));
		this.timer = new Timer(TESTPaxosClient.class.getSimpleName() + myID);
	}
	

	private static final boolean PIN_CLIENT = Config.getGlobalBoolean(TC.PIN_CLIENT);
	private boolean sendRequest(RequestPacket req) throws IOException,
			JSONException {
		int[] group = TESTPaxosConfig.getGroup(req.getPaxosID());
		int index = !PIN_CLIENT ? (int) (req.requestID % group.length)
				: (int) (myID % group.length);
		while (index < 0 || index >= group.length
				|| TESTPaxosConfig.isCrashed(group[index])) {
			assert(false);
			index = (int) (Math.random() * group.length);
		}
		return this.sendRequest(group[index], req);
	}
	
	static ConcurrentHashMap<Integer,Integer> reqCounts = new ConcurrentHashMap<Integer,Integer>();
	synchronized static void urc(int id) {
		reqCounts.putIfAbsent(id, 0);
		reqCounts.put(id, reqCounts.get(id)+1);
	}

	private static final int CLIENT_PORT_OFFSET = Config.getGlobalInt(PC.CLIENT_PORT_OFFSET);
	//private static InterfaceNodeConfig<Integer> nc = TESTPaxosConfig.getFromPaxosConfig(true);
	protected boolean sendRequest(int id, RequestPacket req)
			throws IOException, JSONException {
		assert(nc.getNodeAddress(id)!=null) : id;
		log.log(Level.FINE, "Sending request to node {0}: {1}", new Object[] {
				id, nc.getNodeAddress(id)+":"+nc.getNodePort(id), req.getSummary() });
		if (this.requests.put(req.requestID, req) != null)
			return false; // collision in integer space
		this.incrReqCount();
		this.requestCreateTimes.put(req.requestID,
				System.currentTimeMillis());

		// no retransmission send
		while (this.sendToID(id, req.toJSONObject()) <= 0) {
			try {
				Thread.sleep(req.lengthEstimate()/RequestPacket.SIZE_ESTIMATE + 1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		urc(id);
		// retransmit if enabled
		if (TESTPaxosConfig.ENABLE_CLIENT_REQ_RTX)
			this.timer
					.schedule(new Retransmitter(id, req), (long) getTimeout());
		return true;
	}
	
	private int sendToID(int id, JSONObject json) throws IOException {
		return this.niot.sendToAddress(
				CLIENT_PORT_OFFSET > 0 ? new InetSocketAddress(this.nc
						.getNodeAddress(id), this.nc.getNodePort(id)
						+ CLIENT_PORT_OFFSET) : new InetSocketAddress(this.nc
						.getNodeAddress(id), this.nc.getNodePort(id)
						+ CLIENT_PORT_OFFSET), json);
	}

	private static final String GIBBERISH = "89432hoicnbsd89233u2eoiwdj-329hbousfnc";
	static String gibberish = Config.getGlobalBoolean(TC.COMPRESSIBLE_REQUEST) ? createGibberishCompressible()
			: createGibberish();
	
	private static final String createGibberishCompressible() {
		gibberish = GIBBERISH;
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
		return gibberish;
	}
	
	private static final String createGibberish() {
		int baggageSize = Config.getGlobalInt(TC.REQUEST_BAGGAGE_SIZE);
		byte[] buf = new byte[baggageSize];
		byte[] chars = Util.getAlphanumericAsBytes();
		for(int i=0; i<baggageSize; i++) buf[i] = (chars[(int)(Math.random()*chars.length)]);
		gibberish = new String(buf);
		if (gibberish.length() > baggageSize)
			gibberish = gibberish.substring(0, baggageSize);
		else
			gibberish += gibberish.substring(0,
					baggageSize - gibberish.length());
		Util.assertAssertionsEnabled();
		assert (gibberish.length() == baggageSize);
		return gibberish;
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
		// createGibberish(), // randomly create each string
				gibberish, false);
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

	protected static TESTPaxosClient[] setupClients(NodeConfig<Integer> nc) {
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

	protected static void sendTestRequests(int numReqsPerClient,
			TESTPaxosClient[] clients) throws JSONException, IOException {
		sendTestRequests(numReqsPerClient, clients, false);
	}
	private static final int NUM_CLIENTS = Config.getGlobalInt(TC.NUM_CLIENTS);
	private static double mostRecentSentRate = 0;
	protected static void sendTestRequests(int numReqs,
			TESTPaxosClient[] clients, boolean warmup) throws JSONException, IOException {
		System.out.print((warmup ? "\nWarming up " : "\nTesting ")
				+ "[#requests="
				+ numReqs
				+ ", request_size="
				+ gibberish.length()
				+ "B, #clients="
				+ clients.length
				+ ", #groups="
				+ NUM_GROUPS
				+ ", load=" + TOTAL_LOAD + "/s" + "]...");
		RateLimiter r = new RateLimiter(TOTAL_LOAD);
		long initTime = System.currentTimeMillis();
		for (int i = 0; i < numReqs; i++) {
			while (!clients[i%NUM_CLIENTS].makeAndSendRequest(TEST_GUID_PREFIX
					+ ((RANDOM_REPLAY + i) % (NUM_GROUPS))))
				;
			r.record();
		}
		
		mostRecentSentRate = numReqs * 1000.0
				/ (System.currentTimeMillis() - initTime);

		System.out
				.println("done "
						+ (warmup ? ""
								: "sending "
										+ numReqs
										+ " requests in "
										+ Util.df((System.currentTimeMillis() - initTime) / 1000.0)
										+ " secs; actual sending rate = "
										+ Util.df(mostRecentSentRate) + "/s"
						 + " \n " + reqCounts
						));
	}
	protected static void waitForResponses(TESTPaxosClient[] clients,
			long startTime) {
		waitForResponses(clients, startTime, false);
	}
	
	protected static void waitForResponses(TESTPaxosClient[] clients,
			long startTime, boolean warmup) {
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
						.println("["
								+ clients[i].myID
								+ "] "
								+ getWaiting(clients)
								+ (getRtxCount() > 0 ? "; #num_total_retransmissions = "
										+ getRtxCount()
										: "")
								+ (getRtxCount() > 0 ? "; num_retransmitted_requests = "
										+ getNumRtxReqs()
										: "")
								+ (!warmup ? "; aggregate response rate = "
										+ Util.df(getTotalThroughput(clients,
												startTime)) + " reqs/sec" : ""));
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
				System.out.println("\n\n[SUCCESS] requests issued = "
						+ clients[i].getTotalRequestCount()
						+ "; requests turned to no-ops = "
						+ clients[i].getNoopCount() + "; responses received = "
						+ clients[i].getTotalReplyCount() + "\n");
			} else
				System.out.println("\n[FAILURE]: Requests issued = "
						+ clients[i].getTotalRequestCount()
						+ "; requests turned to no-ops = "
						+ clients[i].getNoopCount() + "; responses received = "
						+ clients[i].getTotalReplyCount() + "\n");
		}
	}

	protected static String getAggregateOutput(long delay) {
		return "\n  average_sent_rate = " + Util.df(mostRecentSentRate) +"/s"
				+ "\n  average_response_time = "
				+ Util.df(TESTPaxosClient.getAvgLatency()) + "ms"
				+ "\n  noop_count = " + TESTPaxosClient.getTotalNoopCount();
	}
	
	private static final int NUM_REQUESTS = Config.getGlobalInt(TC.NUM_REQUESTS);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			TESTPaxosConfig.setConsoleHandler();
			NIOTransport.setUseSenderTask(Config.getGlobalBoolean(PC.USE_NIO_SENDER_TASK));
			TESTPaxosConfig.setDistribtedTest(args.length > 0 ? args[0] : null);

			TESTPaxosClient[] clients = TESTPaxosClient.setupClients(TESTPaxosConfig.getFromPaxosConfig(true));
			System.out.println(TESTPaxosConfig.getFromPaxosConfig(true));
			int numReqs = Config.getGlobalInt(TC.NUM_REQUESTS);

			
			// begin warmup run
			long t1 = System.currentTimeMillis();
			sendTestRequests(Math.min(numReqs, 10*NUM_CLIENTS), clients, true);
			waitForResponses(clients, t1);
			long t2 = System.currentTimeMillis();
			System.out.println("[success]");
			// end warmup run
			
			resetLatencyComputation(clients);
			Thread.sleep(1000);

			// begin first run
			t1 = System.currentTimeMillis();
			sendTestRequests(numReqs, clients);
			waitForResponses(clients, t1);
			t2 = System.currentTimeMillis();
			System.out.println("\n[run1]"
					+ getAggregateOutput(t2 - t1));
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
					+ getAggregateOutput(t2 - t1));
			// end second run

			for (TESTPaxosClient client : clients) {
				client.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
