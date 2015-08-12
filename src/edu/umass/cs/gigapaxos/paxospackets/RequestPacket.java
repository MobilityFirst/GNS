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
package edu.umass.cs.gigapaxos.paxospackets;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.RequestBatcher;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public class RequestPacket extends PaxosPacket implements InterfaceRequest {
	static {
		PaxosConfig.load();
	}
	private static final boolean DEBUG = Config.getGlobalBoolean(PC.DEBUG);
	public static final String NO_OP = InterfaceRequest.NO_OP;

	/**
	 * These JSON keys are rather specific to RequestPacket or for debugging, so
	 * they are here as opposed to PaxosPacket. Application developers don't
	 * have to worry about these.
	 */
	protected static enum Keys {
		/**
		 * True if stop request.
		 */
		STOP,

		/**
		 * Create time.
		 */
		CT,

		/**
		 * Entry time at the first server.
		 */
		ET,

		/**
		 * Number of forwards.
		 */
		NFWDS,

		/**
		 * Most recent forwarder.
		 */
		FWDR,

		/**
		 * DEBUG mode.
		 */
		DBG,

		/**
		 * Request ID.
		 */
		QID,

		/**
		 * Request value.
		 */
		QVAL,

		/**
		 * Client socket address.
		 */
		CSA,

		/**
		 * Boolean flag to specify whether this request came into the system as
		 * a string or RequestPacket.
		 */
		RVAL,

		/**
		 * Batched requests.
		 */
		BATCH
	}

	private static final int MAX_FORWARD_COUNT = 3;
	private static final Random random = new Random();

	/**
	 * A unique requestID for each request. Paxos doesn't actually check or care
	 * whether two requests with the same ID are identical. This field is useful
	 * for asynchronous clients to associate responses with requests.
	 */
	public final int requestID;
	/**
	 * The actual request body. The client will get back this string if that is
	 * what it sent to paxos. If it issued a RequestPacket, then it will get
	 * back the whole RequestPacket back.
	 */
	public final String requestValue;
	/**
	 * Whether this request is a stop request.
	 */
	public final boolean stop;

	// the client address in string form
	private InetSocketAddress clientAddress = null;

	// the replica that first received this request
	private int entryReplica = -1;

	/*
	 * Whether to return requestValue or this.toString() back to client. We need
	 * this in order to distinguish between clients that send RequestPacket
	 * requests from clients that directly propose a string request value
	 * through PaxosManager.
	 */
	private boolean shouldReturnRequestValue = false;

	// needed to stop ping-ponging under coordinator confusion
	private int forwardCount = 0;

	// batch of requests attached to this request
	private RequestPacket[] batched = null;

	// used to optimized batching
	private long entryTime = System.currentTimeMillis();

	/*
	 * These fields are for testing and debugging. They are preserved across
	 * forwarding by nodes, so they are not final
	 */
	// included always
	// private long createTime = System.currentTimeMillis();
	// included only in DEBUG mode
	private String debugInfo = null;
	// included only in DEBUG mode
	private int forwarderID = -1;

	// let a random request ID be picked
	public RequestPacket(String value, boolean stop) {
		this(random.nextInt(), value, stop);
	}

	// the common-case constructor
	public RequestPacket(int reqID, String value, boolean stop) {
		this(reqID, value, stop, null);
	}

	// called by inheritors
	public RequestPacket(RequestPacket req) {
		this(req.requestID, req.requestValue, req.stop, req);
	}

	// used by makeNoop to convert req to a noop
	public RequestPacket(int reqID, String value, boolean stop,
			RequestPacket req) {
		super(req); // will take paxosID and version from req

		// final fields
		this.packetType = PaxosPacketType.REQUEST;
		// this.clientID = clientID;
		this.requestID = reqID;
		this.requestValue = value;
		this.stop = stop;

		if (req == null)
			return;

		// non-final fields
		this.entryReplica = req.entryReplica;
		this.clientAddress = req.clientAddress;
		this.shouldReturnRequestValue = req.shouldReturnRequestValue;

		// debug/testing fields
		// this.createTime = req.createTime;
		this.entryTime = req.entryTime;
		this.forwardCount = req.forwardCount;
		this.forwarderID = req.forwarderID;
		this.debugInfo = req.debugInfo;
		this.batched = req.batched;
	}

	public RequestPacket makeNoop() {
		RequestPacket noop = new RequestPacket(requestID, NO_OP, stop, this);
		// make batched requests noop as well
		for (int i = 0; this.batched != null && i < this.batched.length; i++)
			this.batched[i] = this.batched[i].makeNoop();
		// and put them inside the newly minted noop
		noop.batched = this.batched;
		return noop;
	}

	public RequestPacket setReturnRequestValue() {
		this.shouldReturnRequestValue = true;
		return this;
	}

	public int getClientID() {
		if (this.clientAddress != null)
			return this.clientAddress.getPort();
		return -1;
	}

	public boolean isNoop() {
		return this.requestValue.equals(NO_OP);
	}

	private void incrForwardCount() {
		this.forwardCount++;
	}

	public int getForwardCount() {
		return this.forwardCount;
	}

	public RequestPacket setEntryReplica(int id) {
		if (this.entryReplica == -1) // one-time
			this.entryReplica = id;
		if (this.isBatched())
			for (RequestPacket req : this.batched)
				req.setEntryReplica(id); // recursive
		return this;
	}

	public int getEntryReplica() {
		return this.entryReplica;
	}

	public RequestPacket setForwarderID(int id) {
		this.forwarderID = id;
		this.incrForwardCount();
		return this;
	}

	public int getForwarderID() {
		return this.forwarderID;
	}

	private static String makeDebugInfo(String str, long cTime) {
		return " " + str + ":" + (System.currentTimeMillis() - cTime);
	}

	public void addDebugInfo(String str) {
		if (DEBUG)
			this.debugInfo = (this.debugInfo == null ? "" : this.debugInfo)
					+ makeDebugInfo(str, this.getEntryTime());
	}

	public static boolean addDebugInfo(JSONObject msg, String str)
			throws JSONException {
		String debug = "";
		boolean added = false;
		if (msg.has(Keys.DBG.toString()) && msg.has(Keys.CT.toString())) {
			debug = msg.getString(Keys.DBG.toString())
					+ makeDebugInfo(str, msg.getLong(Keys.CT.toString()));
			added = true;
			msg.put(Keys.DBG.toString(), debug);
		}
		return added;
	}

	public String getDebugInfo() {
		return " [" + this.debugInfo + "] ";
	}

	public static boolean isPingPonging(JSONObject msg) {
		try {
			if (msg.has(Keys.NFWDS.toString())
					&& msg.getInt(Keys.NFWDS.toString()) > MAX_FORWARD_COUNT) {
				return true;
			}
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return false;
	}

	public boolean isPingPonging() {
		return this.forwardCount > MAX_FORWARD_COUNT;
	}

	public RequestPacket(JSONObject json) throws JSONException {
		super(json);
		this.packetType = PaxosPacketType.REQUEST;
		this.stop = json.optBoolean(Keys.STOP.toString());
		this.requestID = json.getInt(Keys.QID.toString());
		// this.clientID = (json.has(Keys.CID.toString()) ?
		// json.getInt(Keys.CID.toString()) : -1);
		this.requestValue = json.getString(Keys.QVAL.toString());
		// this.createTime = json.getLong(Keys.CTIME.toString());
		this.entryTime = json.getLong(Keys.ET.toString());
		this.forwardCount = (json.has(Keys.NFWDS.toString()) ? json
				.getInt(Keys.NFWDS.toString()) : 0);
		this.forwarderID = (json.has(RequestPacket.Keys.FWDR.toString()) ? json
				.getInt(RequestPacket.Keys.FWDR.toString()) : -1);
		this.debugInfo = (json.has(Keys.DBG.toString()) ? json
				.getString(Keys.DBG.toString()) : "");

		this.clientAddress = (json.has(Keys.CSA.toString()) ? Util
				.getInetSocketAddressFromString(json.getString(Keys.CSA
						.toString())) : JSONNIOTransport.getSenderAddress(json));
		this.entryReplica = json.getInt(PaxosPacket.NodeIDKeys.E.toString());
		this.shouldReturnRequestValue = json.optBoolean(Keys.RVAL.toString());
		// unwrap latched along batch
		JSONArray batchedJSON = json.has(Keys.BATCH.toString()) ? json
				.getJSONArray(Keys.BATCH.toString()) : null;
		if (batchedJSON != null && batchedJSON.length() > 0) {
			this.batched = new RequestPacket[batchedJSON.length()];
			for (int i = 0; i < batchedJSON.length(); i++) {
				this.batched[i] = new RequestPacket(
						(JSONObject) batchedJSON.get(i));
			}
		}
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		// json.put(Keys.CID.toString(), clientID);
		json.put(Keys.QID.toString(), this.requestID);
		json.put(Keys.QVAL.toString(), this.requestValue);
		// json.put(Keys.CTIME.toString(), this.createTime);
		json.put(Keys.ET.toString(), this.entryTime);
		if (forwardCount > 0)
			json.put(Keys.NFWDS.toString(), this.forwardCount);
		if (this.stop)
			json.put(Keys.STOP.toString(), this.stop);
		if (DEBUG) {
			json.put(RequestPacket.Keys.FWDR.toString(), this.forwarderID);
			json.putOpt(Keys.DBG.toString(), this.debugInfo);
		}
		json.put(PaxosPacket.NodeIDKeys.E.toString(), this.entryReplica);
		if (this.clientAddress != null)
			json.put(Keys.CSA.toString(), this.clientAddress);
		if (this.shouldReturnRequestValue)
			json.put(Keys.RVAL.toString(), this.shouldReturnRequestValue);
		// convert latched along batch to json array
		if (this.batched != null && this.batched.length > 0) {
			JSONArray batchedJSON = new JSONArray();
			for (int i = 0; i < this.batched.length; i++) {
				batchedJSON.put(this.batched[i].toJSONObject());
			}
			json.put(Keys.BATCH.toString(), batchedJSON);
		}
		return json;
	}

	// only for size estimation
	private RequestPacket setClientAddress(InetSocketAddress sockAddr) {
		this.clientAddress = sockAddr;
		return this;
	}

	public InetSocketAddress getClientAddress() {
		return this.clientAddress;
	}

	public boolean isStopRequest() {
		return stop || this.isAnyBatchedRequestStop();
	}

	private boolean isAnyBatchedRequestStop() {
		if (this.batchSize() == 0)
			return false;
		for (RequestPacket req : this.batched)
			if (req.isStopRequest())
				return true;
		return false;
	}

	/* For testing */
	public static int getRequestID(String req) {
		String[] pieces = req.split("\\s");
		return (pieces != null && pieces.length >= 6 ? Integer
				.parseInt(pieces[5]) : -1);
	}

	public RequestPacket setEntryTime(long t) {
		this.entryTime = t;
		if (this.isBatched())
			for (RequestPacket req : this.batched)
				req.setEntryTime(t);
		return this;
	}

	public long getEntryTime() {
		return this.entryTime;
	}

	private boolean isBatched() {
		return this.batchSize() > 0;
	}

	public RequestPacket latchToBatch(RequestPacket[] reqs) {
		// first flatten out the argument
		RequestPacket[] allThreaded = toArray(reqs);
		if (this.batched == null)
			this.batched = allThreaded;
		else
			this.batched = concatenate(this.batched, allThreaded);
		for (int i = 0; i < this.batched.length; i++)
			assert (!this.batched[i].isBatched());
		return this;
	}

	private static RequestPacket[] concatenate(RequestPacket[] a,
			RequestPacket[] b) {
		RequestPacket[] c = new RequestPacket[a.length + b.length];
		for (int i = 0; i < a.length; i++)
			c[i] = a[i];
		for (int i = 0; i < b.length; i++)
			c[a.length + i] = b[i];
		return c;
	}

	/*
	 * Returns this request unraveled as an array wherein each element is an
	 * unbatched request.
	 * 
	 * Note: This operation is not idempotent because batched gets reset to
	 * null.
	 */
	private RequestPacket[] toArray() {
		RequestPacket[] array = new RequestPacket[1 + this.batchSize()];
		array[0] = this;
		for (int i = 0; i < this.batchSize(); i++) {
			array[i + 1] = this.batched[i];
			assert (!this.batched[i].isBatched());
		}
		// toArray always returns an array of unbatched packets
		this.batched = null;
		return array;
	}

	/*
	 * Converts an array of possibly batched requests to a single unraveled
	 * array wherein each request is unbatched.
	 */
	private static RequestPacket[] toArray(RequestPacket[] reqs) {
		ArrayList<RequestPacket[]> reqArrayList = new ArrayList<RequestPacket[]>();
		int totalSize = 0;
		for (RequestPacket req : reqs) {
			RequestPacket[] reqArray = req.toArray();
			totalSize += reqArray.length;
			reqArrayList.add(reqArray);
		}
		assert (totalSize == size(reqArrayList));
		RequestPacket[] allThreaded = new RequestPacket[totalSize];
		int count = 0;
		for (RequestPacket[] reqArray : reqArrayList) {
			for (int j = 0; j < reqArray.length; j++) {
				assert (!reqArray[j].isBatched());
				allThreaded[count++] = reqArray[j];
			}
		}
		assert (count == totalSize) : count + " != " + totalSize
				+ " while unraveling " + print(reqArrayList);
		return allThreaded;
	}

	private static int size(ArrayList<RequestPacket[]> reqArrayList) {
		int size = 0;
		for (RequestPacket[] reqArray : reqArrayList)
			size += reqArray.length;
		return size;
	}

	public RequestPacket[] getBatched() {
		return this.batched;
	}

	// gets only the first request without the batch
	public RequestPacket getFirstOnly() {
		if (this.batchSize() == 0)
			return this;
		// else
		RequestPacket req = new RequestPacket(this);
		req.batched = null;
		return req;
	}

	private static String print(ArrayList<RequestPacket[]> reqArrayList) {
		String s = "[\n";
		int count = 0;
		for (RequestPacket[] reqArray : reqArrayList) {
			s += "req" + count++ + " = \n[\n";
			for (RequestPacket req : reqArray) {
				s += "    " + req + "\n";
			}
			s += "]\n";

		}
		return s;
	}

	public String[] getRequestValues() {
		String[] reqValues = null;
		if (this.shouldReturnRequestValue) {
			reqValues = new String[this.batchSize() + 1];
			reqValues[0] = this.requestValue;
			if (this.batched != null)
				for (int i = 0; i < this.batched.length; i++) {
					reqValues[i + 1] = this.batched[i].requestValue;
					assert (this.batched[i].batched == null);
				}
		} else {
			reqValues = new String[1];
			reqValues[0] = toString();
		}
		return reqValues;
	}

	public int batchSize() {
		return this.batched != null ? this.batched.length : 0;
	}

	/*
	 * This ugly method is used only for testing and is needed in order to
	 * separate requests that first entered the requests (or requests with
	 * entryReplica==-1) from the rest in a batched request.
	 */
	public RequestPacket[] getNoEntryReplicaRequestsAsBatch() {
		RequestPacket[] reqArray = this.toArray(); // all unbatched

		List<RequestPacket> noEntryReplicaRequests = new LinkedList<RequestPacket>();
		List<RequestPacket> entryReplicaRequests = new LinkedList<RequestPacket>();

		for (int i = 0; i < reqArray.length; i++)
			if (reqArray[i].getEntryReplica() == -1)
				noEntryReplicaRequests.add(reqArray[i]);
			else
				entryReplicaRequests.add(reqArray[i]);

		// [0] is old, [1] is new
		RequestPacket[] retRequests = new RequestPacket[2];
		if (entryReplicaRequests.size() > 0) {
			// else requests with no entry replica exist
			RequestPacket entryReplicaRequest = entryReplicaRequests.remove(0);
			if (!entryReplicaRequests.isEmpty())
				entryReplicaRequest.latchToBatch(noEntryReplicaRequests
						.toArray(new RequestPacket[0]));
			retRequests[0] = entryReplicaRequest;
		}
		if (noEntryReplicaRequests.size() > 0) {
			// else requests with no entry replica exist
			RequestPacket noEntryReplicaRequest = noEntryReplicaRequests
					.remove(0);
			if (!noEntryReplicaRequests.isEmpty())
				noEntryReplicaRequest.latchToBatch(noEntryReplicaRequests
						.toArray(new RequestPacket[0]));
			retRequests[1] = noEntryReplicaRequest;
		}
		return retRequests;
	}

	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return this.getType();
	}

	@Override
	public String getServiceName() {
		return this.paxosID;
	}

	@Override
	protected String getSummaryString() {
		return requestID + ":" + "["
				+ (NO_OP.equals(this.requestValue) ? NO_OP : "...")
				// Util.truncate(requestValue, 16, 16)
				+ "]" + (stop ? ":STOP" : "")
				+ (isBatched() ? "+(" + batchSize() + " batched" + ")" : "");
	}

	/**
	 * We need this estimate to use it in {@link RequestBatcher#dequeueImpl()}.
	 * The value needs to be an upper bound on the sum total of all of the gunk
	 * in PValuePacket other than the requestValue itself, i.e., the size of a
	 * no-op decision.
	 */
	private static final int SIZE_ESTIMATE;
	static PValuePacket samplePValue = null;
	static {
		int length = 0;
		try {
			samplePValue = new PValuePacket(new Ballot(23, 2178),
					new ProposalPacket(3142,
							new RequestPacket(43437, "hello world", true)
									.setClientAddress(new InetSocketAddress(
											"128.119.245.40", 2000))));
			samplePValue.putPaxosID("paxos0", 0);
			String str = samplePValue.toJSONObject().toString();
			length = str.length();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		// 25% extra for other miscellaneous additions
		SIZE_ESTIMATE = (int) (length * 1.25);
	}

	public boolean hasRequestValue() {
		return this.requestValue != null;
	}

	/*
	 * Need an upper bound here for limiting batch size. Currently all the
	 * fields in RequestPacket other than requestValue add up to around 270.
	 */
	public int lengthEstimate() {
		return this.requestValue.length() + SIZE_ESTIMATE;
	}

	public String printBatched() {
		String s = "";
		s += this.getSummary();
		if (this.batchSize() > 0)
			for (int i = 0; i < this.batchSize(); i++)
				s += "\n  " + this.batched[i].getSummary();
		return s;
	}

	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		int numReqs = 25;
		RequestPacket[] reqs = new RequestPacket[numReqs];
		RequestPacket req = new RequestPacket("asd" + 999, true);
		for (int i = 0; i < numReqs; i++) {
			reqs[i] = new RequestPacket("asd" + i, true);
		}

		System.out.println("Decision size estimate = " + SIZE_ESTIMATE);

		req.latchToBatch(reqs);
		String reqStr = req.toString();
		try {
			RequestPacket reqovered = new RequestPacket(req.toJSONObject());
			String reqoveredStr = reqovered.toString();
			assert (reqStr.equals(reqoveredStr));
			System.out.println("batchSize = " + reqovered.batched.length);
			// System.out.println(reqovered.batched[3]);
			System.out.println(samplePValue);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
