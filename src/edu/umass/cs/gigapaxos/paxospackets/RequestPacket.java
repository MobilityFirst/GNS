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

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.RequestBatcher;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public class RequestPacket extends PaxosPacket implements Request,
		ClientRequest {
	static {
		PaxosConfig.load();
	}
	private static final boolean DEBUG = Config.getGlobalBoolean(PC.DEBUG);
	public static final String NO_OP = Request.NO_OP;

	/**
	 * These JSON keys are rather specific to RequestPacket or for debugging, so
	 * they are here as opposed to PaxosPacket. Application developers don't
	 * have to worry about these.
	 */
	public static enum Keys {
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
		QV,

		/**
		 * Client socket address.
		 */
		CSA,

		/**
		 * Boolean query flag to specify whether this request came into the system as
		 * a string or RequestPacket.
		 */
		QF,

		/**
		 * Batched requests.
		 */
		BATCH,

		/**
		 * Response value.
		 */
		RV, 
		
		/**
		 * Batch size.
		 */
		BS, 
				
		/**
		 * Stringified self
		 */
		STRINGIFIED,
		
		/**
		 * Meta request value. Unused.
		 */
		METAVAL,
	}

	public static enum ResponseCodes {
		/**
		 * 
		 */
		ACK,

		/**
		 * 
		 */
		NACK,
	}

	private static final int MAX_FORWARD_COUNT = 3;
	private static final Random random = new Random();

	/**
	 * A unique requestID for each request. Paxos doesn't actually check or care
	 * whether two requests with the same ID are identical. This field is useful
	 * for asynchronous clients to associate responses with requests.
	 * 
	 * FIXME: change to long.
	 */
	public final long requestID;
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
	private int entryReplica = IntegerMap.NULL_INT_NODE;

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
	
	private String stringifiedSelf = null;

	// used to optimized batching
	private long entryTime = System.currentTimeMillis();

	// response to be returned to client
	private String responseValue = null;

	/*
	 * These fields are for testing and debugging. They are preserved across
	 * forwarding by nodes, so they are not final
	 */
	// included only in DEBUG mode
	private String debugInfo = null;
	// included only in DEBUG mode
	private int forwarderID = IntegerMap.NULL_INT_NODE;

	// let a random request ID be picked
	public RequestPacket(String value, boolean stop) {
		this(random.nextInt(), value, stop);
	}

	// the common-case constructor
	public RequestPacket(long reqID, String value, boolean stop) {
		this(reqID, value, stop, null);
	}

	// called by inheritors
	public RequestPacket(RequestPacket req) {
		this(req.requestID, req.requestValue, req.stop, req);
	}

	// used by makeNoop to convert req to a noop
	public RequestPacket(long reqID, String value, boolean stop,
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
		this.responseValue = req.responseValue;

		// debug/testing fields
		this.entryTime = req.entryTime;
		this.forwardCount = req.forwardCount;
		this.forwarderID = req.forwarderID;
		this.debugInfo = req.debugInfo;
		this.batched = req.batched;
		this.stringifiedSelf = req.stringifiedSelf;
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

	// we also set entry time when we initialize entryReplica
	public RequestPacket setEntryReplica(int id) {
		if (this.entryReplica == IntegerMap.NULL_INT_NODE) {// one-time
			this.entryReplica = id;
			this.entryTime = System.currentTimeMillis();
		}
		if (this.isBatched())
			for (RequestPacket req : this.batched)
				// recursive but doesn't have to be
				req.setEntryReplica(id); 
		return this;
	}
	
	public void setEntryTime() {
		this.entryTime = System.currentTimeMillis();
		if(this.batchSize()>0) for(RequestPacket req : this.batched) req.setEntryTime();
	}

	public int setEntryReplicaAndReturnCount(int id) {
		int count = 0;
		if (this.entryReplica == IntegerMap.NULL_INT_NODE) {// one-time
			this.entryReplica = id;
			this.entryTime = System.currentTimeMillis();
			count++;
		}
		if (this.isBatched())
			for (RequestPacket req : this.batched)
				// recursive but doesn't have to be
				count += req.setEntryReplicaAndReturnCount(id); 
		return count;
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
		return str + ":" + (System.currentTimeMillis() - cTime) + " ";
	}

	public RequestPacket addDebugInfo(String str) {
		if (DEBUG)
			this.debugInfo = (this.debugInfo == null ? "" : this.debugInfo)
					+ makeDebugInfo(str, this.getEntryTime());
		return this;
	}

	public RequestPacket addDebugInfoDeep(String str) {
		if (DEBUG)
			if (this.addDebugInfo(str).batched != null)
				for (RequestPacket req : this.batched)
					req.addDebugInfo(str);
		return this;
	}

	public RequestPacket addDebugInfo(String str, int nodeID) {
		if (DEBUG)
			this.addDebugInfo(str + nodeID);
		return this;
	}
	public RequestPacket addDebugInfoDeep(String str, int nodeID) {
		if (DEBUG)
			this.addDebugInfoDeep(str + nodeID);
		return this;
	}

	public static boolean addDebugInfo(JSONObject msg, String str, int nodeID)
			throws JSONException {
		boolean added = false;
		if (DEBUG && msg.has(Keys.DBG.toString())
				&& msg.has(Keys.CT.toString())) {
			str = str + nodeID;
			String debug = msg.getString(Keys.DBG.toString())
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
		this.requestID = json.getLong(Keys.QID.toString());
		this.requestValue = json.getString(Keys.QV.toString());

		this.responseValue = json.has(Keys.RV.toString()) ? json
				.getString(Keys.RV.toString()) : null;
		this.entryTime = json.getLong(Keys.ET.toString());
		this.forwardCount = (json.has(Keys.NFWDS.toString()) ? json
				.getInt(Keys.NFWDS.toString()) : 0);
		this.forwarderID = (json.has(RequestPacket.Keys.FWDR.toString()) ? json
				.getInt(RequestPacket.Keys.FWDR.toString()) : IntegerMap.NULL_INT_NODE);
		this.debugInfo = (json.has(Keys.DBG.toString()) ? json
				.getString(Keys.DBG.toString()) : "");

		this.clientAddress = (json.has(Keys.CSA.toString()) ? Util
				.getInetSocketAddressFromString(json.getString(Keys.CSA
						.toString())) : JSONNIOTransport.getSenderAddress(json));
		this.entryReplica = json.getInt(PaxosPacket.NodeIDKeys.E.toString());
		this.shouldReturnRequestValue = json.optBoolean(Keys.QF.toString());
		
		// unwrap latched along batch
		JSONArray batchedJSON = json.has(Keys.BATCH.toString()) ? json
				.getJSONArray(Keys.BATCH.toString()) : null;
		if (batchedJSON != null && batchedJSON.length() > 0) {
			this.batched = new RequestPacket[batchedJSON.length()];
			for (int i = 0; i < batchedJSON.length(); i++) {
				this.batched[i] = new RequestPacket(
						(JSONObject) batchedJSON.get(i)
				// new JSONObject(batchedJSON.getString(i))
				);
			}
		}

		// we remembered the original string for recalling here
		this.stringifiedSelf = json.has(Keys.STRINGIFIED.toString()) ? (String) json
				.get(Keys.STRINGIFIED.toString()) : null;
	}
	//private static final boolean USE_JSON_SMART = !Config.getGlobalString(PC.JSON_LIBRARY).equals("org.json");
	
	public String getStringifiedSelf() {
		return this.stringifiedSelf;
	}
	public RequestPacket setStringifiedSelf(String s) {
		this.stringifiedSelf = s;
		//if(Util.oneIn(10)) DelayProfiler.updateMovAvg("stringified", s.length());
		return this;
	}

	// for comparing against a different json implementation
	public RequestPacket(net.minidev.json.JSONObject json) throws JSONException {
		super(json);
		assert (PaxosPacket.getPaxosPacketType(json) != PaxosPacketType.ACCEPT || json.containsKey(Keys.STRINGIFIED.toString()));
		this.packetType = PaxosPacketType.REQUEST;
		this.stop = json.containsKey(Keys.STOP.toString()) ?  (Boolean) json.get(Keys.STOP.toString()) : false;
		this.requestID = (long)(Integer)(json.get(Keys.QID.toString()));
		this.requestValue = (String) json.get(Keys.QV.toString());

		this.responseValue = json.containsKey(Keys.RV.toString()) ? (String) json
				.get(Keys.RV.toString()) : null;
		this.entryTime = (Long) json.get(Keys.ET.toString());
		this.forwardCount = (json.containsKey(Keys.NFWDS.toString()) ? (Integer) json
				.get(Keys.NFWDS.toString()) : 0);
		this.forwarderID = (json
				.containsKey(RequestPacket.Keys.FWDR.toString()) ? (Integer) json
				.get(RequestPacket.Keys.FWDR.toString()) : IntegerMap.NULL_INT_NODE);
		this.debugInfo = (json.containsKey(Keys.DBG.toString()) ? (String) json
				.get(Keys.DBG.toString()) : "");

		this.clientAddress = (json.containsKey(Keys.CSA.toString()) ? Util
				.getInetSocketAddressFromString((String) (json.get(Keys.CSA
						.toString()))) : JSONNIOTransport
				.getSenderAddressJSONSmart(json));
		this.entryReplica = (Integer) json.get(PaxosPacket.NodeIDKeys.E
				.toString());
                this.shouldReturnRequestValue = json.containsKey(Keys.QF.toString()) ?  (Boolean) json.get(Keys.QF.toString()) : false;
                
		// unwrap latched along batch
		Collection<?> batchedJSON = json.containsKey(Keys.BATCH.toString()) ? (Collection<?>) json
				.get(Keys.BATCH.toString()) : null;
		if (batchedJSON != null && batchedJSON.size() > 0) {
			this.batched = new RequestPacket[batchedJSON.size()];
			int i = 0;
			for (Object element : batchedJSON) {
				this.batched[i++] = new RequestPacket(
						(net.minidev.json.JSONObject) element
				// (net.minidev.json.JSONObject)net.minidev.json.JSONValue.parse((String)element)
				);
			}
		}

		// we remembered the original string for recalling here
		this.stringifiedSelf = json.containsKey(Keys.STRINGIFIED.toString()) ? (String) json
				.get(Keys.STRINGIFIED.toString()) : null;
		assert (PaxosPacket.getPaxosPacketType(json) != PaxosPacketType.ACCEPT || this.stringifiedSelf != null) : PaxosPacket
				.getPaxosPacketType(json) + ":" + json;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.QID.toString(), this.requestID);
		json.put(Keys.QV.toString(), this.requestValue);

		json.putOpt(Keys.RV.toString(), this.responseValue);
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
			json.put(Keys.QF.toString(), this.shouldReturnRequestValue);
		// convert latched along batch to json array
		if (this.batched != null && this.batched.length > 0) {
			JSONArray batchedJSON = new JSONArray();
			for (int i = 0; i < this.batched.length; i++) {
				batchedJSON.put(this.batched[i].toJSONObject());
				//batchedJSON.put(this.batched[i].toString());
			}
			json.put(Keys.BATCH.toString(), batchedJSON);
		}
		
		return json;
	}

	public net.minidev.json.JSONObject toJSONSmartImpl() throws JSONException {
		net.minidev.json.JSONObject json = new net.minidev.json.JSONObject();
		json.put(Keys.QID.toString(), this.requestID);
		json.put(Keys.QV.toString(), this.requestValue);

		if (this.responseValue != null)
			json.put(Keys.RV.toString(), this.responseValue);
		json.put(Keys.ET.toString(), this.entryTime);
		if (forwardCount > 0)
			json.put(Keys.NFWDS.toString(), this.forwardCount);
		if (this.stop)
			json.put(Keys.STOP.toString(), this.stop);
		if (DEBUG) {
			json.put(RequestPacket.Keys.FWDR.toString(), this.forwarderID);
			if (this.debugInfo != null)
				json.put(Keys.DBG.toString(), this.debugInfo);
		}
		json.put(PaxosPacket.NodeIDKeys.E.toString(), this.entryReplica);
		if (this.clientAddress != null)
			json.put(Keys.CSA.toString(), this.clientAddress.toString());
		if (this.shouldReturnRequestValue)
			json.put(Keys.QF.toString(), this.shouldReturnRequestValue);
		// convert latched along batch to json array
		if (this.batched != null && this.batched.length > 0) {
			net.minidev.json.JSONArray batchedJSON = new net.minidev.json.JSONArray();
			for (int i = 0; i < this.batched.length; i++) {
				batchedJSON.add(this.batched[i].toJSONSmart());
				//batchedJSON.add(this.batched[i].toString());
			}
			json.put(Keys.BATCH.toString(), batchedJSON);
		}
		
		return json;
	}
	
	/**
	 * Learned the hard way that using org.json to stringify is an order of
	 * magnitude slower with large request values compared to manually inserting
	 * the string like below. json-smart is fast enough for our purposes but
	 * still doesn't beat direct string construction like below.
	 * 
	 * Note: we need to account for batching carefully here. So, we use a json
	 * array of strings as opposed to json objects for batched requests above.
	 * Otherwise, this method will only use string construction for the first
	 * request in a batch.
	 */
	public String toString() {
		try {
			if (this.packetType == PaxosPacketType.ACCEPT)
				if (this.stringifiedSelf != null)
					return this.stringifiedSelf;
				else
					return (this.stringifiedSelf = this.toJSONSmart()
							.toString());
			return this.toJSONSmart().toString();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
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

	public boolean isMetaValue() {
		return this.requestValue==null;
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
	protected RequestPacket getFirstOnly() {
		if (this.batchSize() == 0)
			return this;
		// else
		RequestPacket req = (
		// retain slot in first coz it is useful for testing
		this instanceof ProposalPacket ? new ProposalPacket(
				((ProposalPacket) this).slot, this)
		// create new before modifying this
				: new RequestPacket(this));
		req.batched = null;
		return req;
	}

	public RequestPacket getACK() {
		// RequestPacket req = this.getFirstOnly();
		RequestPacket reply = new RequestPacket(this.requestID,
				ResponseCodes.ACK.toString(), this.stop, this);
		reply.batched = null;
		reply.responseValue = this.responseValue;
		return reply;
	}

	public RequestPacket getNACK() {
		RequestPacket req = this.getFirstOnly();
		return new RequestPacket(req.requestID, ResponseCodes.NACK.toString(),
				req.stop, req);
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

	public String getRequestValue() {
		return this.shouldReturnRequestValue ? this.requestValue : this
				.toString();
	}

	public String[] getRequestValues() {
		String[] reqValues = null;
		if (this.shouldReturnRequestValue) {
			reqValues = new String[this.batchSize() + 1];
			reqValues[0] = this.requestValue;
			if (this.batched != null)
				for (int i = 0; i < this.batched.length; i++) {
					reqValues[i + 1] = this.batched[i].shouldReturnRequestValue ? this.batched[i].requestValue
							: this.batched[i].toString();
					assert (this.batched[i].batched == null);
				}
		} else {
			reqValues = new String[1];
			reqValues[0] = toString();
		}
		return reqValues;
	}

	public RequestPacket[] getRequestPackets() {
		RequestPacket[] reqs = new RequestPacket[this.batchSize() + 1];
		reqs[0] = this.getFirstOnly();
		for (int i = 0; i < this.batchSize(); i++)
			reqs[i + 1] = this.batched[i];
		return reqs;
	}

	public int batchSize() {
		return this.batched != null ? this.batched.length : 0;
	}

	/*
	 * This ugly method is used only for testing and is needed in order to
	 * separate requests that first entered the requests (or requests with
	 * entryReplica==-1) from the rest in a batched request.
	 */
	public RequestPacket[] getEntryReplicaRequestsAsBatch(int id) {
		RequestPacket[] reqArray = this.toArray(); // all unbatched

		List<RequestPacket> noEntryReplicaRequests = new LinkedList<RequestPacket>();
		List<RequestPacket> entryReplicaRequests = new LinkedList<RequestPacket>();

		for (int i = 0; i < reqArray.length; i++)
			if (reqArray[i].getEntryReplica() == IntegerMap.NULL_INT_NODE || 
			reqArray[i].getEntryReplica() == id)
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
	public IntegerPacketType getRequestType() {
		return this.getType();
	}

	@Override
	public String getServiceName() {
		return this.paxosID;
	}

	@Override
	protected String getSummaryString() {
		return requestID
				+ ":"
				+ "["
				+ (NO_OP.equals(this.requestValue) ? NO_OP : "...")
				+ "]"
				+ (stop ? ":STOP" : "")
				+ (isBatched() ? "+("
						+ batchSize()
						+ " batched "
						+ (this.batchSize() <= 4 ? getBatchedIDs() : this
								.batchSize()) + ")" : "");
	}
	
	private String getBatchedIDs() {
		String s = "[";
		if(this.batched!=null) 
			for(RequestPacket req : this.batched) {
			s += req.requestID +" ";
		}
		return s+"]";
	}

	// testing
	private static PValuePacket getRandomPValue(String paxosID, int version,
			int slot, Ballot ballot, boolean stop, InetSocketAddress isa) {
		PValuePacket pvalue = new PValuePacket(ballot, new ProposalPacket(slot,
				new RequestPacket("some_request", stop).setClientAddress(isa)));
		pvalue.putPaxosID(paxosID, version);
		return pvalue;
	}

	public static PValuePacket getRandomPValue(String paxosID, int version,
			int slot, Ballot ballot) {
		return getRandomPValue(paxosID, version, slot, ballot, false, null);
	}

	static PValuePacket samplePValue = null;

	public static PValuePacket getSamplePValue() {
		return samplePValue;
	}

	/**
	 * We need this estimate to use it in {@link RequestBatcher#dequeueImpl()}.
	 * The value needs to be an upper bound on the sum total of all of the gunk
	 * in PValuePacket other than the requestValue itself, i.e., the size of a
	 * no-op decision.
	 */
	public static final int SIZE_ESTIMATE = estimateSize();

	private static int estimateSize() {
		int length = 0;
		try {
			length = (samplePValue == null ? (samplePValue = (PValuePacket) (getRandomPValue(
					TC.TEST_GUID.toString(), 0, 3142, new Ballot(23, 2178),
					true, new InetSocketAddress("128.119.245.40", 2000))))
					: samplePValue).toJSONObject().toString().length();
			// 25% extra for other miscellaneous additions
			return (int) (length * 1.25);

		} catch (JSONException e) {
			e.printStackTrace();
		}
		throw new RuntimeException("Failed to get size");
	}

	public boolean hasRequestValue() {
		return this.requestValue != null;
	}

	/*
	 * Need an upper bound here for limiting batch size. Currently all the
	 * fields in RequestPacket other than requestValue add up to ~200B.
	 */
	public int lengthEstimate() {
		int len = this.requestValue.length() + SIZE_ESTIMATE;
		if(this.isBatched())
			for(RequestPacket req : this.batched)
				len += req.lengthEstimate();
		return len;
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

	@Override
	public ClientRequest getResponse() {
		return this.getACK();
	}

	public void setResponse(String response) {
		if (this.responseValue == null)
			this.responseValue = response;
		else assert(isRecovery(this));
	}

	public boolean shouldReturnRequestValue() {
		return this.shouldReturnRequestValue;
	}

	@Override
	public long getRequestID() {
		return this.requestID;
	}
}
