package edu.umass.cs.gigapaxos.paxospackets;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.RequestBatcher;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

import java.util.ArrayList;
import java.util.Random;

/**
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public class RequestPacket extends PaxosPacket implements InterfaceRequest {
	private static final boolean DEBUG = Config.getGlobalBoolean(PC.DEBUG);
	public static final String NO_OP = InterfaceRequest.NO_OP;

	/**
	 * These JSON keys are rather specific to RequestPacket or for debugging, so
	 * they are here as opposed to PaxosPacket. Application developers don't
	 * have to worry about these.
	 */
	protected static enum Keys {
		IS_STOP, CREATE_TIME, RECEIPT_TIME, REPLY_TO_CLIENT, FORWARD_COUNT, FORWARDER_ID,
		//
		DEBUG_INFO,
		//
		REQUEST_ID, REQUEST_VALUE, CLIENT_ID, CLIENT_ADDR, CLIENT_PORT, RETURN_VALUE, BATCHED
	}

	private static final long MAX_AGREEMENT_TIME = 30000;
	private static final int MAX_FORWARD_COUNT = 3;
	private static final Random random = new Random();

	/**
	 * Integer ID of client if one exists. Only integer client IDs are currently
	 * supported in RequestPacket as this field is primarily used in testing. An
	 * application client can include other arbitrary information in
	 * {@code requestValue} if needed.
	 */
	public final int clientID;
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

	// the replica that first received this request
	private int entryReplica = -1;
	// the client IP address in string form
	private String clientAddress = null;
	// the client port
	private int clientPort = -1;
	// whether to return requestValue or this.toString() back to client
	private boolean shouldReturnRequestValue = false;

	// needed to stop ping-ponging under coordinator confusion
	private int forwardCount = 0;
	private int forwarderID = -1;

	// batching
	private RequestPacket[] batched = null;

	/*
	 * These fields are for testing and debugging. They are preserved across
	 * forwarding by nodes, so they are not final
	 */
	private long createTime = System.currentTimeMillis();
	private long receiptTime = System.currentTimeMillis();
	private String debugInfo = null;

	// let a random request ID be picked
	public RequestPacket(int clientID, String value, boolean stop) {
		this(clientID, random.nextInt(), value, stop);
	}

	// the common-case constructor
	public RequestPacket(int clientID, int reqID, String value, boolean stop) {
		this(clientID, reqID, value, stop, null);
	}

	// called by inheritors
	public RequestPacket(RequestPacket req) {
		this(req.clientID, req.requestID, req.requestValue, req.stop, req);
	}

	// used by makeNoop to convert req to a noop
	public RequestPacket(int clientID, int reqID, String value, boolean stop,
			RequestPacket req) {
		super(req); // will take paxosID and version from req

		// final fields
		this.packetType = PaxosPacketType.REQUEST;
		this.clientID = clientID;
		this.requestID = reqID;
		this.requestValue = value;
		this.stop = stop;

		if (req == null)
			return;

		// non-final fields
		this.entryReplica = req.entryReplica;
		this.clientAddress = req.clientAddress;
		this.clientPort = req.clientPort;
		this.shouldReturnRequestValue = req.shouldReturnRequestValue;

		// debug/testing fields
		this.createTime = req.createTime;
		this.receiptTime = req.receiptTime;
		this.forwardCount = req.forwardCount;
		this.forwarderID = req.forwarderID;
		this.debugInfo = req.debugInfo;
		this.batched = req.batched;
	}

	public RequestPacket makeNoop() {
		RequestPacket noop = new RequestPacket(clientID, requestID, NO_OP,
				stop, this);
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
		this.debugInfo = (this.debugInfo == null ? "" : this.debugInfo)
				+ makeDebugInfo(str, this.getCreateTime());
	}

	public static boolean addDebugInfo(JSONObject msg, String str)
			throws JSONException {
		String debug = "";
		boolean added = false;
		if (msg.has(Keys.DEBUG_INFO.toString())
				&& msg.has(Keys.CREATE_TIME.toString())) {
			debug = msg.getString(Keys.DEBUG_INFO.toString())
					+ makeDebugInfo(str,
							msg.getLong(Keys.CREATE_TIME.toString()));
			added = true;
			msg.put(Keys.DEBUG_INFO.toString(), debug);
		}
		return added;
	}

	public String getDebugInfo() {
		return " [" + this.debugInfo + "] ";
	}

	public static boolean isPingPonging(JSONObject msg) {
		try {
			if (msg.has(Keys.FORWARD_COUNT.toString())
					&& msg.getInt(Keys.FORWARD_COUNT.toString()) > MAX_FORWARD_COUNT) {
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
		this.stop = json.optBoolean(Keys.IS_STOP.toString());
		this.requestID = json.getInt(Keys.REQUEST_ID.toString());
		this.clientID = (json.has(Keys.CLIENT_ID.toString()) ? json
				.getInt(Keys.CLIENT_ID.toString()) : -1);
		this.requestValue = json.getString(Keys.REQUEST_VALUE.toString());
		this.createTime = json.getLong(Keys.CREATE_TIME.toString());
		this.receiptTime = json.getLong(Keys.RECEIPT_TIME.toString());
		this.forwardCount = (json.has(Keys.FORWARD_COUNT.toString()) ? json
				.getInt(Keys.FORWARD_COUNT.toString()) : 0);
		this.forwarderID = (json
				.has(RequestPacket.Keys.FORWARDER_ID.toString()) ? json
				.getInt(RequestPacket.Keys.FORWARDER_ID.toString()) : -1);
		this.debugInfo = (json.has(Keys.DEBUG_INFO.toString()) ? json
				.getString(Keys.DEBUG_INFO.toString()) : "");

		this.clientAddress = (json.has(Keys.CLIENT_ADDR.toString()) ? json
				.getString(Keys.CLIENT_ADDR.toString()) : JSONNIOTransport
				.getSenderInetAddressAsString(json));
		this.clientPort = (json.has(Keys.CLIENT_PORT.toString()) ? json
				.getInt(Keys.CLIENT_PORT.toString()) : JSONNIOTransport
				.getSenderPort(json));
		this.entryReplica = json.getInt(PaxosPacket.NodeIDKeys.ENTRY_REPLICA
				.toString());
		this.shouldReturnRequestValue = json.getBoolean(Keys.RETURN_VALUE
				.toString());
		// unwrap latched along batch
		JSONArray batchedJSON = json.has(Keys.BATCHED.toString()) ? json
				.getJSONArray(Keys.BATCHED.toString()) : null;
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
		json.put(Keys.CLIENT_ID.toString(), clientID);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		json.put(Keys.REQUEST_VALUE.toString(), this.requestValue);
		json.put(Keys.CREATE_TIME.toString(), this.createTime);
		json.put(Keys.RECEIPT_TIME.toString(), this.receiptTime);
		json.put(Keys.FORWARD_COUNT.toString(), this.forwardCount);
		json.put(RequestPacket.Keys.FORWARDER_ID.toString(), this.forwarderID);
		json.put(Keys.IS_STOP.toString(), this.stop);
		if (DEBUG)
			json.putOpt(Keys.DEBUG_INFO.toString(), this.debugInfo);
		json.put(PaxosPacket.NodeIDKeys.ENTRY_REPLICA.toString(),
				this.entryReplica);
		if (this.clientAddress != null)
			json.put(Keys.CLIENT_ADDR.toString(), this.clientAddress);
		if (this.clientPort >= 0)
			json.put(Keys.CLIENT_PORT.toString(), this.clientPort);
		json.put(Keys.RETURN_VALUE.toString(), this.shouldReturnRequestValue);
		// convert latched along batch to json array
		if (this.batched != null && this.batched.length > 0) {
			JSONArray batchedJSON = new JSONArray();
			for (int i = 0; i < this.batched.length; i++) {
				batchedJSON.put(this.batched[i].toJSONObject());
			}
			json.put(Keys.BATCHED.toString(), batchedJSON);
		}
		return json;
	}

	public String getClientAddress() {
		return this.clientAddress;
	}

	public int getClientPort() {
		return this.clientPort;
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

	public boolean hasTakenTooLong() {
		return System.currentTimeMillis() - this.receiptTime > MAX_AGREEMENT_TIME;
	}

	/* For testing */
	public static int getRequestID(String req) {
		String[] pieces = req.split("\\s");
		return (pieces != null && pieces.length >= 6 ? Integer
				.parseInt(pieces[5]) : -1);
	}

	/*
	 * Used only for testing database logging to check that the logged packet is
	 * indeed logged across crashes. If this timestamp is different each time,
	 * the test would needlessly fail.
	 */
	public void setCreateTime(long t) {
		this.createTime = t;
	}

	public long getCreateTime() {
		return this.createTime;
	}

	public void setReceiptTime(long t) {
		this.receiptTime = t;
	}

	public void setReceiptTime() {
		this.setReceiptTime(System.currentTimeMillis());
	}

	public long getReceiptTime() {
		return this.receiptTime;
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
	static {
		int length = 0;
		try {
			length = new PValuePacket(new Ballot(23, 2178), new ProposalPacket(
					3142, new RequestPacket(23, 43437, "hello world", false)))
					.toJSONObject().toString().length();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		// 25% extra for other miscellaneous additions
		SIZE_ESTIMATE = (int) (length * 1.25);
	}

	/*
	 * Need an upper bound here for limiting batch size. Currently all the
	 * fields in RequestPacket other than requestValue add up to around 270.
	 */
	public int lengthEstimate() {
		return this.requestValue.length() + SIZE_ESTIMATE;
	}

	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		int numReqs = 25;
		RequestPacket[] reqs = new RequestPacket[numReqs];
		RequestPacket req = new RequestPacket(999, "asd" + 999, true);
		for (int i = 0; i < numReqs; i++) {
			reqs[i] = new RequestPacket(i, "asd" + i, true);
		}

		System.out.println("Decision size estimate = " + SIZE_ESTIMATE);

		req.latchToBatch(reqs);
		String reqStr = req.toString();
		try {
			RequestPacket reqovered = new RequestPacket(req.toJSONObject());
			String reqoveredStr = reqovered.toString();
			assert (reqStr.equals(reqoveredStr));
			System.out.println(reqovered.batched.length);
			System.out.println(reqovered.batched[3]);

		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

}
