package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.JSONNIOTransport;

import java.util.Random;

public class RequestPacket extends PaxosPacket {
	private static final boolean DEBUG = true;

	/*
	 * These are rather specific to RequestPacket or for debugging, so they are
	 * here as opposed to PaxosPacket.
	 */
	public static enum Keys {
		NO_OP, IS_STOP, CREATE_TIME, RECEIPT_TIME, REPLY_TO_CLIENT, FORWARD_COUNT, 
		FORWARDER_ID, DEBUG_INFO, REQUEST_ID, REQUEST_VALUE, CLIENT_ID, CLIENT_ADDR, 
		CLIENT_PORT, RETURN_VALUE
	}

	private static final long MAX_AGREEMENT_TIME = 30000;
	private static final int MAX_FORWARD_COUNT = 3;
	private static final Random random = new Random();

	public final int clientID;
	public final int requestID;
	public final String requestValue;
	public final boolean stop;
	
	private int entryReplica = -1;
	private String clientAddress = null;
	private int clientPort = -1;
	private boolean returnRequestValue = false;

	// needed to stop ping-ponging under coordinator confusion
	private int forwardCount = 0;
	private int forwarderID = -1;

	/* these fields are for testing and debugging */
	// preserved across forwarding by nodes, so not final
	private long createTime = System.currentTimeMillis(); 
	private long receiptTime = System.currentTimeMillis();
	private String debugInfo = "";

	public RequestPacket(int clientID, String value, boolean stop) {
		this(clientID, random.nextInt(), value, stop);
	}

	public RequestPacket(int clientID, int reqID, String value, boolean stop) {
		super((PaxosPacket) null);
		this.clientID = clientID;
		this.requestID = reqID;
		this.requestValue = value;
		this.packetType = PaxosPacketType.REQUEST;
		this.stop = stop;
	}

	protected RequestPacket(RequestPacket req) {
		super(req);
		this.clientID = req.clientID;
		this.requestID = req.requestID;
		this.requestValue = req.requestValue;
		this.stop = req.stop;
		this.entryReplica = req.entryReplica;
		this.clientAddress = req.clientAddress;
		this.clientPort = req.clientPort;
		this.returnRequestValue = req.returnRequestValue;
		this.packetType = PaxosPacketType.REQUEST;
		// debug/testing fields below
		this.createTime = req.createTime;
		this.receiptTime = req.receiptTime;
		this.forwardCount = req.forwardCount;
		this.forwarderID = req.forwarderID;
		this.debugInfo = req.debugInfo;
	}

	public RequestPacket makeNoop() {
		RequestPacket noop = new RequestPacket(clientID, requestID,
				Keys.NO_OP.toString(), stop);
		noop.entryReplica = this.entryReplica;
		noop.clientAddress = this.clientAddress;
		noop.clientPort = this.clientPort;
		noop.createTime = this.createTime;
		noop.returnRequestValue = this.returnRequestValue;
		return noop;
	}
	
	public RequestPacket setReturnRequestValue() {
		this.returnRequestValue = true;
		return this;
	}

	public boolean isNoop() {
		return this.requestValue.equals(Keys.NO_OP.toString());
	}

	private void incrForwardCount() {
		this.forwardCount++;
	}

	public int getForwardCount() {
		return this.forwardCount;
	}

	public int setEntryReplica(int id) {
		if(this.entryReplica == -1) // one-time
			this.entryReplica = id;
		return this.entryReplica;
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
		this.debugInfo += makeDebugInfo(str, this.getCreateTime());
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
		this.stop = json.getBoolean(Keys.IS_STOP.toString());
		this.requestID = json.getInt(Keys.REQUEST_ID.toString());
		this.clientID = (json.has(Keys.CLIENT_ID.toString()) ? json
				.getInt(Keys.CLIENT_ID.toString()) : -1);
		this.requestValue = json.getString(Keys.REQUEST_VALUE.toString());
		this.createTime = json.getLong(Keys.CREATE_TIME.toString());
		this.receiptTime = json.getLong(Keys.RECEIPT_TIME.toString());
		//this.replyToClient = json.getBoolean(Keys.REPLY_TO_CLIENT.toString());
		this.forwardCount = (json.has(Keys.FORWARD_COUNT.toString()) ? json
				.getInt(Keys.FORWARD_COUNT.toString()) : 0);
		this.forwarderID = (json
				.has(RequestPacket.Keys.FORWARDER_ID.toString()) ? json
				.getInt(RequestPacket.Keys.FORWARDER_ID.toString()) : -1);
		this.debugInfo = (json.has(Keys.DEBUG_INFO.toString()) ? json
				.getString(Keys.DEBUG_INFO.toString()) : "");

		this.clientAddress = (json.has(Keys.CLIENT_ADDR.toString()) ? 
				json.getString(Keys.CLIENT_ADDR.toString()) : JSONNIOTransport
				.getSenderInetAddressAsString(json));
		this.clientPort = (json.has(Keys.CLIENT_PORT.toString()) ? 
				json.getInt(Keys.CLIENT_PORT.toString()) : JSONNIOTransport
				.getSenderPort(json));
		this.entryReplica = json.getInt(PaxosPacket.NodeIDKeys.ENTRY_REPLICA.toString());
		this.returnRequestValue = json.getBoolean(Keys.RETURN_VALUE.toString());
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.CLIENT_ID.toString(), clientID);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		json.put(Keys.REQUEST_VALUE.toString(), this.requestValue);
		json.put(Keys.CREATE_TIME.toString(), this.createTime);
		json.put(Keys.RECEIPT_TIME.toString(), this.receiptTime);
		//json.put(Keys.REPLY_TO_CLIENT.toString(), replyToClient);
		json.put(Keys.FORWARD_COUNT.toString(), this.forwardCount);
		json.put(RequestPacket.Keys.FORWARDER_ID.toString(), this.forwarderID);
		json.put(Keys.IS_STOP.toString(), this.stop);
		if (DEBUG)
			json.put(Keys.DEBUG_INFO.toString(), this.debugInfo);
		json.put(PaxosPacket.NodeIDKeys.ENTRY_REPLICA.toString(), this.entryReplica);
		if (this.clientAddress != null)
			json.put(Keys.CLIENT_ADDR.toString(), this.clientAddress);
		if (this.clientPort >= 0)
			json.put(Keys.CLIENT_PORT.toString(), this.clientPort);
		json.put(Keys.RETURN_VALUE.toString(), this.returnRequestValue);
		return json;
	}
	
	public String getClientAddress() {
		return this.clientAddress;
	}
	public int getClientPort() {
		return this.clientPort;
	}

	public boolean isStopRequest() {
		return stop;
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
	
	public String toString() {
		return this.returnRequestValue ? this.requestValue : super.toString();
	}

	public static void main(String[] args) {
		RequestPacket req1 = new RequestPacket(23, "asd", true);
		RequestPacket req2 = new RequestPacket(23, "asd", true);
		assert (!req1.equals(req2));
	}
}
