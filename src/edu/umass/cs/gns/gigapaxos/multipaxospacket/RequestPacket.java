package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;


import java.util.Random;

public class RequestPacket extends PaxosPacket {
	private static final boolean DEBUG = true;
	
	/* These are rather specific to RequestPacket or for
	 * debugging, so they are here as opposed to PaxosPacket.
	 */
	public static enum Keys {NO_OP, IS_STOP, CLIENT_INFO, CREATE_TIME,
		RECEIPT_TIME, REPLY_TO_CLIENT, FORWARD_COUNT, FORWARDER_ID,
		DEBUG_INFO
	}
	private static final long MAX_AGREEMENT_TIME = 30000;
	private static final int MAX_FORWARD_COUNT = 3;
	private static final Random random  = new Random();

	public final int clientID;
	public final int requestID;
	public final String requestValue;
	public final boolean stop; 

	// needed to stop ping-ponging under coordinator confusion
	private int forwardCount=0; 
	private int forwarderID=-1;

	/* These are for testing and debugging */
	private long createTime = System.currentTimeMillis(); // preserved across forwarding by nodes, so not final
	private long receiptTime = System.currentTimeMillis();
	private boolean replyToClient=false;
	private String debugInfo = "";

	public RequestPacket(int clientID,  String value, boolean stop) {
    	super((PaxosPacket)null);
		this.clientID = clientID;
		this.requestID = random.nextInt();
		this.requestValue = value;
		this.packetType = PaxosPacketType.REQUEST;
		this.stop = stop;
	}
	// Used only for testing
	public RequestPacket(int clientID,  int reqID, String value, boolean stop) {
    	super((PaxosPacket)null);
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
		this.packetType = PaxosPacketType.REQUEST;
		this.createTime = req.createTime;
		this.receiptTime = req.receiptTime;
		this.forwardCount = req.forwardCount;
		this.forwarderID = req.forwarderID;
		this.debugInfo = req.debugInfo;
	}
	public RequestPacket makeNoop() {
		RequestPacket noop = new RequestPacket(clientID, requestID, Keys.NO_OP.toString(), stop);
		noop.createTime = this.createTime;
		return noop;
	}
	public boolean isNoop() {return this.requestValue.equals(Keys.NO_OP.toString());}
	private void incrForwardCount() {this.forwardCount++;}
	public int getForwardCount() {return this.forwardCount;}
	public RequestPacket setForwarderID(int id) {this.forwarderID=id;this.incrForwardCount(); return this;}
	public int getForwarderID() {return this.forwarderID;}
	private static String makeDebugInfo(String str, long cTime) {
		return " " + str + ":" + (System.currentTimeMillis() - cTime);
	}
	public void addDebugInfo(String str) {this.debugInfo += makeDebugInfo(str, this.getCreateTime());}
	public static boolean addDebugInfo(JSONObject msg, String str) throws JSONException {
		String debug="";
		boolean added = false;
		if(msg.has(Keys.DEBUG_INFO.toString()) && msg.has(Keys.CREATE_TIME.toString())) { 
			debug = msg.getString(Keys.DEBUG_INFO.toString()) + makeDebugInfo(str, msg.getLong(Keys.CREATE_TIME.toString()));
			added = true;
			msg.put(Keys.DEBUG_INFO.toString(), debug);
		}
		return added;
	}
	public String getDebugInfo() {return " [" + this.debugInfo + "] ";}
	public static boolean isPingPonging(JSONObject msg) {
		try {
			if(msg.has(Keys.FORWARD_COUNT.toString()) && msg.getInt(Keys.FORWARD_COUNT.toString()) > MAX_FORWARD_COUNT) {
				return true;
			}
		} catch(JSONException je) {je.printStackTrace();}
		return false;
	}
	public boolean isPingPonging() {
		return this.forwardCount > MAX_FORWARD_COUNT;
	}

	public RequestPacket(JSONObject json) throws JSONException{
		super(json);
		this.packetType = PaxosPacketType.REQUEST;
		String x = json.getString(Keys.CLIENT_INFO.toString());
		String[] tokens = x.split("\\s");
		this.clientID = Integer.parseInt(tokens[0]);
		this.requestID = Integer.parseInt(tokens[1]);

		this.stop = tokens[2].equals("1") ? true : false;
		this.requestValue = x.substring(tokens[0].length() + tokens[1].length() + tokens[2].length() + 3);
		this.createTime = json.getLong(Keys.CREATE_TIME.toString());
		this.receiptTime = json.getLong(Keys.RECEIPT_TIME.toString());
		this.replyToClient = json.getBoolean(Keys.REPLY_TO_CLIENT.toString());
		this.forwardCount = (json.has(Keys.FORWARD_COUNT.toString()) ? json.getInt(Keys.FORWARD_COUNT.toString()) : 0);
		this.forwarderID = (json.has(RequestPacket.Keys.FORWARDER_ID.toString()) ? 
				json.getInt(RequestPacket.Keys.FORWARDER_ID.toString()) : -1);
		this.debugInfo = (json.has(Keys.DEBUG_INFO.toString()) ? json.getString(Keys.DEBUG_INFO.toString()) : "");
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		if (stop) {
			json.put(Keys.CLIENT_INFO.toString(), clientID + " " + requestID + " " + 1 + " " + requestValue);
		} else {
			json.put(Keys.CLIENT_INFO.toString(), clientID + " " + requestID + " " + 0 + " " + requestValue);
		}
		json.put(Keys.CREATE_TIME.toString(), this.createTime);
		json.put(Keys.RECEIPT_TIME.toString(), this.receiptTime);
		json.put(Keys.REPLY_TO_CLIENT.toString(), replyToClient);
		json.put(Keys.FORWARD_COUNT.toString(), this.forwardCount);
		json.put(RequestPacket.Keys.FORWARDER_ID.toString(), this.forwarderID);
		json.put(Keys.IS_STOP.toString(), this.stop);
		if(DEBUG) json.put(Keys.DEBUG_INFO.toString(), this.debugInfo);
		return json;
	}

	public boolean isStopRequest() {return stop;}
	public boolean hasTakenTooLong() {return System.currentTimeMillis() - this.receiptTime > MAX_AGREEMENT_TIME;}
	public void setReplyToClient(boolean b) {this.replyToClient=b;}
	public boolean getReplyToClient() {return this.replyToClient;}
	
	/* For testing */
	public static int getRequestID(String req) {
		String[] pieces = req.split("\\s");
		return (pieces!=null && pieces.length>=6 ? Integer.parseInt(pieces[5]) : -1);
	}
	/* Used only for testing database logging to check that the logged
	 * packet is indeed logged across crashes. If this timestamp is
	 * different each time, the test would needlessly fail.
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
	
	public static void main(String[] args) {
		RequestPacket req1 = new RequestPacket(23, "asd", true);
		RequestPacket req2 = new RequestPacket(23, "asd", true);
		assert(!req1.equals(req2));
	}
}
