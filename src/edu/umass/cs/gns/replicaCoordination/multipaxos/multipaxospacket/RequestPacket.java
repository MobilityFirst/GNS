package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import org.json.JSONException;
import org.json.JSONObject;


import java.util.Random;

public class RequestPacket extends PaxosPacket {
	private static final boolean DEBUG = true;
	public static final String NO_OP="NO_OP";
	private static final long MAX_AGREEMENT_TIME = 30000;
	private static final int MAX_FORWARD_COUNT = 3;
	private static final String CLIENT_INFO = "client_id";
	private static final String CREATE_TIME = "create_time";
	private static final String REPLY_TO_CLIENT = "reply_to_client";
	public static final String FORWARD_COUNT = "forwardCount";
	public static final String FORWARDER_ID = "forwarderID";
	public static final String DEBUG_INFO = "DEBUG_INFO";
	private static final Random random  = new Random();

	public final NodeId<String> clientID;
	public final int requestID;
	public final String requestValue;
	public final boolean stop; 

	// needed to stop ping-ponging under coordinator confusion
	private int forwardCount=0; 
	private NodeId<String> forwarderID = GNSNodeConfig.INVALID_NAME_SERVER_ID;

	/* These are for testing and debugging */
	private long createTime = System.currentTimeMillis(); // preserved across forwarding by nodes, so not final
	private boolean replyToClient=false;
	private String debugInfo = "";

	public RequestPacket(NodeId<String> clientID,  String value, boolean stop) {
    	super((PaxosPacket)null);
		this.clientID = clientID;
		this.requestID = random.nextInt();
		this.requestValue = value;
		this.packetType = PaxosPacketType.REQUEST;
		this.stop = stop;
	}
	// Used only for testing
	public RequestPacket(NodeId<String> clientID,  int reqID, String value, boolean stop) {
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
		this.forwardCount = req.forwardCount;
		this.forwarderID = req.forwarderID;
		this.debugInfo = req.debugInfo;
	}
	public RequestPacket makeNoop() {
		RequestPacket noop = new RequestPacket(clientID, requestID, NO_OP, stop);
		noop.createTime = this.createTime;
		return noop;
	}
	public boolean isNoop() {return this.requestValue.equals(NO_OP);}
	private void incrForwardCount() {this.forwardCount++;}
	public int getForwardCount() {return this.forwardCount;}
	public RequestPacket setForwarderID(NodeId<String> id) {this.forwarderID=id;this.incrForwardCount(); return this;}
	public NodeId<String> getForwarderID() {return this.forwarderID;}
	private static String makeDebugInfo(String str, long cTime) {
		return " " + str + ":" + (System.currentTimeMillis() - cTime);
	}
	public void addDebugInfo(String str) {this.debugInfo += makeDebugInfo(str, this.getCreateTime());}
	public static boolean addDebugInfo(JSONObject msg, String str) throws JSONException {
		String debug="";
		boolean added = false;
		if(msg.has(DEBUG_INFO) && msg.has(CREATE_TIME)) { 
			debug = msg.getString(DEBUG_INFO) + makeDebugInfo(str, msg.getLong(CREATE_TIME));
			added = true;
			msg.put(DEBUG_INFO, debug);
		}
		return added;
	}
	public String getDebugInfo() {return " [" + this.debugInfo + "] ";}
	public static boolean isPingPonging(JSONObject msg) {
		try {
			if(msg.has(FORWARD_COUNT) && msg.getInt(FORWARD_COUNT) > MAX_FORWARD_COUNT) {
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
		String x = json.getString(CLIENT_INFO);
		String[] tokens = x.split("\\s");
		this.clientID = new NodeId<String>(tokens[0]);
		this.requestID = Integer.parseInt(tokens[1]);

		this.stop = tokens[2].equals("1") ? true : false;
		this.requestValue = x.substring(tokens[0].length() + tokens[1].length() + tokens[2].length() + 3);
		this.createTime = json.getLong(CREATE_TIME);
		this.replyToClient = json.getBoolean(REPLY_TO_CLIENT);
		this.forwardCount = (json.has(FORWARD_COUNT) ? json.getInt(FORWARD_COUNT) : 0);
		this.forwarderID = (json.has(FORWARDER_ID) ? new NodeId<String>(json.getString(FORWARDER_ID)) 
                        : GNSNodeConfig.INVALID_NAME_SERVER_ID);
		this.debugInfo = (json.has(DEBUG_INFO) ? json.getString(DEBUG_INFO) : "");
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		if (stop) {
			json.put(CLIENT_INFO, clientID.get() + " " + requestID + " " + 1 + " " + requestValue);
		} else {
			json.put(CLIENT_INFO, clientID.get() + " " + requestID + " " + 0 + " " + requestValue);
		}
		json.put(CREATE_TIME, this.createTime);
		json.put(REPLY_TO_CLIENT, replyToClient);
		json.put(FORWARD_COUNT, this.forwardCount);
		json.put(FORWARDER_ID, this.forwarderID);
		if(DEBUG) json.put(DEBUG_INFO, this.debugInfo);
		return json;
	}

	public boolean isStopRequest() {return stop;}
	public boolean hasTakenTooLong() {return System.currentTimeMillis() - this.createTime > MAX_AGREEMENT_TIME;}
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
	
	public static void main(String[] args) {
		RequestPacket req1 = new RequestPacket(new NodeId<String>(23), "asd", true);
		RequestPacket req2 = new RequestPacket(new NodeId<String>(23), "asd", true);
		assert(!req1.equals(req2));
	}
}
