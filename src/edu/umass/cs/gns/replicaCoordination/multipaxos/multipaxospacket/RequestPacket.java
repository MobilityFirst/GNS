package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;

import java.util.Random;

public class RequestPacket extends PaxosPacket {
	public static final String NO_OP="NO_OP";
	private static final long MAX_AGREEMENT_TIME = 30000;
	private static final String CLIENT_INFO = "client_id";
	private static final String CREATE_TIME = "create_time";
	private static final String REPLY_TO_CLIENT = "reply_to_client";

	public final int clientID;
	public final int requestID;
	public final String requestValue;
	public final boolean stop; 

	private long createTime = System.currentTimeMillis(); // preserved across forwarding by nodes, so not final
	private boolean replyToClient=false;

	public RequestPacket(int clientID,  String value, boolean stop) {
    	super((PaxosPacket)null);
		Random r  = new Random();
		this.clientID = clientID;
		this.requestID = r.nextInt();
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
	}
	public RequestPacket makeNoop() {
		return new RequestPacket(clientID, requestID, NO_OP, stop);
	}
	public boolean isNoop() {return this.requestValue.equals(NO_OP);}

	public RequestPacket(JSONObject json) throws JSONException{
		super(json);
		this.packetType = PaxosPacketType.REQUEST;
		String x = json.getString(CLIENT_INFO);
		String[] tokens = x.split("\\s");
		this.clientID = Integer.parseInt(tokens[0]);
		this.requestID = Integer.parseInt(tokens[1]);

		this.stop = tokens[2].equals("1") ? true : false;
		this.requestValue = x.substring(tokens[0].length() + tokens[1].length() + tokens[2].length() + 3);
		this.createTime = json.getLong(CREATE_TIME);
		this.replyToClient = json.getBoolean(REPLY_TO_CLIENT);
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		if (stop) {
			json.put(CLIENT_INFO, clientID + " " + requestID + " " + 1 + " " + requestValue);
		} else {
			json.put(CLIENT_INFO, clientID + " " + requestID + " " + 0 + " " + requestValue);
		}
		json.put(CREATE_TIME, this.createTime);
		json.put(REPLY_TO_CLIENT, replyToClient);
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
	
	public static void main(String[] args) {
		RequestPacket req1 = new RequestPacket(23, "asd", true);
		RequestPacket req2 = new RequestPacket(23, "asd", true);
		assert(!req1.equals(req2));
	}
}
