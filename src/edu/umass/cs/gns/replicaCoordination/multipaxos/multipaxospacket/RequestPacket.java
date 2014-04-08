package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;

import java.util.Random;

public class RequestPacket extends PaxosPacket {
	private static final String CLIENT_INFO = "client_id";

	public final int clientID;
	public final int requestID;
	public final String requestValue;
	private final boolean stop; // could also be public, but private coz we might redefine isStopRequest later

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

	public RequestPacket(JSONObject json) throws JSONException{
		super(json);
		this.packetType = PaxosPacketType.REQUEST;
		String x = json.getString(CLIENT_INFO);
		String[] tokens = x.split("\\s");
		this.clientID = Integer.parseInt(tokens[0]);
		this.requestID = Integer.parseInt(tokens[1]);

		this.stop = tokens[2].equals("1") ? true : false;
		this.requestValue = x.substring(tokens[0].length() + tokens[1].length() + tokens[2].length() + 3);
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		if (stop) {
			json.put(CLIENT_INFO, clientID + " " + requestID + " " + 1 + " " + requestValue);
		} else {
			json.put(CLIENT_INFO, clientID + " " + requestID + " " + 0 + " " + requestValue);
		}
		return json;
	}

	public boolean isStopRequest() {
		return stop;
	}
	
	/* For testing */
	public static int getRequestID(String req) {
		String[] pieces = req.split("\\s");
		return (pieces!=null && pieces.length>=6 ? Integer.parseInt(pieces[5]) : -1);
	}
	
	public static void main(String[] args) {
		RequestPacket req1 = new RequestPacket(23, "asd", true);
		RequestPacket req2 = new RequestPacket(23, "asd", true);
		assert(!req1.equals(req2));
	}
}
