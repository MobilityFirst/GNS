package edu.umass.cs.gns.packet.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Random;

public class RequestPacket extends Packet {

	public int clientID;

	public int requestID;

	public String value;

  private boolean stop = false;


//    String CLIENT_ID = "y1";
//    String PAXOS_REQUEST_ID = "y2";
//    String VALUE = "y3";

	public RequestPacket(int clientID,  String value, int packetType, boolean stop) {
		Random r  = new Random();
		this.clientID = clientID;
		this.requestID = r.nextInt();
		this.value = value;
		this.packetType = packetType;
    this.stop = stop;

	}




//	public RequestPacket getResponsePacket() {
//		return new RequestPacket(clientID,  value, PaxosPacketType.RESPONSE);
//	}



	public RequestPacket(JSONObject json) throws JSONException{
        this.packetType = PaxosPacketType.REQUEST;
        String x = json.getString("y1");
        String[] tokens = x.split("\\s");
        this.clientID = Integer.parseInt(tokens[0]);
        this.requestID = Integer.parseInt(tokens[1]);

    this.stop = tokens[2].equals("1") ? true : false;
    this.value = x.substring(tokens[0].length() + tokens[1].length() + tokens[2].length() + 3);
//		this.clientID = json.getInt(CLIENT_ID);
//		this.requestID = json.getInt(PAXOS_REQUEST_ID);
//		this.value = json.getString(VALUE);
	}

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacketType.ptype, this.packetType);
    if (stop) {
      json.put("y1", clientID + " " + requestID + " " + 1 + " " + value);
    } else {
        json.put("y1", clientID + " " + requestID + " " + 0 + " " + value);
    }
		return json;
	}


	@Override
	public boolean equals(Object obj) {
		RequestPacket req2 = (RequestPacket) obj;
		if (req2.clientID == this.clientID && req2.requestID == this.requestID 
				&& req2.value.equals(this.value)) 
			return true;

		return false;
	}

  public boolean isStopRequest() {
    return stop;
  }
}
