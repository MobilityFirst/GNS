package edu.umass.cs.gns.packet.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;

public abstract class Packet {
	
	// One type of packet can have more than one value of packetType.  
	public int packetType;
	
	
	
	public abstract JSONObject toJSONObject() throws JSONException;
	
	@Override
  public String toString() {
    try {
      return this.toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();
      return null;
    }
  }	
}
