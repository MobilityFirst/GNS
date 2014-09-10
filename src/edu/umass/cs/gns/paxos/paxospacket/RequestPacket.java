package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.Objects;
import java.util.Random;

public class RequestPacket extends PaxosPacket implements Serializable {

  public NodeId<String> clientID;

  public int requestID;

  public String value;

  private boolean stop = false;

  public RequestPacket(NodeId<String> clientID, String value, PaxosPacketType packetType, boolean stop) {
    Random r = new Random();
    this.clientID = clientID;
    this.requestID = r.nextInt();
    this.value = value;
    this.packetType = packetType.getInt();
    this.stop = stop;

  }

  public RequestPacket(JSONObject json) throws JSONException {
    this.packetType = PaxosPacketType.REQUEST.getInt();
    String x = json.getString("y1");
    String[] tokens = x.split("\\s");
    this.clientID = new NodeId<String>(tokens[0]);
    this.requestID = Integer.parseInt(tokens[1]);

    this.stop = tokens[2].equals("1");
    this.value = x.substring(tokens[0].length() + tokens[1].length() + tokens[2].length() + 3);
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, this.packetType);
    if (stop) {
      json.put("y1", clientID + " " + requestID + " " + 1 + " " + value);
    } else {
      json.put("y1", clientID + " " + requestID + " " + 0 + " " + value);
    }
    return json;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    RequestPacket other = (RequestPacket) obj;
    if (other.clientID == this.clientID && other.requestID == this.requestID
            && other.value.equals(this.value)) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 79 * hash + this.clientID.hashCode();
    hash = 79 * hash + this.requestID;
    hash = 79 * hash + Objects.hashCode(this.value);
    return hash;
  }

  public boolean isStopRequest() {
    return stop;
  }
}
