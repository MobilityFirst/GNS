
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;


public class StopPacket extends BasicPacketWithClientAddress implements ReconfigurableRequest,
        ReplicableRequest {

  private final static String NAME = "name";
  private final static String VERSION = "version";
  private final static String QID = "qid";


  private final String name;

  private final int version;

  private final long requestID;


  private boolean needsCoordination = true;


  public StopPacket(String name, int version) {
    this.type = Packet.PacketType.STOP;
    this.name = name;
    this.version = version;
    this.requestID = (long) (Math.random() * Long.MAX_VALUE);
  }


  public StopPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.STOP) {
      throw new JSONException("STOP: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.PacketType.STOP;
    this.name = json.getString(NAME);
    this.version = json.getInt(VERSION);
    this.requestID = json.getLong(QID);
  }


  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(NAME, name);
    json.put(VERSION, version);
    json.put(QID, this.requestID);
    return json;
  }


  @Override
  public int getEpochNumber() {
    return version;
  }


  @Override
  public boolean isStop() {
    return true;
  }


  @Override
  public String getServiceName() {
    return name;
  }


  @Override
  public boolean needsCoordination() {
    return needsCoordination;
  }


  @Override
  public void setNeedsCoordination(boolean needsCoordination) {
    this.needsCoordination = needsCoordination;
  }


  @Override
  public long getRequestID() {
    return this.requestID;
  }

}
