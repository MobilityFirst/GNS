package edu.umass.cs.gns.protocoltask.json;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.packet.BasicPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;

/**
 * @author V. Arun
 */
/* This class is concretized to use integer node IDs, long keys, and JSON
 * messages. 
 */
public abstract class ProtocolPacket extends BasicPacket implements ProtocolEvent<Packet.PacketType, Long> {

  public static final String SENDER = "SENDER";
  public static final String INITIATOR = "INITIATOR";
  public static final String KEY = "KEY";

  private NodeId<String> initiator =  GNSNodeConfig.INVALID_NAME_SERVER_ID;
  private NodeId<String> sender = GNSNodeConfig.INVALID_NAME_SERVER_ID;
  private long key = -1;

  public ProtocolPacket(NodeId<String> sid, NodeId<String> iid) {
    this.sender = sid;
    this.initiator = iid;
  }

  public ProtocolPacket(JSONObject json) throws JSONException {
    this.setType(Packet.getPacketType(json));
    this.sender = (json.has(SENDER) ? new NodeId<String>(json.getInt(SENDER)) : GNSNodeConfig.INVALID_NAME_SERVER_ID);
    this.initiator = (json.has(INITIATOR) ? new NodeId<String>(json.getInt(INITIATOR)) : GNSNodeConfig.INVALID_NAME_SERVER_ID);
    this.key = (json.has(KEY) ? json.getLong(KEY) : -1);
  }

  public NodeId<String> getInitiator() {
    return this.initiator;
  }

  public void setSender(NodeId<String> id) {
    this.sender = id;
  }

  public NodeId<String> getSender() {
    return this.sender;
  }

  public NodeId<String> flip(NodeId<String> rcvr) { // flip sender and rcvr
    NodeId<String> prevSender = this.sender;
    this.sender = rcvr;
    return prevSender;
  }

  public abstract JSONObject toJSONObjectImpl() throws JSONException;

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = toJSONObjectImpl();
    json.put(Packet.PACKET_TYPE, this.getType().getInt());
    json.put(SENDER, this.sender.get());
    json.put(INITIATOR, this.initiator);
    json.put(KEY, this.key);
    return json;
  }

  @Override
  public Long getKey() {
    return key;
  }

  @Override
  public void setKey(Long key) {
    this.key = key;
  }

  @Override
  public Object getMessage() {
    return this;
  }

  public static void main(String[] args) {

  }
}
