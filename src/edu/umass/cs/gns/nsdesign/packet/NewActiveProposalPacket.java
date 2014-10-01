package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

/**
 * This packet is exchanged among replica controllers to propose a new set of actives among primary name servers.
 *
 * This packet is created by a replica controller that wants to propose a new set of actives. The packet is
 * then forwarded to the appropriate set of replica controllers. After being committed by active replicas,
 * each replica controller updates its database with the new set of proposed actives.
 *
 * @author abhigyan
 *
 */
public class NewActiveProposalPacket<NodeIDType> extends BasicPacketWithLnsAddress {

  private final static String NAME = "name";

  private final static String PROPOSING_NODE = "propNode";

  private final static String NEW_ACTIVES = "actives";

  private final static String VERSION = "version";

  //private final static String LNS_ID = "lns_id"; // this field is used during testing only

  /**
   * name for which the new actives are being proposed
   */
  String name;

  /**
   * node which proposed this message.
   */
  NodeIDType proposingNode;

  /**
   * current set of actives of this node.
   */
  Set<NodeIDType> newActives;

  /**
   * Verion number of this new set of active name servers.
   */
  int version;
//
//  private int lnsId = -1;

  /**
   * Constructor method
   *
   * @param name name for which the new actives are being proposed
   * @param proposingNode node which proposed this message.
   * @param newActives current set of actives of this node.
   * @param version Version number for this new set of active name servers.
   */
  public NewActiveProposalPacket(String name, NodeIDType proposingNode, Set<NodeIDType> newActives, int version) {
    super(null);
    this.type = PacketType.NEW_ACTIVE_PROPOSE;
    this.name = name;
    this.proposingNode = proposingNode;
    this.newActives = newActives;
    this.version = version;
  }

  public NewActiveProposalPacket(JSONObject json) throws JSONException {
    super(json.optString(LNS_ADDRESS, null), json.optInt(LNS_PORT, INVALID_PORT));
    this.type = Packet.getPacketType(json);
    this.name = json.getString(NAME);

    this.proposingNode = (NodeIDType)json.get(PROPOSING_NODE);

    String actives = json.getString(NEW_ACTIVES);

    this.newActives = new HashSet<NodeIDType>();

    String[] activeSplit = actives.split(":");

    for (String x : activeSplit) {
      newActives.add((NodeIDType) x);
    }

    this.version = json.getInt(VERSION);
    //this.lnsId = json.getInt(LNS_ID);
  }

  /**
   * JSON object that is implemented.
   *
   * @return
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(NAME, name);
    json.put(PROPOSING_NODE, proposingNode);

    // convert array to string
    StringBuilder sb = new StringBuilder();
    for (NodeIDType x : newActives) {
      if (sb.length() == 0) {
        sb.append(x.toString());
      } else {
        sb.append(":");
        sb.append(x.toString());
      }
    }
    String actives = sb.toString();
    json.put(NEW_ACTIVES, actives);
    json.put(VERSION, version);
    //json.put(LNS_ID, lnsId);

    return json;
  }

  public String getName() {
    return name;
  }

  public NodeIDType getProposingNode() {
    return proposingNode;
  }

  public Set<NodeIDType> getProposedActiveNameServers() {
    return newActives;
  }

  public int getVersion() {
    return version;
  }

//  public int getLnsId() {
//    return lnsId;
//  }
//
//  public void setLnsId(int lnsId) {
//    this.lnsId = lnsId;
//  }
}
