package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.util.Stringifiable;
import org.json.JSONException;
import org.json.JSONObject;
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
public class NewActiveProposalPacket<NodeIDType> extends BasicPacketWithCCPAddress implements InterfaceRequest {

  private final static String NAME = "name";
  private final static String PROPOSING_NODE = "propNode";
  private final static String NEW_ACTIVES = "actives";
  private final static String VERSION = "version";

  /**
   * name for which the new actives are being proposed
   */
  private final String name;

  /**
   * node which proposed this message.
   */
  private final NodeIDType proposingNode;

  /**
   * current set of actives of this node.
   */
  private final Set<NodeIDType> newActives;

  /**
   * Verion number of this new set of active name servers.
   */
  private final int version;

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

  public NewActiveProposalPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    this.type = Packet.getPacketType(json);
    this.name = json.getString(NAME);
    this.proposingNode = unstringer.valueOf(json.getString(PROPOSING_NODE));
    this.newActives = unstringer.getValuesFromJSONArray(json.getJSONArray(NEW_ACTIVES));
    this.version = json.getInt(VERSION);
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
    json.put(NEW_ACTIVES, newActives);
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

  // For InterfaceRequest
  @Override
  public String getServiceName() {
    return this.name;
  }

}
