package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.gns.nio.Stringifiable;
import edu.umass.cs.gns.util.JSONUtils;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/5/13
 * Time: 7:45 PM
 * To change this template use File | Settings | File Templates.
 */
@Deprecated
public class SynchronizeReplyPacket<NodeIDType> extends PaxosPacket {

  /**
   * node ID of sending node
   */
  private NodeIDType nodeID;

  /**
   * maximum slot for which nodeID has received decision
   */
  private int maxDecisionSlot;

  /**
   * slot numbers less than max slot which are missing
   */
  private ArrayList<Integer> missingSlotNumbers;
  String NODE = "x1";
  String MAX_SLOT = "x2";
  String MISSING = "x3";
  String FLAG = "x4";

  public boolean flag;

  public SynchronizeReplyPacket(NodeIDType nodeID, int maxDecisionSlot, ArrayList<Integer> missingSlotNumbers, boolean flag1) {
    this.packetType = PaxosPacketType.SYNC_REPLY.getInt();
    this.nodeID = nodeID;
    this.maxDecisionSlot = maxDecisionSlot;
    this.missingSlotNumbers = missingSlotNumbers;
    this.flag = flag1;
  }

  public SynchronizeReplyPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    this.nodeID = unstringer.valueOf(json.getString(NODE));
    this.maxDecisionSlot = json.getInt(MAX_SLOT);
    if (json.has(MISSING)) {
      missingSlotNumbers = JSONUtils.JSONArrayToArrayListInteger(json.getJSONArray(MISSING));
    } else {
      missingSlotNumbers = null;
    }
    this.packetType = PaxosPacketType.SYNC_REPLY.getInt();
    this.flag = json.getBoolean(FLAG);
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, this.packetType);
    json.put(NODE, nodeID);
    json.put(MAX_SLOT, maxDecisionSlot);
    json.put(FLAG, flag);
    if (missingSlotNumbers != null && missingSlotNumbers.size() > 0) {
      json.put(MISSING, new JSONArray(missingSlotNumbers));
    }
    return json;

  }

  /**
   * @return the nodeID
   */
  public NodeIDType getNodeID() {
    return nodeID;
  }

  /**
   * @param nodeID the nodeID to set
   */
  public void setNodeID(NodeIDType nodeID) {
    this.nodeID = nodeID;
  }

  /**
   * @return the maxDecisionSlot
   */
  public int getMaxDecisionSlot() {
    return maxDecisionSlot;
  }

  /**
   * @param maxDecisionSlot the maxDecisionSlot to set
   */
  public void setMaxDecisionSlot(int maxDecisionSlot) {
    this.maxDecisionSlot = maxDecisionSlot;
  }

  /**
   * @return the missingSlotNumbers
   */
  public ArrayList<Integer> getMissingSlotNumbers() {
    return missingSlotNumbers;
  }

  /**
   * @param missingSlotNumbers the missingSlotNumbers to set
   */
  public void setMissingSlotNumbers(ArrayList<Integer> missingSlotNumbers) {
    this.missingSlotNumbers = missingSlotNumbers;
  }
}
