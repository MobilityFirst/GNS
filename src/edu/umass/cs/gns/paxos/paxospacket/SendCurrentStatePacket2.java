package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.paxos.Ballot;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.Util;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

/**
 * A paxos replica sends complete state to another paxos replica using this message.
 * User: abhigyan
 * Date: 12/4/13
 * Time: 10:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class SendCurrentStatePacket2 extends PaxosPacket {

  private static final String SENDING_NODE_ID = "sendingNodeID";

  private static final String NODE_IDS = "nodeIDs";

  private static final String CURRENT_BALLOT = "acceptorBallot";

  private static final String SLOT_NUMBER = "slotNum";

  private static final String DB_STATE = "dbState";

  /**
   * ID of the node sending its state.
   */
  public NodeId<String> sendingNodeID;


  /**
   * IDs of nodes that belong to this paxos group.
   */
  public Set<NodeId<String>> nodeIDs;

  /**
   * currentBallot accepted by the sending node.
   */
  public Ballot currentBallot;

  /**
   * current slot number at this node.
   */
  public int slotNumber;

  /**
   * Record from the database corresponding to this paxos instance.
   */
  public String dbState;

  public SendCurrentStatePacket2(NodeId<String> sendingNodeID, Ballot currentBallot, int slotNumber, String dbState,
                                 Set<NodeId<String>> nodeIDs, PaxosPacketType packetType) {
    this.sendingNodeID = sendingNodeID;
    this.currentBallot = currentBallot;
    this.slotNumber = slotNumber;
    this.dbState = dbState;
    this.nodeIDs = nodeIDs;
    this.packetType =  packetType.getInt();
  }

  public SendCurrentStatePacket2(JSONObject json) throws JSONException {
    this.packetType = json.getInt(PaxosPacket.PACKET_TYPE_FIELD_NAME);
    this.sendingNodeID = new NodeId<String>(json.getString(SENDING_NODE_ID));
    if (json.has(NODE_IDS)) {
      this.nodeIDs = Util.stringToSetOfNodeId(NODE_IDS);
    } else this.nodeIDs = null;
    this.currentBallot = new Ballot(json.getString(CURRENT_BALLOT));
    this.slotNumber = json.getInt(SLOT_NUMBER);
    this.dbState = json.getString(DB_STATE);
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, this.packetType);
    json.put(SENDING_NODE_ID, sendingNodeID.get());
    if (nodeIDs != null) {
      json.put(NODE_IDS, Util.setOfNodeIdToString(nodeIDs));
    }
    json.put(CURRENT_BALLOT, currentBallot.toString());
    json.put(SLOT_NUMBER, slotNumber);
    json.put(DB_STATE, dbState);
    return json;
  }

}
