package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.gns.paxos.Ballot;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PreparePacket extends PaxosPacket {

  public int coordinatorID;

  public Ballot ballot;

  public int receiverID;

  public int slotNumber;
  public Map<Integer, PValuePacket> accepted;

  public PreparePacket(int coordinatorID, int receiverID, Ballot b, PaxosPacketType packetType) {
    this.coordinatorID = coordinatorID;
    this.receiverID = receiverID;
    this.ballot = b;
    this.packetType = packetType.getInt();
    this.slotNumber = -1;

  }

  public PreparePacket getPrepareReplyPacket(Ballot b, int receiverID, Map<Integer, PValuePacket> accepted, int slotNumber) {

    if (b.equals(this.ballot)) {
      PreparePacket prep = new PreparePacket(this.coordinatorID, receiverID,
              this.ballot, PaxosPacketType.PREPARE_REPLY);
      prep.accepted = accepted;
      prep.slotNumber = slotNumber;
      return prep;
    }

    PreparePacket prep = new PreparePacket(this.coordinatorID, receiverID,
            b, PaxosPacketType.PREPARE_REPLY);
    prep.accepted = accepted;
    prep.slotNumber = slotNumber;
    return prep;
  }

  public PreparePacket(JSONObject json) throws JSONException {
    this.packetType = json.getInt(PaxosPacket.PACKET_TYPE_FIELD_NAME);
    this.coordinatorID = json.getInt("coordinatorID");
    this.receiverID = json.getInt("receiverID");
    this.ballot = new Ballot(json.getString("ballot"));
    this.slotNumber = json.getInt("slotNumber");
    if (this.packetType == PaxosPacketType.PREPARE_REPLY.getInt()) {
      this.accepted = parseJsonForAccepted(json);
    }
  }

  private ConcurrentHashMap<Integer, PValuePacket> parseJsonForAccepted(JSONObject json)
          throws JSONException {
    ConcurrentHashMap<Integer, PValuePacket> accepted = new ConcurrentHashMap<Integer, PValuePacket>();
    if (json.has("accepted")) {
      JSONArray jsonArray = json.getJSONArray("accepted");
      for (int i = 0; i < jsonArray.length(); i++) {
        JSONObject element = jsonArray.getJSONObject(i);
        PValuePacket pval = new PValuePacket(element);
        accepted.put(pval.proposal.slot, pval);
      }
    }
    return accepted;
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, this.packetType);
    json.put("coordinatorID", coordinatorID);
    json.put("receiverID", receiverID);
    json.put("ballot", ballot.toString());
    json.put("slotNumber", slotNumber);
    if (this.packetType == PaxosPacketType.PREPARE_REPLY.getInt()) {
      addAcceptedToJSON(json);
    }
    return json;
  }

  private void addAcceptedToJSON(JSONObject json) throws JSONException {
    if (accepted != null) {
      JSONArray jsonArray = new JSONArray();
      for (PValuePacket pValues : accepted.values()) {
        jsonArray.put(pValues.toJSONObject());
      }
      json.put("accepted", jsonArray);
    }
  }

}
