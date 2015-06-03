package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.nio.Stringifiable;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;

@Deprecated
public class ProposalPacket<NodeIDType> extends PaxosPacket implements Serializable {

  public int slot;

  public RequestPacket<NodeIDType> req;

  public static String SLOT = "s1";
  public static String GC_SLOT = "s2";

  public int gcSlot = 0;

  public ProposalPacket(int slot, RequestPacket<NodeIDType> req, PaxosPacketType packetType, int gcSlot) {
    this.slot = slot;
    this.req = req;
    this.packetType = packetType.getInt();
    this.gcSlot = gcSlot;
  }

  public ProposalPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    try {
      this.req = new RequestPacket<NodeIDType>(json, unstringer);
    } catch (JSONException e) {
      this.req = null;
    }
    this.packetType = json.getInt(PaxosPacket.PACKET_TYPE_FIELD_NAME);
    this.slot = json.getInt(SLOT);
    if (json.has(GC_SLOT)) {
      gcSlot = json.getInt(GC_SLOT);
    }
  }

  public ProposalPacket<NodeIDType> getDecisionPacket() {
    return new ProposalPacket<NodeIDType>(slot, req, PaxosPacketType.DECISION, gcSlot);
  }

  public void makeDecisionPacket() {
    this.packetType = PaxosPacketType.DECISION.getInt();
//        this.req = null;

  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = this.req.toJSONObject();
    json.put(SLOT, slot);
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, packetType);
    if (gcSlot > 0) {
      json.put(GC_SLOT, gcSlot);
    }
    return json;
  }

}
