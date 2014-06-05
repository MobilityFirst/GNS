package edu.umass.cs.gns.paxos.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.paxos.Ballot;

import java.io.Serializable;

public class PValuePacket extends PaxosPacket implements Serializable {

  public Ballot ballot;
  public ProposalPacket proposal;

  public PValuePacket(Ballot b, ProposalPacket p) {
    this.ballot = b;
    this.proposal = p;
  }

  static String BALLOT = "b1";

  public PValuePacket(JSONObject json) throws JSONException {
    this.proposal = new ProposalPacket(json);
    this.ballot = new Ballot(json.getString(BALLOT));
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = this.proposal.toJSONObject();
    json.put(BALLOT, ballot.toString());
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, packetType);
    return json;
  }

}
