package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.gns.paxos.Ballot;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Represents the state of a paxos instance that is periodically logged to disk.
 * User: abhigyan
 * Date: 8/2/13
 * Time: 10:13 AM
 * To change this template use File | Settings | File Templates.
 */
public class StatePacket extends PaxosPacket {

  public Ballot b;
  public int slotNumber;
  public String state;

  public StatePacket(Ballot b, int slotNumber, String state) {
    this.b = b;
    this.slotNumber = slotNumber;
    this.state = state;
  }


  @Override
  public JSONObject toJSONObject() throws JSONException {
    throw new UnsupportedOperationException();

  }

}
