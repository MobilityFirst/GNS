package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Temporary class created for the testing with multipaxos package. This is the replicable app that we give to
 * multipaxos. This replicable app creates a wrapper around the actual replicable app (GNSReconfigurable or ReplicaController).
 *
 * Why this class? When multipaxos calls the method handleDecision, the parameter 'String value' in
 * that method is actually a RequestPacket represented in string form. The actual replicable app
 * does not recognize RequestPacket. but expects that 'value' is actually
 * field 'requestValue' of RequestPacket. The handleDecision method extracts 'requestValue' from RequestPacket and
 * calls the handleDecision of the actual replicable app.
 *
 * Created by abhigyan on 6/17/14.
 */
public class TestReplicable implements Replicable{

  private Replicable replicable;

  public TestReplicable(Replicable replicable) {
    this.replicable = replicable;
  }

  @Override
  public boolean handleDecision(String name, String value, boolean doNotReplyToClient) {
    RequestPacket requestPacket;
    try {
      requestPacket = new RequestPacket(new JSONObject(value));
    } catch (JSONException e) {
      e.printStackTrace();
      return false;
    }
    return handleDecision(name, requestPacket.requestValue, doNotReplyToClient);
  }

  @Override
  public String getState(String name) {
    return replicable.getState(name);
  }

  @Override
  public boolean updateState(String name, String state) {
    return replicable.updateState(name, state);
  }
}
