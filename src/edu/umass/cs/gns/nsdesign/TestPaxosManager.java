package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.paxos.AbstractPaxosManager;
import edu.umass.cs.gns.replicaCoordination.multipaxos.PaxosManager;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

/**
 * Temporary class created for the testing with multipaxos package. The multipaxos package assumes that
 * the parameter 'String value' in these two 'propose' methods are actually RequestPacket objects represented
 * as a string:
 *
 * public String propose(String paxosIDNoVersion, String value);
 * public String proposeStop(String paxosIDNoVersion, String value, short version)
 *
 * The above two methods in this class wrap the actual value inside a RequestPacket before calling the
 * propose/proposeStop methods.
 *
 * Created by abhigyan on 6/17/14.
 */
public class TestPaxosManager extends AbstractPaxosManager {

  private PaxosManager paxosManager;

  public TestPaxosManager(PaxosManager paxosManager) {
    this.paxosManager = paxosManager;
  }

  @Override
  public boolean createPaxosInstance(String paxosIDNoVersion, short version, Set<Integer> nodeIDs,
                                     Replicable paxosInterface) {
    return paxosManager.createPaxosInstance(paxosIDNoVersion, version, nodeIDs, paxosInterface);
  }

  @Override
  public Set<Integer> getPaxosNodeIDs(String paxosIDNoVersion) {
    return paxosManager.getPaxosNodeIDs(paxosIDNoVersion);
  }

  @Override
  public String propose(String paxosIDNoVersion, String value) {
    RequestPacket requestPacket = new RequestPacket(0, value, false);
//    try {
//      GNS.getLogger().info(" Size of value: " + value.length() + "\trequestpacket size " + requestPacket.toJSONObject().toString().length());
//      GNS.getLogger().info(" String value: " + value);
//      GNS.getLogger().info(" String requestpacket " + requestPacket.toJSONObject().toString());
//    } catch (JSONException e) {
//      e.printStackTrace();
//    }
    return paxosManager.propose(paxosIDNoVersion, requestPacket.toString());
  }

  @Override
  public String proposeStop(String paxosIDNoVersion, String value, short version) {
    RequestPacket requestPacket = new RequestPacket(0, value, true);
    return paxosManager.proposeStop(paxosIDNoVersion, requestPacket.toString(), version);
  }

  @Override
  public void handleIncomingPacket(JSONObject json) {
    paxosManager.handleIncomingPacket(json);
  }

  @Override
  public void resetAll() {
    paxosManager.resetAll();
  }
}
