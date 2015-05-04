package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.paxos.AbstractPaxosManager;
import edu.umass.cs.gns.gigapaxos.PaxosManager;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket;
import org.json.JSONObject;

import java.util.Set;

/**
 * Class created to patch over differences with the new gigapaxos code. The gigapaxos package assumes that
 * the parameter 'String value' in these two 'propose' methods are actually RequestPacket objects represented
 * as a string:
 *
 * public String propose(String paxosIDNoVersion, String value);
 * public String proposeStop(String paxosIDNoVersion, String value, short version)
 *
 * The above two methods in this class wrap the actual value inside a RequestPacket before calling the
 * propose/proposeStop methods.
 * 
 */
@Deprecated
public class PaxosManagerTransition<NodeIDType> extends AbstractPaxosManager<NodeIDType> {

  private PaxosManager<NodeIDType> paxosManager;

  public PaxosManagerTransition(PaxosManager<NodeIDType> paxosManager) {
    this.paxosManager = paxosManager;
  }

  @Override
  public boolean createPaxosInstance(String paxosIDNoVersion, short version, Set<NodeIDType> nodeIDs, Replicable paxosInterface) {
    return paxosManager.createPaxosInstance(paxosIDNoVersion, version, nodeIDs, paxosInterface);
  }
  
  @Override
  public Set<NodeIDType> getPaxosNodeIDs(String paxosIDNoVersion) {
    return paxosManager.getPaxosNodeIDs(paxosIDNoVersion);
  }

  @Override
  public String propose(String paxosIDNoVersion, String value) {
    RequestPacket requestPacket = new RequestPacket(-1, value, false);
    return paxosManager.propose(paxosIDNoVersion, requestPacket.toString());
  }

  @Override
  public String proposeStop(String paxosIDNoVersion, String value, short version) {
    RequestPacket requestPacket = new RequestPacket(-1, value, true);
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
