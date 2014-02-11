package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;

/**
 * This interface must be implemented by the system (e.g. GNS) to use the paxos instance
 * User: abhigyan
 * Date: 6/29/13
 * Time: 8:31 PM
 * To change this template use File | Settings | File Templates.
 */
public interface PaxosInterface {

//    public abstract void proposeRequestToPaxos(String paxosID, RequestPacket requestPacket);

  /**
   *
   * @param paxosID
   * @param requestPacket
   */
    public abstract void handlePaxosDecision(String paxosID, RequestPacket requestPacket, boolean recovery);

    public abstract void handleFailureMessage(FailureDetectionPacket fdPacket);

    public abstract String getState(String paxosID);

    public abstract void updateState(String paxosID, String state);




}
