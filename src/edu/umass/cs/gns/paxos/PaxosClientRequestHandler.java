package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 8:31 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class PaxosClientRequestHandler {

    public abstract void handleRequestFromClient(String paxosID, RequestPacket requestPacket);

    public abstract void forwardDecisionToClient(String paxosID, RequestPacket requestPacket);

    public abstract void handleFailureMessage(FailureDetectionPacket fdPacket);
}
