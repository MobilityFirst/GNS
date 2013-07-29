package paxos;

import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.main.StartNameServer;
import edu.umass.cs.gnrs.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gnrs.packet.paxospacket.RequestPacket;
import org.json.JSONException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 8:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class DefaultPaxosClientRequestHandler extends PaxosClientRequestHandler {

    ConcurrentHashMap<Integer, Integer> requestsReceived = new ConcurrentHashMap<Integer, Integer>();

    @Override
    public void handleRequestFromClient(String paxosID, RequestPacket requestPacket) {
        if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tReceived Request From Client: " + requestPacket.value);
        // the replica which receives request from client stores the request in requestsReceived
        // when decision is made, response is sent back to client.
//        requestsReceived.put(requestPacket.requestID, requestPacket.requestID);
        PaxosManager.propose(paxosID, requestPacket);
    }

    @Override
    public void forwardDecisionToClient(String paxosID, RequestPacket requestPacket) {
        if (PaxosManager.nodeID == 0)
        // if I received this request from client, i will send reply to client.
//        if (requestsReceived.remove(requestPacket.requestID) != null) {
            try {
                PaxosManager.sendMessage(requestPacket.clientID, requestPacket.toJSONObject());
            } catch (JSONException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
//        }

    }

    @Override
    public void handleFailureMessage(FailureDetectionPacket fdPacket) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
