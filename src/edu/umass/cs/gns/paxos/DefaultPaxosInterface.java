package edu.umass.cs.gns.paxos;

//import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nio.NioServer;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import org.json.JSONException;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 8:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class DefaultPaxosInterface implements PaxosInterface {

  int nodeID;


  NioServer nioServer;

//    ConcurrentHashMap<Integer, Integer> requestsReceived = new ConcurrentHashMap<Integer, Integer>();

  public DefaultPaxosInterface(int nodeID, NioServer nioServer) {
    this.nodeID = nodeID;
    this.nioServer = nioServer;
  }

//    public void proposeRequestToPaxos(String paxosID, RequestPacket requestPacket) {
//        if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tReceived Request From Client: " + requestPacket.value);
//        // the replica which receives request from client stores the request in requestsReceived
//        // when decision is made, response is sent back to client.
////        requestsReceived.put(requestPacket.requestID, requestPacket.requestID);
//        PaxosManager.propose(paxosID, requestPacket);
//    }

    @Override
    public void handlePaxosDecision(String paxosID, RequestPacket requestPacket, boolean recovery) {

        if (nodeID == 0)
        // if I received this request from client, i will send reply to client.
//        if (requestsReceived.remove(requestPacket.requestID) != null) {
            try {
//              GNS.getLogger().info("sending response: " + nodeID);
              nioServer.sendToID(requestPacket.clientID, requestPacket.toJSONObject());

            } catch (JSONException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (IOException e) {
              e.printStackTrace();
            }
//        }

    }

    @Override
    public void handleFailureMessage(FailureDetectionPacket fdPacket) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

  @Override
  public String getState(String paxosID) {
    return "ABCD\nEFGH\nIJKL\nMNOP\n";  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void updateState(String paxosID, String state) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public String getPaxosKeyForPaxosID(String paxosID) {
    return paxosID;
  }
}
