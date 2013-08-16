package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.KeepAlivePacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.util.HashFunction;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.TimerTask;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 8/9/13
 * Time: 12:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class KeepAliveWorker extends TimerTask{
  JSONObject incomingJson;

  public KeepAliveWorker(JSONObject json) {
    this.incomingJson = json;
  }


  public static void handleIncomingPacket(JSONObject json) throws JSONException{
    NameServer.executorService.submit(new KeepAliveWorker(json));
  }

  @Override
  public void run() {

    try {
      switch (Packet.getPacketType(incomingJson.getInt(Packet.TYPE))) {
        case KEEP_ALIVE_PRIMARY:
          handleKeepAlivePrimary();
          break;
        case DELETE_PRIMARY:
          handleDeletePrimary();
          break;

      }
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  private void handleDeletePrimary() throws JSONException {
    KeepAlivePacket delPacket = new KeepAlivePacket(incomingJson);
    ReplicaControllerRecord record = NameServer.getNameRecordPrimaryLazy(delPacket.getName());
    if (record != null) {
      record.setMarkedForRemoval();
      PaxosManager.deletePaxosInstance(ReplicaController.getPrimaryPaxosID(delPacket.getName()));
    }
  }


  private void handleKeepAlivePrimary() throws JSONException, IOException {
    KeepAlivePacket packet = new KeepAlivePacket(incomingJson);
    ReplicaControllerRecord record = NameServer.getNameRecordPrimaryLazy(packet.getName());

    if (StartNameServer.debugMode) GNS.getLogger().fine(" Received keep alive for name: " + packet.getName()
            + " from " + packet.getSender());
    if (record != null && record.isMarkedForRemoval() == false) {
      if (StartNameServer.debugMode) GNS.getLogger().fine(" UpdateTrace keep alive for name: " + packet.getName());
      record.setKeepAliveTime(System.currentTimeMillis()/1000); // convert to sec
      return;
    }

    if (record == null) {
      // TODO query for current state, add to  database, and then create paxos instance.
      PaxosManager.createPaxosInstance(ReplicaController.getPrimaryPaxosID(packet.getName()),
              HashFunction.getPrimaryReplicas(packet.getName()),null);
      return;
    }

    if (record.isMarkedForRemoval()) {
      KeepAlivePacket reply = new KeepAlivePacket(packet.getName(),null,NameServer.nodeID, Packet.PacketType.DELETE_PRIMARY);
//      NameServer.tcpTransport.sendToID(reply.toJSONObject(), packet.getSender(), GNS.PortType.STATS_PORT);
      NameServer.tcpTransport.sendToID(packet.getSender(), reply.toJSONObject());
      // send delete req
    }



  }

}
