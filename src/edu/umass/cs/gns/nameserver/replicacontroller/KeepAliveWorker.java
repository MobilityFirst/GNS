package edu.umass.cs.gns.nameserver.replicacontroller;

import java.util.TimerTask;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 8/9/13
 * Time: 12:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class KeepAliveWorker extends TimerTask{
  @Override
  public void run() {
    // TODO uncomment code below and implement this.
  }
//  JSONObject incomingJson;
//
//  public KeepAliveWorker(JSONObject json) {
//    this.incomingJson = json;
//  }
//
//
//  public static void handleIncomingPacket(JSONObject json) throws JSONException{
//    NameServer.executorService.submit(new KeepAliveWorker(json));
//  }
//
//  @Override
//  public void run() {
//
//    try {
//      switch (Packet.getPacketType(incomingJson.getInt(Packet.TYPE))) {
//        case KEEP_ALIVE_PRIMARY:
//          handleKeepAlivePrimary();
//          break;
//        case DELETE_PRIMARY:
//          handleDeletePrimary();
//          break;
//
//      }
//    } catch (JSONException e) {
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    } catch (IOException e) {
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    }
//  }
//
//  private void handleKeepAlivePrimary() throws JSONException, IOException {
//    KeepAlivePacket packet = new KeepAlivePacket(incomingJson);
//    ReplicaControllerRecord record = NameServer.getNameRecordPrimaryLazy(packet.getName());
//
//    if (StartNameServer.debugMode) GNS.getLogger().fine(" Received keep alive for name: " + packet.getName()
//            + " from " + packet.getSender());
//    if (record != null && record.isMarkedForRemoval() == false) {
//      if (StartNameServer.debugMode) GNS.getLogger().fine(" UpdateTrace keep alive for name: " + packet.getName());
//      record.setKeepAliveTime(System.currentTimeMillis()/1000); // convert to sec
//      return;
//    }
//
//    if (record == null) {
//      // TODO query for current state, add to  database, and then create paxos instance.
//      PaxosManager.createPaxosInstance(ReplicaController.getPrimaryPaxosID(packet.getName()),
//              HashFunction.getPrimaryReplicas(packet.getName()),null);
//      return;
//    }
//
//    if (record.isMarkedForRemoval()) {
//      KeepAlivePacket reply = new KeepAlivePacket(packet.getName(),null,NameServer.nodeID, Packet.PacketType.DELETE_PRIMARY);
////      NameServer.tcpTransport.sendToID(reply.toJSONObject(), packet.getLnsId(), GNS.PortType.PERSISTENT_TCP_PORT);
//      NameServer.tcpTransport.sendToID(packet.getSender(), reply.toJSONObject());
//      // send delete req
//    }
//
//
//
//  }
//
//
//  private void handleDeletePrimary() throws JSONException {
//    KeepAlivePacket delPacket = new KeepAlivePacket(incomingJson);
//    ReplicaControllerRecord record = NameServer.getNameRecordPrimaryLazy(delPacket.getName());
//    if (record != null) {
//      record.setMarkedForRemoval();
//      PaxosManager.deletePaxosInstance(ReplicaController.getPrimaryPaxosID(delPacket.getName()));
//    }
//  }

}
