package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.recordExceptions.FieldNotFoundException;
import edu.umass.cs.gns.packet.KeepAlivePacket;
import edu.umass.cs.gns.packet.Packet;
import org.json.JSONException;

import java.io.IOException;
import java.util.Set;
import java.util.TimerTask;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 8/9/13
 * Time: 12:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class SenderKeepAliveRC extends TimerTask{

  public static int KEEP_ALIVE_INTERVAL_SEC = 5;

  @Override
  public void run() {

    Set<ReplicaControllerRecord> nameRecords = NameServer.getAllPrimaryNameRecords();
    if (StartNameServer.debugMode) GNS.getLogger().fine("\tKeep Alive Sender\tNumberOfNameRecords\t"
            + nameRecords.size());
    long curTimeSec = System.currentTimeMillis()/1000; // convert to sec

    for (ReplicaControllerRecord record : nameRecords) {
      try {
        if (record.isMarkedForRemoval()) continue; // deleted or will-be deleted; ignore


        if (curTimeSec - record.getKeepAliveTime() >= KEEP_ALIVE_INTERVAL_SEC) {
          GNS.getLogger().fine(" Send keep alive for name: " + record.getName() +
                  " time diff = "  + (curTimeSec - record.getKeepAliveTime()));
          record.setKeepAliveTime(curTimeSec);
          // send message to other primaries
          KeepAlivePacket packet = new KeepAlivePacket(record.getName(), null, NameServer.nodeID,
                  Packet.PacketType.KEEP_ALIVE_PRIMARY);
//          NameServer.tcpTransport.sendToAll(packet.toJSONObject(),record.getPrimaryNameservers(),
//                  GNS.PortType.PERSISTENT_TCP_PORT,NameServer.nodeID);
          NameServer.tcpTransport.sendToIDs(record.getPrimaryNameservers(), packet.toJSONObject(),NameServer.nodeID);
        } else {
          GNS.getLogger().fine(" NOT Sending keep alive for record. " + record.getName() +
                  " time diff = " + (curTimeSec - record.getKeepAliveTime()));
        }
      } catch (JSONException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (FieldNotFoundException e) {
        GNS.getLogger().fine("Field not found exception. " + e.getMessage());
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }
}


