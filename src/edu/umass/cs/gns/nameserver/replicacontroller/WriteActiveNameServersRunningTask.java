package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.ChangeActiveStatusPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosManager;

import java.util.TimerTask;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 11/14/13
 * Time: 9:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class WriteActiveNameServersRunningTask extends TimerTask {

  /**
   *
   */
  String name;

  /**
   *
   */
  String paxosID;

  int count = 0;

  public WriteActiveNameServersRunningTask(String name, String paxosID) {
    this.name = name;
    this.paxosID = paxosID;
  }
  @Override
  public void run() {

    count++;

    ReplicaControllerRecord rcRecord;
    try {
      rcRecord = NameServer.getNameRecordPrimaryMultiField(name, ReplicaControllerRecord.ACTIVE_PAXOS_ID,
              ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("RCRecord\t" + name + "\tdeleted. Task Cancelled");
      this.cancel();
      return;
    }

    try {
      if (rcRecord.getActivePaxosID().equals(paxosID) == false) {
        GNS.getLogger().warning("Group change to  " + paxosID + " complete. and new group change started.");
        this.cancel();
        return;
      }

      if (rcRecord.isActiveRunning() == true) {
        GNS.getLogger().info("Group change complete. Record updated. Name " + name + "\tPaxosID\t" + paxosID);
        this.cancel();
        return;
      }
    } catch (FieldNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      GNS.getLogger().severe("Field Not Found Exception " + e.getMessage() + "\tPaxosID\t" + paxosID + "\tcount" +
              count);
      this.cancel();
      return;
    }

    if (StartNameServer.debugMode) GNS.getLogger().info("PAXOS PROPOSAL write active NS running. PaxosID " + paxosID +
            "\tcount\t" + count);

    // otherwise propose again.
    ChangeActiveStatusPacket proposePacket = new ChangeActiveStatusPacket(paxosID, name,
            Packet.PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY);

    // write to replica controller record object using Primary-paxos that newActive is running
    PaxosManager.propose(ReplicaController.getPrimaryPaxosID(name), new RequestPacket(
            Packet.PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY.getInt(), proposePacket.toString(),
            PaxosPacketType.REQUEST, false));
  }
}
