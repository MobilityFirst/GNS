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

import java.util.TimerTask;

/**
 * This class checks if replica controllers have updated the database indicating that the group change for the name
 * is complete. If yes, the task is immediately cancelled, otherwise it again proposes a request to replica controllers
 * to mark that group change is complete.
 * <p>
 * IMPORTANT: We may need to request replica controllers again because a proposed paxos request may not get committed.
 * Therefore, it is necessary to retry a request. How many times to retry?  Usually, requests need to be tried
 * on a coordinator failure, therefore we should retry longer than the failure detection interval.
 * <p>
 * User: abhigyan
 * Date: 11/14/13
 * Time: 9:24 AM
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

  int MAX_RETRY = 10;

  public WriteActiveNameServersRunningTask(String name, String paxosID) {
    this.name = name;
    this.paxosID = paxosID;
    int timeout = NameServer.getPaxosManager().getFailureDetectionTimeout();
    if (timeout != -1) MAX_RETRY = (int)(timeout * 1.5 / ReplicaController.RC_TIMEOUT_MILLIS );
  }

  @Override
  public void run() {

    count++;

    ReplicaControllerRecord rcRecord;
    try {
      rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(NameServer.getReplicaController(), name,
              ReplicaControllerRecord.ACTIVE_PAXOS_ID, ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
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

      if (rcRecord.isActiveRunning()) {
        GNS.getLogger().info("Group change complete. Record updated. Name " + name + "\tPaxosID\t" + paxosID);
        this.cancel();
        return;
      }
    } catch (FieldNotFoundException e) {
      e.printStackTrace();
      GNS.getLogger().severe("Field Not Found Exception " + e.getMessage() + "\tPaxosID\t" + paxosID + "\tcount" +
              count);
      this.cancel();
      return;
    }
    if (count == MAX_RETRY) {
      GNS.getLogger().severe(" ERROR: Max retries reached: Active name servers not written. ");
      this.cancel();
      return;
    }

    if (StartNameServer.debugMode) GNS.getLogger().info("PAXOS PROPOSAL write active NS running. PaxosID " + paxosID +
            "\tcount\t" + count);

    // otherwise propose again.
    ChangeActiveStatusPacket proposePacket = new ChangeActiveStatusPacket(paxosID, name,
            Packet.PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY);

    // write to replica controller record object using Primary-paxos that newActive is running
    NameServer.getPaxosManager().propose(ReplicaController.getPrimaryPaxosID(name), new RequestPacket(
            Packet.PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY.getInt(), proposePacket.toString(),
            PaxosPacketType.REQUEST, false));
  }
}
