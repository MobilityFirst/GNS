package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.RequestActivesPacket;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.util.*;

public class SendUpdatesTask extends TimerTask {

  String name;
  UpdateAddressPacket updateAddressPacket;
  InetAddress senderAddress;
  int senderPort;
  int updateRequestID;
  HashSet<Integer> activesQueried;
  int timeoutCount = -1;
  long requestRecvdTime;
  int numRestarts;
  int coordinatorID = -1;

  public SendUpdatesTask(UpdateAddressPacket updateAddressPacket, InetAddress senderAddress, int senderPort,
          long requestRecvdTime, HashSet<Integer> activesQueried, int numRestarts) {
    this.name = updateAddressPacket.getName();
    //this.nameRecordKey = updateAddressPacket.getRecordKey();
    this.updateAddressPacket = updateAddressPacket;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.activesQueried = activesQueried;
    this.requestRecvdTime = requestRecvdTime;
    this.numRestarts = numRestarts;
  }

  @Override
  public void run() {
    try {

      timeoutCount++;

      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("ENTER name = " + name + " timeout = " + timeoutCount);
      }

      if (timeoutCount > 0 && LocalNameServer.getUpdateInfo(updateRequestID) == null) {

        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("UpdateInfo not found. Update complete or actives invalidated. Cancel task.");
        }
        throw new CancelExecutorTaskException();
      }

      if (System.currentTimeMillis() - requestRecvdTime > StartLocalNameServer.maxQueryWaitTime) {
        // send failed msg to user and log error
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("UPDATE FAILED no response until MAX-wait time: request ID = " + updateRequestID + " name = " + name);
        }
        handleFailure();
        throw new CancelExecutorTaskException();
      }

      int nameServerID;

      if (StartLocalNameServer.replicateAll) {
        nameServerID = BestServerSelection.getSmallestLatencyNS(ConfigFileInfo.getAllNameServerIDs(), activesQueried);
      } else {
        CacheEntry cacheEntry = LocalNameServer.getCacheEntry(name);

        if (cacheEntry == null) {
          RequestActivesPacket pkt = new RequestActivesPacket(name, LocalNameServer.nodeID);
          pkt.setActiveNameServers(HashFunction.getPrimaryReplicas(name));
          cacheEntry = LocalNameServer.addCacheEntry(pkt);
        }

        if (cacheEntry == null || cacheEntry.isValidNameserver() == false) {
          // remove update info from LNS
          if (timeoutCount > 0) {
            LocalNameServer.removeUpdateInfo(updateRequestID);
          }

          // add to pending requests task
          try {
            PendingTasks.addToPendingRequests(name,
                    new SendUpdatesTask(updateAddressPacket, senderAddress, senderPort, requestRecvdTime,
                    new HashSet<Integer>(), numRestarts + 1),
                    StartLocalNameServer.queryTimeout,
                    ConfirmUpdateLNSPacket.createFailPacket(updateAddressPacket).toJSONObject(),
                    UpdateInfo.getUpdateFailedStats(name, new HashSet<Integer>(), LocalNameServer.nodeID,
                    updateAddressPacket.getRequestID(), requestRecvdTime, numRestarts + 1, -1), 0);

          } catch (JSONException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
          }

          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("Created a request actives task. " + numRestarts);
          }
          // cancel this task
          throw new CancelExecutorTaskException();
        }

//        else
        if (StartLocalNameServer.loadDependentRedirection) {
          nameServerID = LocalNameServer.getBestActiveNameServerFromCache(name, activesQueried);
        } else if (StartLocalNameServer.replicationFramework == ReplicationFrameworkType.BEEHIVE) {
          nameServerID = LocalNameServer.getBeehiveNameServerFromCache(name, activesQueried);
        } else {
          nameServerID = BestServerSelection.getSmallestLatencyNS(cacheEntry.getActiveNameServers(), activesQueried);
          coordinatorID = LocalNameServer.getDefaultCoordinatorReplica(name, cacheEntry.getActiveNameServers());
        }

      }

      if (nameServerID == -1) {

        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("ERROR: No more actives left to query. Actives Queried " + activesQueried);
        }
        return;
      }

      activesQueried.add(nameServerID);

      if (timeoutCount == 0) {
        String hostAddress = null;
        if (senderAddress != null) {
          hostAddress = senderAddress.getHostAddress();
        }
        updateRequestID = LocalNameServer.addUpdateInfo(name, nameServerID,
                requestRecvdTime, hostAddress, senderPort, numRestarts, updateAddressPacket);
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("Update Info Added: Id = " + updateRequestID);
        }
      }
      // create the packet that we'll send to the primary
      UpdateAddressPacket pkt = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_LNS,
              updateAddressPacket.getRequestID(), updateRequestID,
              name, updateAddressPacket.getRecordKey(),
              updateAddressPacket.getUpdateValue(),
              updateAddressPacket.getOldValue(),
              updateAddressPacket.getOperation(),
              LocalNameServer.nodeID, nameServerID, updateAddressPacket.getTTL());

      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Sending Update to Node: " + nameServerID);
      }

      // and send it off
      try {
        JSONObject jsonToSend = pkt.toJSONObject();
        LocalNameServer.sendToNS(jsonToSend, nameServerID);
        UpdateInfo updateInfo = LocalNameServer.getUpdateInfo(nameServerID);
        if (updateInfo != null) {
          updateInfo.setNameserverID(nameServerID);
        }

        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("LNSListenerUpdate: Send to: " + nameServerID + " Name:" + name + " Id:" + updateRequestID
                  + " Time:" + System.currentTimeMillis()
                  + " --> " + jsonToSend.toString());
        }
      } catch (JSONException e) {
        e.printStackTrace();
      }

    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Exception Exception Exception ... ");
      e.printStackTrace();
    }
  }


  private void handleFailure() {
    ConfirmUpdateLNSPacket confirmPkt = ConfirmUpdateLNSPacket.createFailPacket(updateAddressPacket);
    try {
      Intercessor.handleIncomingPackets(confirmPkt.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }

    UpdateInfo updateInfo = LocalNameServer.removeUpdateInfo(updateRequestID);
    if (updateInfo == null) {
      if (timeoutCount == 0) {
        GNS.getStatLogger().fine(UpdateInfo.getUpdateFailedStats(name, activesQueried,
                LocalNameServer.nodeID, updateAddressPacket.getRequestID(), requestRecvdTime, numRestarts, coordinatorID));
      }
    } else {
      GNS.getStatLogger().fine(updateInfo.getUpdateFailedStats(activesQueried, LocalNameServer.nodeID, updateAddressPacket.getRequestID(), coordinatorID));

    }
  }
}
