package edu.umass.cs.gns.nsdesign.activeReconfiguration;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.TransferableNameRecordState;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONException;

import java.io.IOException;
import java.util.HashSet;
import java.util.TimerTask;

/**
 *
 * todo write doc here.
 * Created by abhigyan on 3/28/14.
 * 
 * Arun: 
 * @param <NodeIDType>
 */
public class CopyStateFromOldActiveTask<NodeIDType> extends TimerTask {

  private ActiveReplica<?,?> activeReplica;

  private NewActiveSetStartupPacket packet;

  private HashSet<NodeIDType> oldActivesQueried;

  private int requestID;

  public CopyStateFromOldActiveTask(NewActiveSetStartupPacket packet, ActiveReplica<?,?> activeReplica) throws JSONException {
    this.oldActivesQueried = new HashSet<NodeIDType>();
    this.activeReplica = activeReplica;
    // first, store the original packet in hash map
    this.requestID = activeReplica.getOngoingStateTransferRequests().put(packet);
    // next, create a copy as which we will modify
    this.packet = new NewActiveSetStartupPacket(packet.toJSONObject());
    this.packet.setUniqueID(this.requestID);  // ID assigned by this active replica.
    this.packet.changePacketTypeToPreviousValueRequest();
    this.packet.changeSendingActive(activeReplica.getNodeID());
  }

  private static void makeFakeResponse(NewActiveSetStartupPacket packet) {
    packet.changePreviousValueCorrect(true);
    ValuesMap valuesMap = new ValuesMap();
    ResultValue rv = new ResultValue();
    rv.add("pqrst");
    valuesMap.putAsArray("EdgeRecord", rv);
    packet.changePacketTypeToPreviousValueResponse();
    packet.changePreviousValue(new TransferableNameRecordState(valuesMap, 0).toString());
  }

  public void run_alt() {
    makeFakeResponse(packet);
    try {
      activeReplica.getNioServer().sendToID(activeReplica.getNodeID(), packet.toJSONObject());
      throw new CancelExecutorTaskException();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (CancelExecutorTaskException e) {
      throw new RuntimeException();
    }
  }

  public void run() {
    if (Config.dummyGNS) {
      run_alt();
      return;
    }
    try {

      if (Config.debuggingEnabled) GNS.getLogger().info(" NEW_ACTIVE_START_FORWARD received packet: " + packet.toJSONObject());

      if (activeReplica.getOngoingStateTransferRequests().get(requestID) == null) {
        GNS.getLogger().info(" COPY State from Old Active Successful! Cancel Task; Actives Queried: " + oldActivesQueried);
        throw new CancelExecutorTaskException();
      }

      // select old active to send request to
      NodeIDType oldActive = (NodeIDType) activeReplica.getGnsNodeConfig().getClosestServer(packet.getOldActiveNameServers(),
              oldActivesQueried);

      if (oldActive == null) {
        // this will happen after all actives have been tried at least once.
        GNS.getLogger().severe(" Exception ERROR:  No More Actives Left To Query. Cancel Task!!! " + packet + " Actives queried: " + oldActivesQueried);
        activeReplica.getOngoingStateTransferRequests().remove(requestID);
        throw new CancelExecutorTaskException();
      }
      oldActivesQueried.add(oldActive);
      if (Config.debuggingEnabled) GNS.getLogger().info(" OLD ACTIVE SELECTED = : " + oldActive);

      try {
        activeReplica.getNioServer().sendToID(oldActive, packet.toJSONObject());
      } catch (IOException e) {
        GNS.getLogger().severe(" IOException here: " + e.getMessage());
        e.printStackTrace();
      } catch (JSONException e) {
        GNS.getLogger().severe(" JSONException here: " + e.getMessage());
        e.printStackTrace();
      }
      if (Config.debuggingEnabled) GNS.getLogger().info(" REQUESTED VALUE from OLD ACTIVE. PACKET: " + packet);

    } catch (JSONException e) {
      e.printStackTrace();
    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException(); // this is only way to cancel this task as it is run via ExecutorService
      }
      // other types of exception are not expected. so, log them.
      GNS.getLogger().severe("Exception in Copy State from old actives task. " + e.getMessage());
      e.printStackTrace();
      throw new RuntimeException();
    }
  }
}
