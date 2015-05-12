package edu.umass.cs.gns.newApp;

import java.io.IOException;
import java.util.Set;
import org.json.JSONException;
import edu.umass.cs.gns.gigapaxos.PaxosManager;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurable;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicable;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.util.Stringifiable;
import org.json.JSONObject;

/**
 * @author Westy
 * @param <NodeIDType>
 */
public class NewAppCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

  private final NodeIDType nodeID;
  private final Stringifiable<NodeIDType> unstringer;
  private final PaxosManager<NodeIDType> paxosManager;

  NewAppCoordinator(InterfaceReplicable app, Stringifiable<NodeIDType> unstringer, JSONMessenger<NodeIDType> messenge) {
    super(app, messenge);
    this.paxosManager = new PaxosManager<NodeIDType>(messenger.getMyID(), unstringer, messenger, this);
    this.nodeID = messenger.getMyID();
    this.unstringer = unstringer;
  }

  @Override
  public boolean coordinateRequest(InterfaceRequest request)
          throws IOException, RequestParseException {
    if (this.app == null) {
      return false; // replicable app not set
    }
    JSONObject json = null;
    try {
      json = new JSONObject(request.toString());
    } catch (JSONException e) {
      throw new RequestParseException(e);
    }
    InterfaceRequest callHandleDecision = null;
    try {
      Packet.PacketType type = Packet.getPacketType(json);
      if (type != null) {
        if (Config.debuggingEnabled) {
          GNS.getLogger().info("######################## Received " + type + " : " + json);
        }
        switch (type) {
          // call propose
          case UPDATE: // updates need coordination
            UpdatePacket<NodeIDType> update = new UpdatePacket<NodeIDType>(json, unstringer);
            update.setNameServerID(nodeID);
            if (Config.debuggingEnabled) {
              GNS.getLogger().info("@@@@@@@@@@@@@@@@@@@@@ Proposing update for " + update.getName() + ": " + update.toString());
            }
            String paxosID = paxosManager.propose(update.getName(), update.toString());
            if (paxosID == null) {
              callHandleDecision = update;
              GNS.getLogger().warning("Update no paxos state: " + update);
            }
            break;
          default:
            GNS.getLogger().severe("Packet type not found in coordination: " + type);
            break;
        }
        if (callHandleDecision != null) {
          callHandleDecisionWithRetry(callHandleDecision, false);
        }
      } else { // packet type was null
        if (Config.debuggingEnabled) {
          GNS.getLogger().warning("Received unknown packet type " + json);
        }
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return true;
  }

  private static final long HANDLE_DECISION_RETRY_INTERVAL_MILLIS = 1000;

  /**
   * Retries a request at period interval until successfully executed by application.
   */
  private void callHandleDecisionWithRetry(InterfaceRequest request, boolean doNotReplyToClient) {
    while (!app.handleRequest(request, doNotReplyToClient)) {
      try {
        Thread.sleep(HANDLE_DECISION_RETRY_INTERVAL_MILLIS);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      GNS.getLogger().severe("Failed to execute decision. Retry. name = "
              + request.getServiceName() + " value = " + request.toString());
    }
  }

  @Override
  public boolean createReplicaGroup(String serviceName, int epoch,
          String state, Set<NodeIDType> nodes) {
    this.paxosManager.createPaxosInstance(serviceName, (short) epoch,
            nodes, this, state);
    /* FIXME: This putInitialState may not happen atomically with paxos
     * instance creation. However, gigapaxos currently has no way to 
     * specify any initial state.
     */
    //this.app.putInitialState(serviceName, epoch, state);
    return true;
  }

  @Override
  public void deleteReplicaGroup(String serviceName, int epoch) {
    // FIXME: invoke paxosManager remove here
  }

  @Override
  public Set<NodeIDType> getReplicaGroup(String serviceName) {
    return this.paxosManager.getPaxosNodeIDs(serviceName);
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return this.app.getRequestTypes();
  }

  @Override
  public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
    if (this.app instanceof InterfaceReconfigurable) {
      return ((InterfaceReconfigurable) this.app).getStopRequest(name,
              epoch);
    }
    throw new RuntimeException(
            "Can not get stop request for a non-reconfigurable app");
  }

}
