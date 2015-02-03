package edu.umass.cs.gns.newApp;

import java.io.IOException;
import java.util.Set;
import org.json.JSONException;
import edu.umass.cs.gns.gigapaxos.PaxosManager;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nsdesign.Config;
import static edu.umass.cs.gns.nsdesign.Config.readCoordination;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.RequestActivesPacket;
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
public class AppCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

  private final NodeIDType nodeID;
  private final Stringifiable<NodeIDType> unstringer;
  private final PaxosManager<NodeIDType> paxosManager;

  AppCoordinator(InterfaceReplicable app, Stringifiable<NodeIDType> unstringer, JSONMessenger<NodeIDType> messenge) {
    super(app, messenge);
    this.paxosManager = new PaxosManager<NodeIDType>(messenger.getMyID(), unstringer, messenger, this);
    this.nodeID = messenger.getMyID();
    this.unstringer = unstringer;
  }

  @Override
  public boolean coordinateRequest(InterfaceRequest request)
          throws IOException, RequestParseException {
    JSONObject json = null;
    try {
      json = new JSONObject(request.toString());
    } catch (JSONException e) {
      throw new RequestParseException(e);
    }
    if (this.app == null) {
      return false; // replicable app not set
    }
    InterfaceRequest callHandleDecision = null;
    boolean noCoordinatorState = false;
    try {
      Packet.PacketType type = Packet.getPacketType(json);
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("######## Coordinating " + type
                + (type.equals(Packet.PacketType.ACTIVE_COORDINATION)
                        ? (" PaxosType: " + PaxosPacket.getPaxosPacketType(json)) : ""));
      }
      switch (type) {
        // coordination packets internal to paxos
        case ACTIVE_COORDINATION:
          Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);
          paxosManager.handleIncomingPacket(json);
          break;
        // call propose
        case UPDATE: // updates need coordination
          UpdatePacket<NodeIDType> update = new UpdatePacket<NodeIDType>(json, unstringer);
          update.setNameServerID(nodeID);
          String paxosID = paxosManager.propose(update.getName(), update.toString());
          if (paxosID == null) {
            callHandleDecision = update;
            noCoordinatorState = true;
            GNS.getLogger().warning("Update no paxos state: " + update);
          }
          break;

        // call proposeStop
        case ACTIVE_REMOVE: // stop request for removing a name record
          if (Config.debuggingEnabled) {
            GNS.getLogger().info("*******Before proposing remove: " + request);
          }
          OldActiveSetStopPacket<NodeIDType> stopPacket1 = new OldActiveSetStopPacket<NodeIDType>(json, unstringer);
          paxosID = paxosManager.proposeStop(stopPacket1.getName(), stopPacket1.toString(), stopPacket1.getVersion());
          if (paxosID == null) {
            callHandleDecision = stopPacket1;
            noCoordinatorState = true;
          }
          if (Config.debuggingEnabled) {
            GNS.getLogger().info("*******Remove proposed: " + request);
          }
          break;
        case OLD_ACTIVE_STOP: // (sent by active replica) stop request on a group change
          OldActiveSetStopPacket<NodeIDType> stopPacket2 = new OldActiveSetStopPacket<NodeIDType>(json, unstringer);
          paxosID = paxosManager.proposeStop(stopPacket2.getName(), stopPacket2.toString(), stopPacket2.getVersion());
          if (paxosID == null) {
            callHandleDecision = stopPacket2;
            noCoordinatorState = true;
          }
          break;
        // call createPaxosInstance
        case ACTIVE_ADD:  // createPaxosInstance when name is added for the first time
          // calling handle decision before creating paxos instance to insert state for name in database.
          if (Config.debuggingEnabled) {
            GNS.getLogger().info("*******Before creating paxos instance: " + request);
          }
          callHandleDecisionWithRetry(request, false);
          AddRecordPacket<NodeIDType> recordPacket = new AddRecordPacket<NodeIDType>(json, unstringer);
          paxosManager.createPaxosInstance(recordPacket.getName(), (short) Config.FIRST_VERSION,
                  recordPacket.getActiveNameServers(), app);
          if (Config.debuggingEnabled) {
            GNS.getLogger().info("*******Added paxos instance:" + recordPacket.getName());
          }
          break;
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE: // (sent by active replica) createPaxosInstance after a group change
          // active replica has already put initial state for the name in DB. we only need to create paxos instance.
          NewActiveSetStartupPacket<NodeIDType> newActivePacket = new NewActiveSetStartupPacket<NodeIDType>(json, unstringer);
          paxosManager.createPaxosInstance(newActivePacket.getName(), newActivePacket.getNewActiveVersion(),
                  newActivePacket.getNewActiveNameServers(), app);
          break;

        // no coordination needed for these requests
        case DNS:
          DNSPacket<NodeIDType> dnsPacket = new DNSPacket<NodeIDType>(json, unstringer);
          String name = dnsPacket.getGuid();

          // Send the current set of active replicas for this name to the LNS to keep it updated of the
          // current replica set. This message is necessary for the case when the active replica set has
          // changed but the old and new replicas share some members (which is actually quite common).
          // Why is this necessary? Let's say closest name server to a LNS in the previous replica set was quite far, but
          // in the new replica set the closest name server is very near to LNS. If we do not inform the LNS of
          // current active replica set, it will continue sending requests to the far away name server.
          Set<NodeIDType> nodeIds = paxosManager.getPaxosNodeIDs(name);
          if (nodeIds != null) {
            RequestActivesPacket<NodeIDType> requestActives = new RequestActivesPacket<NodeIDType>(name, dnsPacket.getLnsAddress(), 0, nodeID);
            requestActives.setActiveNameServers(nodeIds);
            messenger.sendToAddress(dnsPacket.getLnsAddress(), requestActives.toJSONObject());
          }
          if (readCoordination && dnsPacket.isQuery()) {
            // Originally the responder field was used to communicate back to the client about which node responded to a query.
            // Now it appears someone is using it for another purpose, undocumented. This seems like a bad idea.
            // Get your own field! - Westy
            dnsPacket.setResponder(nodeID);
            paxosID = paxosManager.propose(dnsPacket.getGuid(), dnsPacket.toString());
            if (paxosID == null) {
              callHandleDecision = dnsPacket;
              noCoordinatorState = true;
            }
          } else {
            callHandleDecision = request;
          }
          break;
        case SELECT_REQUEST:
        case SELECT_RESPONSE:
        case UPDATE_CONFIRM:
        case ADD_CONFIRM:
        case REMOVE_CONFIRM:
          // Packets sent from replica controller
          callHandleDecision = request;

          break;
        default:
          GNS.getLogger().severe("Packet type not found in coordination: " + type);
          break;
      }
      if (callHandleDecision != null) {
        // FIXME: verify that NO_COORDINATOR_STATE_MARKER isn't needed
//        if (noCoordinatorState) {
//          callHandleDecision.put(Config.NO_COORDINATOR_STATE_MARKER, 0);
//        }
        callHandleDecisionWithRetry(callHandleDecision, false);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
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
            nodes, this);
    /* FIXME: This putInitialState may not happen atomically with paxos
     * instance creation. However, gigapaxos currently has no way to 
     * specify any initial state.
     */
    this.app.putInitialState(serviceName, epoch, state);
    return true;
  }

  @Override
  public void deleteReplicaGroup(String serviceName) {
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
