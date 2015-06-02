package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.newApp.packet.deprecated.OldActiveSetStopPacket;
import edu.umass.cs.gns.newApp.packet.RequestActivesPacket;
import edu.umass.cs.gns.newApp.packet.Packet;
import edu.umass.cs.gns.newApp.packet.deprecated.NewActiveSetStartupPacket;
import edu.umass.cs.gns.newApp.packet.DNSPacket;
import edu.umass.cs.gns.newApp.packet.UpdatePacket;
import edu.umass.cs.gns.newApp.packet.AddRecordPacket;
import edu.umass.cs.gns.gigapaxos.deprecated.AbstractPaxosManager;
import edu.umass.cs.gns.gigapaxos.deprecated.Replicable;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.PacketTypeStampAndSend;
import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplicaApp;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.nsdesign.replicaCoordination.ActiveReplicaCoordinator;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Set;

/**
 * Module for coordinating among active replicas of a name using paxos protocol.
 * This is the entry point for all messages into gnsReconfigurable module. Its main task
 * is to decide whether coordination is needed for a request. If yes, it proposes requests
 * to paxos for coordination. Otherwise, requests are forwarded to GNS for execution.
 *
 *
 * Created by abhigyan on 3/28/14.
 */
@Deprecated
public class GnsCoordinatorPaxos<NodeIDType> extends ActiveReplicaCoordinator {

  private static final long HANDLE_DECISION_RETRY_INTERVAL_MILLIS = 1000;

  private final NodeIDType nodeID;
  // this is the app object
  private final Replicable paxosInterface;

  private final AbstractPaxosManager<NodeIDType> paxosManager;

  // if true, reads are coordinated as well.
  private boolean readCoordination = false;

  private final InterfaceJSONNIOTransport<NodeIDType> nioTransport;
  private final InterfaceNodeConfig<NodeIDType> nodeConfig;

  public GnsCoordinatorPaxos(NodeIDType nodeID, InterfaceJSONNIOTransport<NodeIDType> nioServer, InterfaceNodeConfig<NodeIDType> nodeConfig,
          Replicable paxosInterface, PaxosConfig paxosConfig, boolean readCoordination) {
    this.nodeID = nodeID;
    this.nodeConfig = nodeConfig;
    this.readCoordination = readCoordination;
    this.nioTransport = nioServer;

    if (!Config.useOldPaxos) {
      GNS.getLogger().info("Using gigapaxos in GnsCoordinatorPaxos");
      this.paxosInterface = paxosInterface;
      // this doesn't do any good because somebody is calling createPaxosInstance with their own app
      //this.paxosInterface = new ReplicableTransition(paxosInterface);
      this.paxosManager = new edu.umass.cs.gns.gigapaxos.PaxosManager<NodeIDType>(nodeID,
              nodeConfig, new PacketTypeStampAndSend(nioServer, Packet.PacketType.ACTIVE_COORDINATION),
              this.paxosInterface, paxosConfig.getPaxosLogFolder());
//      this.paxosManager = new PaxosManagerTransition(new edu.umass.cs.gns.gigapaxos.PaxosManager<NodeIDType>(nodeID,
//              nodeConfig, new PacketTypeStampAndSend(nioServer, Packet.PacketType.ACTIVE_COORDINATION),
//              this.paxosInterface, paxosConfig));
    } else {
      GNS.getLogger().info("Using old Paxos (not gigapaxos) in GnsCoordinatorPaxos");
      this.paxosInterface = paxosInterface;
      this.paxosManager = new PaxosManager<NodeIDType>(nodeID, nodeConfig,
              new PacketTypeStampAndSend<NodeIDType>(nioServer, Packet.PacketType.ACTIVE_COORDINATION),
              this.paxosInterface, paxosConfig);
    }
  }

  /**
   * Handles coordination among replicas for a request. Returns -1 in case of error, 0 otherwise.
   * Error could happen if replicable app is not initialized, or paxos instance for this name does not exist.
   *
   * @param request
   * @return
   */
  @Override
  public int coordinateRequest(JSONObject request) {
    if (this.paxosInterface == null) {
      return -1; // replicable app not set
    }
    JSONObject callHandleDecision = null;
    boolean noCoordinatorState = false;
    try {
      Packet.PacketType type = Packet.getPacketType(request);
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("######## Coordinating " + type
                + (type.equals(Packet.PacketType.ACTIVE_COORDINATION)
                        ? (" PaxosType: " + PaxosPacket.getPaxosPacketType(request)) : ""));
      }
      switch (type) {
        // coordination packets internal to paxos
        case ACTIVE_COORDINATION:
          Packet.putPacketType(request, Packet.PacketType.PAXOS_PACKET);
          paxosManager.handleIncomingPacket(request);
          break;
        // call propose
        case UPDATE: // updates need coordination
          UpdatePacket<NodeIDType> update = new UpdatePacket<>(request, nodeConfig);
          update.setNameServerID(nodeID);
          String requestString;
          // Create a digest form of the update.
          if (Config.useRequestDigest) {
            // hack this for testing
            requestString = new String(((GnsReconfigurable) ((ActiveReplicaApp) paxosInterface).app).getDigester().createDigest(update.toString()));
          } else {
            requestString = update.toString();
          }
          String paxosID = paxosManager.propose(update.getName(), requestString);
          if (paxosID == null) {
            callHandleDecision = update.toJSONObject();
            noCoordinatorState = true;
            GNS.getLogger().warning("Update no paxos state: " + update);
          }
          break;

        // call proposeStop
        case ACTIVE_REMOVE: // stop request for removing a name record
          if (Config.debuggingEnabled) {
            GNS.getLogger().info("*******Before proposing remove: " + request);
          }
          OldActiveSetStopPacket<NodeIDType> stopPacket1 = new OldActiveSetStopPacket<NodeIDType>(request, nodeConfig);
          paxosID = paxosManager.proposeStop(stopPacket1.getName(), stopPacket1.toString(), stopPacket1.getVersion());
          if (paxosID == null) {
            callHandleDecision = stopPacket1.toJSONObject();
            noCoordinatorState = true;
          }
          if (Config.debuggingEnabled) {
            GNS.getLogger().info("*******Remove proposed: " + request);
          }
          break;
        case OLD_ACTIVE_STOP: // (sent by active replica) stop request on a group change
          OldActiveSetStopPacket<NodeIDType> stopPacket2 = new OldActiveSetStopPacket<NodeIDType>(request, nodeConfig);
          paxosID = paxosManager.proposeStop(stopPacket2.getName(), stopPacket2.toString(), stopPacket2.getVersion());
          if (paxosID == null) {
            callHandleDecision = stopPacket2.toJSONObject();
            noCoordinatorState = true;
          }
          break;
        // call createPaxosInstance
        case ACTIVE_ADD:  // createPaxosInstance when name is added for the first time
          // calling handle decision before creating paxos instance to insert state for name in database.
          if (Config.debuggingEnabled) {
            GNS.getLogger().info("*******Before creating paxos instance: " + request);
          }
          callHandleDecisionWithRetry(null, request.toString(), false);
          AddRecordPacket<NodeIDType> recordPacket = new AddRecordPacket<NodeIDType>(request, nodeConfig);
          paxosManager.createPaxosInstance(recordPacket.getName(), (short) Config.FIRST_VERSION, recordPacket.getActiveNameServers(), paxosInterface);
          if (Config.debuggingEnabled) {
            GNS.getLogger().info("*******Added paxos instance:" + recordPacket.getName());
          }
          break;
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE: // (sent by active replica) createPaxosInstance after a group change
          // active replica has already put initial state for the name in DB. we only need to create paxos instance.
          NewActiveSetStartupPacket<NodeIDType> newActivePacket = new NewActiveSetStartupPacket<NodeIDType>(request, nodeConfig);
          paxosManager.createPaxosInstance(newActivePacket.getName(), newActivePacket.getNewActiveVersion(),
                  newActivePacket.getNewActiveNameServers(), paxosInterface);
          break;

        // no coordination needed for these requests
        case DNS:
          DNSPacket<NodeIDType> dnsPacket = new DNSPacket<NodeIDType>(request, nodeConfig);
          String name = dnsPacket.getGuid();

          // Send the current set of active replicas for this name to the LNS to keep it updated of the
          // current replica set. This message is necessary for the case when the active replica set has
          // changed but the old and new replicas share some members (which is actually quite common).
          // Why is this necessary? Let's say closest name server to a LNS in the previous replica set was quite far, but
          // in the new replica set the closest name server is very near to LNS. If we do not inform the LNS of
          // current active replica set, it will continue sending requests to the far away name server.
          Set<NodeIDType> nodeIds = paxosManager.getPaxosNodeIDs(name);
          if (nodeIds != null) {
            RequestActivesPacket<NodeIDType> requestActives = new RequestActivesPacket<NodeIDType>(name, dnsPacket.getCCPAddress(), 0, nodeID);
            requestActives.setActiveNameServers(nodeIds);
            nioTransport.sendToAddress(dnsPacket.getCCPAddress(), requestActives.toJSONObject());
          }
          if (readCoordination && dnsPacket.isQuery()) {
            // Originally the responder field was used to communicate back to the client about which node responded to a query.
            // Now it appears someone is using it for another purpose, undocumented. This seems like a bad idea.
            // Get your own field! - Westy
            dnsPacket.setResponder(nodeID);
            paxosID = paxosManager.propose(dnsPacket.getGuid(), dnsPacket.toString());
            if (paxosID == null) {
              callHandleDecision = dnsPacket.toJSONObjectQuestion();
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
        if (noCoordinatorState) {
          callHandleDecision.put(Config.NO_COORDINATOR_STATE_MARKER, 0);
        }
        callHandleDecisionWithRetry(null, callHandleDecision.toString(), false);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return 0;
  }

  /**
   * Retries a request at period interval until successfully executed by application.
   */
  private void callHandleDecisionWithRetry(String name, String value, boolean doNotReplyToClient) {
    while (!paxosInterface.handleDecision(name, value, doNotReplyToClient)) {
      try {
        Thread.sleep(HANDLE_DECISION_RETRY_INTERVAL_MILLIS);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      GNS.getLogger().severe("Failed to execute decision. Retry. name = " + name + " value = " + value);
    }
  }

  @Override
  public void reset() {
    paxosManager.resetAll();
  }

  @Override
  public void shutdown() {
    // todo how to shutdown multipaxos's PaxosManager.
    if (paxosManager instanceof PaxosManager) {
      ((PaxosManager) paxosManager).shutdown();
    }
  }
}
