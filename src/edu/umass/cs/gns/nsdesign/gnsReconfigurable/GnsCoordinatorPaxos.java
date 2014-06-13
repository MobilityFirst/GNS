package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.GNSNIOTransportInterface;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.PacketTypeStamper;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.paxos.AbstractPaxosManager;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.util.ConsistentHashing;
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
public class GnsCoordinatorPaxos extends ActiveReplicaCoordinator{

  private int nodeID;
  // this is the app object
  private Replicable paxosInterface;

  private AbstractPaxosManager paxosManager;

  // if true, reads are coordinated as well.
  private boolean readCoordination = false;

  private GNSNIOTransportInterface nioTransport;

  public GnsCoordinatorPaxos(int nodeID, GNSNIOTransportInterface nioServer, NodeConfig nodeConfig,
                             Replicable paxosInterface, PaxosConfig paxosConfig, boolean readCoordination) {
    this.nodeID = nodeID;
    this.paxosInterface = paxosInterface;
    this.readCoordination = readCoordination;
    this.nioTransport = nioServer;
    if (Config.useMultiPaxos) {
      assert false: "Not working yet. Known Issue: we need to fix packet demultiplexing";
      this.paxosManager = new edu.umass.cs.gns.replicaCoordination.multipaxos.PaxosManager(nodeID, nodeConfig,
      (GNSNIOTransport) nioServer, paxosInterface, paxosConfig);
    } else {
      this.paxosManager = new PaxosManager(nodeID, nodeConfig,
              new PacketTypeStamper(nioServer, Packet.PacketType.ACTIVE_COORDINATION), paxosInterface, paxosConfig);
    }
  }

  /**
   * Handles coordination among replicas for a request. Returns -1 in case of error, 0 otherwise.
   * Error could happen if replicable app is not initialized, or paxos instance for this name does not exist.
   */
  @Override
  public int coordinateRequest(JSONObject request) {
    if (this.paxosInterface == null) return -1; // replicable app not set
    JSONObject callHandleDecision = null;
    boolean noCoordinatorState = false;
    try {
      Packet.PacketType type = Packet.getPacketType(request);
      if (!type.equals(Packet.PacketType.ACTIVE_COORDINATION)) {
        if (Config.debugMode) GNS.getLogger().fine(" GNS recvd msg: " + request);
      }
      switch (type) {
        // coordination packets internal to paxos
        case ACTIVE_COORDINATION:
          paxosManager.handleIncomingPacket(request);
          break;

        // call propose
        case UPDATE: // updates need coordination
          UpdatePacket update = new UpdatePacket(request);
          update.setNameServerId(nodeID);
          String paxosID = paxosManager.propose(update.getName(), update.toString());
          if (paxosID == null) {
            callHandleDecision = update.toJSONObject();
            noCoordinatorState = true;
          }
          break;

        // call proposeStop
        case ACTIVE_REMOVE: // stop request for removing a name record
          OldActiveSetStopPacket stopPacket1 = new OldActiveSetStopPacket(request);
          paxosID = paxosManager.proposeStop(stopPacket1.getName(), stopPacket1.toString(), stopPacket1.getVersion());
          if (paxosID == null) {
            callHandleDecision = stopPacket1.toJSONObject();
            noCoordinatorState = true;
          }
          break;
        case OLD_ACTIVE_STOP: // (sent by active replica) stop request on a group change
          OldActiveSetStopPacket stopPacket2 = new OldActiveSetStopPacket(request);
          paxosID = paxosManager.proposeStop(stopPacket2.getName(), stopPacket2.toString(), stopPacket2.getVersion());
          if (paxosID == null) {
            callHandleDecision = stopPacket2.toJSONObject();
            noCoordinatorState = true;
          }
          break;
        // call createPaxosInstance
        case ACTIVE_ADD:  // createPaxosInstance when name is added for the first time
          // calling handle decision before creating paxos instance to insert state for name in database.
          paxosInterface.handleDecision(null, request.toString(), false);
          AddRecordPacket recordPacket = new AddRecordPacket(request);
          paxosManager.createPaxosInstance(recordPacket.getName(), (short)Config.FIRST_VERSION, recordPacket.getActiveNameSevers(), paxosInterface);
          if (Config.debugMode) GNS.getLogger().fine("Added paxos instance:" + recordPacket.getName());
          break;
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE: // (sent by active replica) createPaxosInstance after a group change
          // active replica has already put initial state for the name in DB. we only need to create paxos instance.
          NewActiveSetStartupPacket newActivePacket = new NewActiveSetStartupPacket(request);
          paxosManager.createPaxosInstance(newActivePacket.getName(), (short)newActivePacket.getNewActiveVersion(),
                  newActivePacket.getNewActiveNameServers(), paxosInterface);
          break;

        // no coordination needed for these requests
        case DNS:
          DNSPacket dnsPacket = new DNSPacket(request);
          String name = dnsPacket.getGuid();

          // Send the current set of active replicas for this name to the LNS to keep it updated of the
          // current replica set. This message is necessary for the case when the active replica set has
          // changed but the old and new replicas share some common members (which is actually quite common).
          // Why is this useful? Let's say closest name server to a LNS in the previous replica set was quite far, but
          // in the new replica set the closest name server is very near to LNS. If we do not inform the LNS of
          // current active replica set, it will continue sending requests to the far away name server.
          Set<Integer> nodeIds = paxosManager.getPaxosNodeIDs(name);
          if (nodeIds != null) {
            RequestActivesPacket requestActives = new RequestActivesPacket(name, dnsPacket.getLnsId(), 0, nodeID);
            requestActives.setActiveNameServers(nodeIds);
            nioTransport.sendToID(dnsPacket.getLnsId(), requestActives.toJSONObject());
          }
          if (readCoordination) {

            if (dnsPacket.isQuery()) {
              dnsPacket.setResponder(nodeID);
              paxosID = paxosManager.propose(dnsPacket.getGuid(), dnsPacket.toString());
              if (paxosID == null) {
                callHandleDecision = dnsPacket.toJSONObjectQuestion();
                noCoordinatorState = true;
              }
              break;
            }

          }
        case SELECT_REQUEST:
        case SELECT_RESPONSE:
        case CONFIRM_UPDATE:
        case CONFIRM_ADD:
        case CONFIRM_REMOVE:
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
        paxosInterface.handleDecision(null, callHandleDecision.toString(), false);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return 0;
  }

  @Override
  public void reset() {
    paxosManager.resetAll();
  }

}


