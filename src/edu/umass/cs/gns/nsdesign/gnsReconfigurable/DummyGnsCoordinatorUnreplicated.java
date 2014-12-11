package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinator for dummy GNS app, when the app is run in the un-replicated mode. This class only exists for testing.
 *
 * This coordinator informs the dummy app, whether it is a valid active replica or not.
 *
 * Created by abhigyan on 4/8/14.
 * @param <NodeIDType>
 */
public class DummyGnsCoordinatorUnreplicated<NodeIDType> extends ActiveReplicaCoordinator {

  private final NodeIDType nodeID;
  private final Replicable replicable;
  private final ConcurrentHashMap<String, Short>  nameAndGroupVersion = new ConcurrentHashMap<String, Short>();
  private final GNSNodeConfig<NodeIDType> gnsNodeConfig;

  public DummyGnsCoordinatorUnreplicated(NodeIDType nodeID, GNSNodeConfig<NodeIDType> gnsNodeConfig, Replicable replicable) {
    this.nodeID = nodeID;
    this.gnsNodeConfig = gnsNodeConfig;
    this.replicable = replicable;
  }

  /**
   * Handles coordination among replicas for a request. Returns -1 in case of error, 0 otherwise.
   * Error could happen if replicable app is not initialized, or paxos instance for this name does not exist.
   */
  @Override
  public int coordinateRequest(JSONObject request) {
    GNS.getLogger().fine(" Recvd request for coordination: " + request);
    if (this.replicable == null) return -1; // replicable app not set
    boolean noCoordinatorState = false;
    try {
      Packet.PacketType type = Packet.getPacketType(request);
      switch (type) {
        // no coordination needed for any request.

        case UPDATE: // set a field in update packet because LNS may not set this field correctly.
          UpdatePacket update = new UpdatePacket<NodeIDType>(request, gnsNodeConfig);
          update.setNameServerID(nodeID);
          request = update.toJSONObject();
          if (!nameAndGroupVersion.contains(update.getName())) {
            noCoordinatorState = true;
          }
          break;
        case ACTIVE_REMOVE: // stop request for removing a name record
        case OLD_ACTIVE_STOP: // (sent by active replica) stop request on a group change
          OldActiveSetStopPacket stopPacket1 = new OldActiveSetStopPacket<NodeIDType>(request);
          Short version = nameAndGroupVersion.get(stopPacket1.getName());
          if (version == null || version != stopPacket1.getVersion()) {
            noCoordinatorState = true;
          } else {
            nameAndGroupVersion.remove(stopPacket1.getName());
          }
          break;
        case ACTIVE_ADD:  // createPaxosInstance when name is added for the first time
          AddRecordPacket recordPacket = new AddRecordPacket<NodeIDType>(request, gnsNodeConfig);
          nameAndGroupVersion.put(recordPacket.getName(), Config.FIRST_VERSION);
          break;
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:
          NewActiveSetStartupPacket newActivePacket = new NewActiveSetStartupPacket<NodeIDType>(request, gnsNodeConfig);
          nameAndGroupVersion.put(newActivePacket.getName(), newActivePacket.getNewActiveVersion());
          request = null;
          break;
        case DNS:
          DNSPacket dnsPacket = new DNSPacket<NodeIDType>(request, gnsNodeConfig);
          if (!nameAndGroupVersion.contains(dnsPacket.getGuid())) {
            noCoordinatorState = true;
          }
          break;
        case SELECT_REQUEST:
        case SELECT_RESPONSE:
        case CONFIRM_UPDATE:
        case CONFIRM_ADD:
        case CONFIRM_REMOVE:
          throw new UnsupportedOperationException();
        case ACTIVE_COORDINATION:
          // we do not expect any coordination packets
          throw new UnsupportedOperationException();
        default:
          GNS.getLogger().severe("Packet type not found in coordination: " + type);
          break;
      }
      if (request != null) { // 'request'  is set to null when calling handle decision is not necessary
        if (noCoordinatorState) {
          request.put(Config.NO_COORDINATOR_STATE_MARKER, 0);
        }
        GNS.getLogger().fine(" Final coordination decision State: " + noCoordinatorState + "\tFinal request: " + request);
        replicable.handleDecision(null, request.toString(), false);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return 0;
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown() {
    // nothing to do here currently, may need updating if we create a thread inside this module
  }
}
