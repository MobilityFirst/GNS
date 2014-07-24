package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.ping.PingServer;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * WARNING: DO NOT USE THIS CLASS!!! It is guaranteed to return fake and incorrect values of name records.
 *
 * It is an app resembling GnsReconfigurable.java which sends confirmation to clients as required by GNS
 * except that it does not use a database to store records and returns fake and incorrect values of name records.
 * It sends only one type of error message, invalid active error, which it sends when the gns coordinator
 * informs that it not a valid replica of this name.
 *
 * Created by abhigyan on 5/19/14.
 */
public class DummyGnsReconfigurable implements GnsReconfigurableInterface {

  /*** ID of this node */
  private final int nodeID;

  /*** nio server */
  private final InterfaceJSONNIOTransport nioServer;

  /** Configuration for all nodes in GNS **/
  private final GNSNodeConfig gnsNodeConfig;

  /**
   * Pings all nodes periodically and updates ping latencies to all name servers in
   * {@link edu.umass.cs.gns.nsdesign.GNSNodeConfig}
   */
  private PingManager pingManager;

  public DummyGnsReconfigurable(int nodeID, GNSNodeConfig gnsNodeConfig,
                           InterfaceJSONNIOTransport nioServer) {
    GNS.getLogger().info("Starting DUMMY gns .... NodeID: " + nodeID);
    this.nodeID = nodeID;

    this.gnsNodeConfig = gnsNodeConfig;

    this.nioServer = nioServer;

    if (!Config.emulatePingLatencies) {
      // when emulating ping latencies we do not
      PingServer.startServerThread(nodeID, gnsNodeConfig);
      this.pingManager = new PingManager(nodeID, gnsNodeConfig);
      this.pingManager.startPinging();
    }
  }

  @Override
  public boolean stopVersion(String name, short version) {
    return true;
  }

  @Override
  public String getFinalState(String name, short version) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putInitialState(String name, short version, String state) {
    // haha do nothing
  }

  @Override
  public int deleteFinalState(String name, short version) {
    return 0;
  }

  @Override
  public String getState(String name) {
    return fakeState;
  }

  @Override
  public boolean updateState(String name, String state) {
    // haha do nothing
    return true;
  }

  @Override
  public boolean handleDecision(String name, String value, boolean recovery) {
    try {
      JSONObject json = new JSONObject(value);
      boolean noCoordinationState = json.has(Config.NO_COORDINATOR_STATE_MARKER);
      Packet.PacketType packetType = Packet.getPacketType(json);
      switch (packetType) {
        case DNS:
          executeLookupLocal(new DNSPacket(json), noCoordinationState);
          break;
        case UPDATE:
          executeUpdateLocal(new UpdatePacket(json),noCoordinationState);
          break;
        case SELECT_REQUEST:
          throw new UnsupportedOperationException();
        case SELECT_RESPONSE:
          throw new UnsupportedOperationException();
        /**
         * Packets sent from replica controller *
         */
        case ACTIVE_ADD: // sent when new name is added to GNS
          if (Config.debugMode) GNS.getLogger().fine("Add received at GNS: " + json);
          // ha ha do nothing
          break;
        case ACTIVE_REMOVE: // sent when a name is to be removed from GNS
          executeRemoveLocal(new OldActiveSetStopPacket(json));
          break;
        // NEW CODE TO HANDLE CONFIRMATIONS COMING BACK FROM AN LNS
        case CONFIRM_UPDATE:
        case CONFIRM_ADD:
        case CONFIRM_REMOVE:
          throw new UnsupportedOperationException();
        default:
          GNS.getLogger().severe(" ERROR: Packet type not found: " + json);
          break;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }

  private void executeRemoveLocal(OldActiveSetStopPacket oldActiveStopPacket) throws JSONException, IOException {
    if (oldActiveStopPacket.getActiveReceiver() == nodeID) {
      // the active node who received this node, sends confirmation to primary
      // confirm to primary
      oldActiveStopPacket.changePacketTypeToActiveRemoved();
      nioServer.sendToID(oldActiveStopPacket.getPrimarySender(), oldActiveStopPacket.toJSONObject());
      if (Config.debugMode) GNS.getLogger().fine("Active removed: Name Record updated. Sent confirmation to replica " +
              "controller. Packet = " + oldActiveStopPacket);
    } else {
      // other nodes do nothing.
      GNS.getLogger().info("Active removed: Name Record updated. OldVersion = " + oldActiveStopPacket.getVersion());
    }
  }

  private void executeUpdateLocal(UpdatePacket updatePacket, boolean noCoordinationState) throws JSONException, IOException {
    JSONObject returnJson = null;
    if (noCoordinationState) {
      if (Config.debugMode) GNS.getLogger().fine("Sending invalid active error to client: " + updatePacket);
      ConfirmUpdatePacket failConfirmPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER);
      returnJson = failConfirmPacket.toJSONObject();
    } else {
      if (updatePacket.getNameServerId() == nodeID) {
        ConfirmUpdatePacket confirmPacket = new ConfirmUpdatePacket(Packet.PacketType.CONFIRM_UPDATE,
                updatePacket.getSourceId(), updatePacket.getRequestID(), updatePacket.getLNSRequestID(),
                NSResponseCode.NO_ERROR);
        if (Config.debugMode)
          GNS.getLogger().fine("NS Sent confirmation to LNS. Sent packet: " + confirmPacket.toJSONObject());
        returnJson = confirmPacket.toJSONObject();
      }
    }
    if (returnJson!=null) {
      nioServer.sendToID(updatePacket.getLocalNameServerId(), returnJson);
    }
  }

  private void executeLookupLocal(DNSPacket dnsPacket, boolean noCoordinatorState) throws JSONException, IOException {
    if (noCoordinatorState) {
      if (Config.debugMode) GNS.getLogger().fine("Sending invalid active error to client: " + dnsPacket);
      dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER);
      dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
    } else {
      if (Config.debugMode) GNS.getLogger().fine("Sending correct lookup response to client: " + dnsPacket);
      dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
      dnsPacket.setResponder(nodeID);
      dnsPacket.getHeader().setResponseCode(NSResponseCode.NO_ERROR);
      dnsPacket.setTTL(0);
      dnsPacket.setSingleReturnValue(fakeResultValue);
    }
    nioServer.sendToID(dnsPacket.getLnsId(), dnsPacket.toJSONObject());
  }

  private static ResultValue fakeResultValue;

  private static String fakeState;

  static {
    ValuesMap valuesMap = new ValuesMap();
    fakeResultValue = new ResultValue();
    fakeResultValue.add("pqrst");
    valuesMap.putAsArray(NameRecordKey.EdgeRecord.getName(), fakeResultValue);
    fakeState = new TransferableNameRecordState(valuesMap, 0).toString();
  }

  @Override
  public int getNodeID() {
    return nodeID;
  }

  @Override
  public BasicRecordMap getDB() {
    throw new UnsupportedOperationException();
  }

  @Override
  public GNSNodeConfig getGNSNodeConfig() {
    throw new UnsupportedOperationException();
  }

  @Override
  public InterfaceJSONNIOTransport getNioServer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PingManager getPingManager() {
    throw new UnsupportedOperationException();
  }
}
