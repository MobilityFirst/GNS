package edu.umass.cs.gns.newApp.localNameServer;

import edu.umass.cs.gns.localnameserver.AddRemove;
import edu.umass.cs.gns.localnameserver.UpdateInfo;
import edu.umass.cs.gns.main.GNS;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.util.NSResponseCode;
import java.net.UnknownHostException;
import org.json.JSONException;

/**
 * Currently boring use of ProtocolTask for the Local Name Server.
 * The handleEvent doesn't return any messaging tasks.
 * 
 * @author Westy
 *
 * @param <NodeIDType>
 */
public class LNSProtocolTask<NodeIDType> implements
        ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

  private static final ReconfigurationPacket.PacketType[] types = {
    ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME,
    ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME,};

  private String key = null;
  private final EnhancedClientRequestHandlerInterface requestHandler;

  public LNSProtocolTask(EnhancedClientRequestHandlerInterface<NodeIDType> requestHandler) {
    this.requestHandler = requestHandler;
  }

  @Override
  public String getKey() {
    return this.key;
  }

  @Override
  public GenericMessagingTask<NodeIDType, ?>[] start() {
    return null;
  }

  @Override
  public String refreshKey() {
    return (this.key = (requestHandler.getNodeAddress().getHostString() + (int) (Math.random() * Integer.MAX_VALUE)));
  }

  @Override
  public Set<ReconfigurationPacket.PacketType> getEventTypes() {
    Set<ReconfigurationPacket.PacketType> types = new HashSet<ReconfigurationPacket.PacketType>(
            Arrays.asList(LNSProtocolTask.types));
    return types;
  }

  @Override
  public GenericMessagingTask<NodeIDType, ?>[] handleEvent(
          ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
          ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {

    ReconfigurationPacket.PacketType type = event.getType();
    ReconfigurationPacket packet = (ReconfigurationPacket) event.getMessage();
    GenericMessagingTask mtask = null;
    switch (type) {
      case CREATE_SERVICE_NAME:
        mtask = handleCreate((CreateServiceName) packet);
        break;
      case DELETE_SERVICE_NAME:
        mtask = handleDelete((DeleteServiceName) packet);
        break;
      default:
        throw new RuntimeException("Unrecognizable message");
    }
    return mtask != null ? mtask.toArray() : null;
  }

  private GenericMessagingTask handleCreate(CreateServiceName packet) {
    Integer lnsRequestID = requestHandler.getCreateMapping(packet.getServiceName());
    if (lnsRequestID != null) {
      GNS.getLogger().info("App created " + packet.getServiceName());
      // Basically we gin up a confirmation packet for the original AddRecord packet and
      // send it back to the originator of the request.
      UpdateInfo info = (UpdateInfo) requestHandler.getRequestInfo(lnsRequestID);
      if (info != null) {
        AddRecordPacket originalPacket = (AddRecordPacket) info.getUpdatePacket();
        ConfirmUpdatePacket confirmPacket = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, originalPacket);

        try {
          AddRemove.handlePacketConfirmAdd(confirmPacket.toJSONObject(), requestHandler);
        } catch (JSONException | UnknownHostException e) {
          GNS.getLogger().severe("Unable to send create confirmation for " + packet.getServiceName() + ":" + e);
        }
      } else {
        GNS.getLogger().severe("Unable to find request info for create confirmation for " + packet.getServiceName());
      }
    } else {
      if (requestHandler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Ignoring spurious create confirmation for " + packet.getServiceName());
      }
    }
    return null;
  }

  private GenericMessagingTask handleDelete(DeleteServiceName packet) {
    Integer lnsRequestID = requestHandler.getRemoveMapping(packet.getServiceName());
    if (lnsRequestID != null) {
      GNS.getLogger().info("App removed " + packet.getServiceName());
      // Basically we gin up a confirmation packet for the original AddRecord packet and
      // send it back to the originator of the request.
      UpdateInfo info = (UpdateInfo) requestHandler.getRequestInfo(lnsRequestID);
      if (info != null) {
        RemoveRecordPacket originalPacket = (RemoveRecordPacket) info.getUpdatePacket();
        ConfirmUpdatePacket confirmPacket = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, originalPacket);

        try {
          AddRemove.handlePacketConfirmRemove(confirmPacket.toJSONObject(), requestHandler);
        } catch (JSONException | UnknownHostException e) {
          GNS.getLogger().severe("Unable to send remove confirmation for " + packet.getServiceName() + ":" + e);
        }
      } else {
        GNS.getLogger().severe("Unable to find request info for remove confirmation for " + packet.getServiceName());
      }
    } else {
      if (requestHandler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Ignoring spurious remove confirmation for " + packet.getServiceName());
      }
    }
    return null;
  }

}
