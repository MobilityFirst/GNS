package edu.umass.cs.gns.gnsApp.clientCommandProcessor;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.AddRemove;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.UpdateInfo;
import edu.umass.cs.gns.main.GNS;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gns.gnsApp.packet.AddRecordPacket;
import edu.umass.cs.gns.gnsApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.gnsApp.packet.RemoveRecordPacket;
import edu.umass.cs.gns.gnsApp.NSResponseCode;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import java.net.UnknownHostException;

import org.json.JSONException;

/**
 * Currently boring use of ProtocolTask for the Client Command Processor.
 * The handleEvent doesn't return any messaging tasks.
 *
 * @author Westy
 *
 * @param <NodeIDType>
 */
public class CCPProtocolTask<NodeIDType> implements
        ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

  private static final ReconfigurationPacket.PacketType[] types = {
    ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME,
    ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME, // ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS
  };

  private final String key;
  private final ClientRequestHandlerInterface handler;

  /**
   * Sets the handler.
   *
   * @param requestHandler
   */
  public CCPProtocolTask(ClientRequestHandlerInterface requestHandler) {
    this.handler = requestHandler;
    this.key = refreshKey();
  }

  @Override
  public String getKey() {
    return this.key;
  }

  @Override
  public GenericMessagingTask<NodeIDType, ?>[] start() {
    return null;
  }

  private String refreshKey() {
    return ((handler.getNodeAddress().getHostString() + (int) (Math.random() * Integer.MAX_VALUE)));
  }

  @Override
  public Set<ReconfigurationPacket.PacketType> getEventTypes() {
    Set<ReconfigurationPacket.PacketType> types = new HashSet<ReconfigurationPacket.PacketType>(
            Arrays.asList(CCPProtocolTask.types));
    return types;
  }

  @Override
  public GenericMessagingTask<NodeIDType, ?>[] handleEvent(
          ProtocolEvent<ReconfigurationPacket.PacketType, String> event,
          ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {

    ReconfigurationPacket.PacketType type = event.getType();
    ReconfigurationPacket packet = (ReconfigurationPacket) event.getMessage();
    GenericMessagingTask<NodeIDType, ?> mtask = null;
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

  private GenericMessagingTask<NodeIDType, ?> handleCreate(CreateServiceName packet) {
    Integer lnsRequestID = handler.removeCreateRequestNameToIDMapping(packet.getServiceName());
    if (lnsRequestID != null) {

      // Basically we gin up a confirmation packet for the original AddRecord packet and
      // send it back to the originator of the request.
      @SuppressWarnings("unchecked")
      UpdateInfo<String> info = (UpdateInfo<String>) handler.getRequestInfo(lnsRequestID);
      if (info != null) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("??????????????????????????? App created " + packet.getServiceName()
                  + " in " + (System.currentTimeMillis() - info.getStartTime()) + "ms");
        }
        @SuppressWarnings("unchecked")
        AddRecordPacket<String> originalPacket = (AddRecordPacket<String>) info.getUpdatePacket();
        ConfirmUpdatePacket<String> confirmPacket = new ConfirmUpdatePacket<String>(NSResponseCode.NO_ERROR, originalPacket);

        try {
          AddRemove.handlePacketConfirmAdd(confirmPacket.toJSONObject(), handler);
        } catch (JSONException | UnknownHostException e) {
          GNS.getLogger().severe("Unable to send create confirmation for " + packet.getServiceName() + ":" + e);
        }
      } else {
        GNS.getLogger().severe("Unable to find request info for create confirmation for " + packet.getServiceName());
      }
    } else {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Ignoring spurious create confirmation for " + packet.getServiceName());
      }
    }
    return null;
  }

  private GenericMessagingTask<NodeIDType, ?> handleDelete(DeleteServiceName packet) {
    if (!packet.isFailed()) { // it appears that these requests can fail now
      Integer lnsRequestID = handler.removeDeleteRequestNameToIDMapping(packet.getServiceName());
      if (lnsRequestID != null) {
        // Basically we gin up a confirmation packet for the original AddRecord packet and
        // send it back to the originator of the request.
        @SuppressWarnings("unchecked")
        UpdateInfo<String> info = (UpdateInfo<String>) handler.getRequestInfo(lnsRequestID);
        if (info != null) {
          if (handler.getParameters().isDebugMode()) {
            GNS.getLogger().info("??????????????????????????? App removed " + packet.getServiceName()
                    + "in " + (System.currentTimeMillis() - info.getStartTime()) + "ms");
          }
          @SuppressWarnings("unchecked")
          RemoveRecordPacket<String> originalPacket = (RemoveRecordPacket<String>) info.getUpdatePacket();
          ConfirmUpdatePacket<String> confirmPacket = new ConfirmUpdatePacket<String>(NSResponseCode.NO_ERROR, originalPacket);

          try {
            AddRemove.handlePacketConfirmRemove(confirmPacket.toJSONObject(), handler);
          } catch (JSONException | UnknownHostException e) {
            GNS.getLogger().severe("Unable to send remove confirmation for " + packet.getServiceName() + ":" + e);
          }
        } else {
          GNS.getLogger().severe("Unable to find request info for remove confirmation for " + packet.getServiceName());
        }
      } else {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("Ignoring spurious remove confirmation for " + packet.getServiceName());
        }
      }
    } else {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Remove request failed for " + packet.getServiceName() + "; will be resent.");
      }
    }
    return null;
  }

}
