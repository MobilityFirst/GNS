package edu.umass.cs.gns.newApp.localNameServer;

import edu.umass.cs.gns.main.GNS;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;

/**
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
    GNS.getLogger().info("App created " + packet.getServiceName());
    return null;
    //return new GenericMessagingTask(null, packet);
  }

  private GenericMessagingTask handleDelete(DeleteServiceName packet) {
    GNS.getLogger().info("App deleted " + packet.getServiceName());
    return null;
    //return new GenericMessagingTask(null, packet);
  }

}
