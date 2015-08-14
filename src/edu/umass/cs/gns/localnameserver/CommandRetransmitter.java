package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;

import java.net.InetSocketAddress;
import org.json.JSONObject;

public class CommandRetransmitter implements SchedulableProtocolTask<InetSocketAddress, PacketType, String> {

  private final long RESTART_PERIOD = 10000;

  private final int requestId;
  private final RequestHandlerInterface handler;
  private final JSONObject json;
  private final String key;
  private final Set<InetSocketAddress> actives;
  private final Set<InetSocketAddress> activesAlreadyContacted = new HashSet<>();

  public static final Logger log = Logger.getLogger(Reconfigurator.class.getName());

  public CommandRetransmitter(int requestId, JSONObject json, Set<InetSocketAddress> actives, 
          RequestHandlerInterface handler) {
    this.requestId = requestId;
    this.json = json;
    this.handler = handler;
    this.actives = actives;
    this.key = this.refreshKey();
    if (handler.isDebugMode()) {
      GNS.getLogger().fine("CommandSender starting: " + key);
    }
  }

  @Override
  public GenericMessagingTask<InetSocketAddress, ?>[] restart() {
    if (this.amObviated()) {
      ProtocolExecutor.cancel(this);
    }
    if (handler.isDebugMode()) {
      GNS.getLogger().info(this.refreshKey() + " re-sending ");
    }
    return start();
  }

  private boolean amObviated() {
    if ((handler.getRequestInfo(requestId)) == null) {
      return true;
    } else if (activesAlreadyContacted.size() == actives.size()) {
      if (handler.isDebugMode()) {
        GNS.getLogger().info("~~~~~~~~~~~~~~~~~~~~~~~~" + this.refreshKey()
                + " No answer after trying all actives.");
      }
      // should we return an NACK here or just let the client timeout?
      return true;
    } else {
      return false;
    }
  }

  @Override
  public GenericMessagingTask<InetSocketAddress, ?>[] start() {
    InetSocketAddress address = handler.getClosestReplica(actives, activesAlreadyContacted);
    // Remove these so the stamper will put new ones in so the packet will find it's way back here.
    json.remove(MessageNIOTransport.SNDR_IP_FIELD);
    json.remove(MessageNIOTransport.SNDR_PORT_FIELD);
    if (handler.isDebugMode()) {
      GNS.getLogger().info(this.refreshKey()
              + " Sending to " + address
              + " " + json);
    }
    activesAlreadyContacted.add(address);
    GenericMessagingTask<InetSocketAddress, ?> mtasks[] = new GenericMessagingTask<>(address, json).toArray();
    return mtasks;
  }

  //@Override
  public String refreshKey() {
    return Integer.toString(requestId);
  }

  // empty as task does not expect any events and will be explicitly removed
  public static final ReconfigurationPacket.PacketType[] types = {};

  @Override
  public Set<PacketType> getEventTypes() {
    return new HashSet<>();
  }

  @Override
  public String getKey() {
    return this.key;
  }

  @Override
  public GenericMessagingTask<InetSocketAddress, ?>[] handleEvent(
          ProtocolEvent event,
          ProtocolTask[] ptasks) {
    return null;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " " + requestId;
  }

  @Override
  public long getPeriod() {
    return RESTART_PERIOD;
  }
}
