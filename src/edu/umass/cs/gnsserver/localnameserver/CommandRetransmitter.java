
package edu.umass.cs.gnsserver.localnameserver;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


public class CommandRetransmitter implements SchedulableProtocolTask<InetSocketAddress, PacketType, String> {

  private final long RESTART_PERIOD = 10000;

  private final long requestId;
  private final RequestHandlerInterface handler;
  private final JSONObject json;
  private final String key;
  private final Set<InetSocketAddress> actives;
  private final Set<InetSocketAddress> activesAlreadyContacted = new HashSet<>();


  public static final Logger LOG = Logger.getLogger(Reconfigurator.class.getName());


  public CommandRetransmitter(long requestId, JSONObject json, Set<InetSocketAddress> actives,
          RequestHandlerInterface handler) {
    this.requestId = requestId;
    this.json = json;
    this.handler = handler;
    this.actives = actives;
    this.key = this.refreshKey();
    LOG.log(Level.FINE, "CommandSender starting: {0}", key);
  }


  @Override
  public GenericMessagingTask<InetSocketAddress, ?>[] restart() {
    if (this.amObviated()) {
      ProtocolExecutor.cancel(this);
    }
    LOG.log(Level.FINE, "{0} re-sending ", this.refreshKey());
    return start();
  }

  private boolean amObviated() {
    if ((handler.getRequestInfo(requestId)) == null) {
      return true;
    } else if (activesAlreadyContacted.size() == actives.size()) {
      LOG.log(Level.FINE,
              "~~~~~~~~~~~~~~~~~~~~~~~~{0} No answer after trying all actives.", this.refreshKey());
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
    LOG.log(Level.FINE, 
            "{0} Sending to {1} {2}", new Object[]{this.refreshKey(), address, json});
    activesAlreadyContacted.add(address);
    GenericMessagingTask<InetSocketAddress, ?> mtasks[] = new GenericMessagingTask<>(address, json).toArray();
    return mtasks;
  }

  private String refreshKey() {
    return Long.toString(requestId);
  }


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
          ProtocolEvent<PacketType, String> event,
          ProtocolTask<InetSocketAddress, PacketType, String>[] ptasks) {
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
