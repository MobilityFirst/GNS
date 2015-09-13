package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Sends create and delete requests to a reconfigurator with retransmission.
 *
 * @author westy
 */
public class SendCreateDelete implements SchedulableProtocolTask<InetSocketAddress, PacketType, String> {

  private final long RESTART_PERIOD = 1000;

  private final LNSRequestInfo lnsRequestInfo;
  private final RequestHandlerInterface handler;
  private final String key;
  private final List<InetSocketAddress> reconfigurators;
  private int requestCount = 0; // number of times we have requested

  /**
   * The logger.
   */
  public static final Logger log = Logger.getLogger(SendCreateDelete.class.getName());

  /**
   * Create a SendCreateDelete instance.
   *
   * @param lnsRequestInfo
   * @param handler
   */
  public SendCreateDelete(LNSRequestInfo lnsRequestInfo,
          RequestHandlerInterface handler) {

    this.lnsRequestInfo = lnsRequestInfo;
    this.handler = handler;
    reconfigurators = new ArrayList<InetSocketAddress>(handler.getNodeConfig().getReplicatedReconfigurators(lnsRequestInfo.getServiceName()));
    this.key = this.refreshKey();
    if (handler.isDebugMode()) {
      GNS.getLogger().info("~~~~~~~~~~~~~~~~~~~~~~~~ Request actives starting: " + key);
    }
  }

  @Override
  public GenericMessagingTask<InetSocketAddress, ?>[] restart() {
    if (this.amObviated()) {
      ProtocolExecutor.cancel(this);
    }
    if (handler.isDebugMode()) {
      log.info("~~~~~~~~~~~~~~~~~~~~~~~~" + this.refreshKey() + " re-sending ");
    }
    return start();
  }

  private boolean amObviated() {
    if (handler.getActivesIfValid(lnsRequestInfo.getServiceName()) != null) {
      return true;
    } else if (requestCount >= reconfigurators.size()) {
      if (handler.isDebugMode()) {
        log.info("~~~~~~~~~~~~~~~~~~~~~~~~" + this.refreshKey() + " No answer, using defaults");
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public GenericMessagingTask<InetSocketAddress, ?>[] start() {
    RequestActiveReplicas packet = new RequestActiveReplicas(handler.getNodeAddress(),
            lnsRequestInfo.getServiceName(), 0);

    int reconfigIndex = requestCount % reconfigurators.size();
    if (handler.isDebugMode()) {
      log.info("~~~~~~~~~~~~~~~~~~~~~~~~" + this.refreshKey()
              + " Sending to " + reconfigurators.get(reconfigIndex)
              + " " + packet);
    }
    GenericMessagingTask<InetSocketAddress, ?> mtasks[]
            = new GenericMessagingTask<>(reconfigurators.get(reconfigIndex), packet).toArray();
    requestCount++;
    return mtasks;
  }

  private String refreshKey() {
    return lnsRequestInfo.getServiceName() + " | " + Integer.toString(lnsRequestInfo.getLNSReqID());
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
          ProtocolEvent event,
          ProtocolTask[] ptasks) {
    return null;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " " + lnsRequestInfo.getServiceName() + " " + lnsRequestInfo.getLNSReqID();
  }

  @Override
  public long getPeriod() {
    return RESTART_PERIOD;
  }
}
