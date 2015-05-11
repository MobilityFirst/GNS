package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.gns.reconfiguration.Reconfigurator;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RequestActiveReplicas;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONException;

public class RequestActives implements SchedulableProtocolTask {

  private final long RESTART_PERIOD = 1000;

  private final LNSRequestInfo lnsRequestInfo;
  private final RequestHandlerInterface handler;
  private final String key;
  private final List<InetSocketAddress> reconfigurators;

  public static final Logger log = Logger.getLogger(Reconfigurator.class.getName());

  public RequestActives(LNSRequestInfo lnsRequestInfo,
          RequestHandlerInterface handler) {

    this.lnsRequestInfo = lnsRequestInfo;
    this.handler = handler;
    reconfigurators = new ArrayList(handler.getNodeConfig().getReplicatedReconfigurators(lnsRequestInfo.getServiceName()));
    this.key = this.refreshKey();
    if (handler.isDebugMode()) {
      GNS.getLogger().info("Request actives starting: " + key);
    }
  }
  
  

  @Override
  public GenericMessagingTask[] restart() {
    if (this.amObviated()) {
      try {
        handler.sendToClosestServer(handler.getActivesIfValid(lnsRequestInfo.getServiceName()),
                lnsRequestInfo.getCommandPacket().toJSONObject());
      } catch (IOException | JSONException e) {
        log.severe(this.refreshKey() + " unable to send command packet " + e);
      }
      ProtocolExecutor.cancel(this);
    }
    if (handler.isDebugMode()) {
      log.info(this.refreshKey() + " re-sending "
              + lnsRequestInfo.getServiceName() + " " + lnsRequestInfo.getLNSReqID());
    }
    return start();
  }

  private boolean amObviated() {
    return handler.getActivesIfValid(lnsRequestInfo.getServiceName()) != null;
  }

  @Override
  public GenericMessagingTask[] start() {
    RequestActiveReplicas packet = new RequestActiveReplicas(handler.getNodeAddress(),
            lnsRequestInfo.getServiceName(), 0);
    return new GenericMessagingTask(reconfigurators.get(0), packet).toArray();
  }

  @Override
  public String refreshKey() {
    return Integer.toString(lnsRequestInfo.getLNSReqID());
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
  public GenericMessagingTask[] handleEvent(
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
