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
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;

public class RequestActives implements SchedulableProtocolTask<String, PacketType, String> {

  private final long RESTART_PERIOD = 1000;

  private final LNSRequestInfo lnsRequestInfo;
  private final RequestHandlerInterface handler;
  private final String key;
  private final List<InetSocketAddress> reconfigurators;
  private int requestCount = 0; // number of times we have requested

  public static final Logger log = Logger.getLogger(Reconfigurator.class.getName());

  public RequestActives(LNSRequestInfo lnsRequestInfo,
          RequestHandlerInterface handler) {

    this.lnsRequestInfo = lnsRequestInfo;
    this.handler = handler;
    reconfigurators = new ArrayList(handler.getNodeConfig().getReplicatedReconfigurators(lnsRequestInfo.getServiceName()));
    this.key = this.refreshKey();
    if (handler.isDebugMode()) {
      GNS.getLogger().info("~~~~~~~~~~~~~~~~~~~~~~~~ Request actives starting: " + key);
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
      log.info("~~~~~~~~~~~~~~~~~~~~~~~~" + this.refreshKey() + " / " + lnsRequestInfo.getServiceName() + " re-sending ");
    }
    return start();
  }

  private boolean amObviated() {
    if (handler.getActivesIfValid(lnsRequestInfo.getServiceName()) != null) {
      return true;
    } else if (requestCount >= reconfigurators.size()) {
      if (handler.isDebugMode()) {
        log.info("~~~~~~~~~~~~~~~~~~~~~~~~" + this.refreshKey() + " / " + lnsRequestInfo.getServiceName() + " No answer, using defaults");
      }
      // no answer so we stuff in the default choices and return
      handler.updateCacheEntry(lnsRequestInfo.getServiceName(),
              handler.getNodeConfig().getReplicatedActives(lnsRequestInfo.getServiceName()));
      return true;
    } else {
      return false;
    }
  }

  @Override
  public GenericMessagingTask[] start() {
    RequestActiveReplicas packet = new RequestActiveReplicas(handler.getNodeAddress(),
            lnsRequestInfo.getServiceName(), 0);
    
    int reconfigIndex = requestCount % reconfigurators.size();
    if (handler.isDebugMode()) {
      log.info("~~~~~~~~~~~~~~~~~~~~~~~~" + this.refreshKey() + " / " + lnsRequestInfo.getServiceName() 
              + " Sending to " + reconfigurators.get(reconfigIndex) 
              + " " + packet);
    }
    GenericMessagingTask mtasks[] = new GenericMessagingTask(reconfigurators.get(reconfigIndex), packet).toArray();
    requestCount++;
    return mtasks;
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
