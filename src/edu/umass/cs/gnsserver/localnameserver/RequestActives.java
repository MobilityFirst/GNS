/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.localnameserver;

import edu.umass.cs.gnscommon.GNSCommandProtocol;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.reconfiguration.ActiveReplica;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.utils.Config;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import java.util.logging.Level;
import org.json.JSONException;

/**
 * Handles the requesting of active replicas from a reconfigurator.
 *
 * @author westy
 */
public class RequestActives implements SchedulableProtocolTask<InetSocketAddress, PacketType, String> {

  private final long RESTART_PERIOD = 1000;

  private final LNSRequestInfo lnsRequestInfo;
  private final RequestHandlerInterface handler;
  private final String key;
  private final List<InetSocketAddress> reconfigurators;
  private int requestCount = 0; // number of times we have requested

  /**
   * The logger.
   */
  public static final Logger LOG = Logger.getLogger(RequestActives.class.getName());

  /**
   * Creates a RequestActives instance.
   *
   * @param lnsRequestInfo
   * @param handler
   */
  public RequestActives(LNSRequestInfo lnsRequestInfo,
          RequestHandlerInterface handler) {

    this.lnsRequestInfo = lnsRequestInfo;
    this.handler = handler;
    reconfigurators = new ArrayList<>(handler.getNodeConfig().getReplicatedReconfigurators(lnsRequestInfo.getServiceName()));
    this.key = this.refreshKey();
    LOG.log(Level.FINE, "~~~~~~~~~~~~~~~~~~~~~~~~ Request actives starting: {0}", key);
  }

  @Override
  public GenericMessagingTask<InetSocketAddress, ?>[] restart() {
    if (this.amObviated()) {
      try {
        Set<InetSocketAddress> actives = handler.getActivesIfValid(lnsRequestInfo.getServiceName());
        // HACK - the cache entry might not be correct for all operations so destroy it
        // This code is going away soon anyway... don't sweat it.
        handler.invalidateCacheEntry(lnsRequestInfo.getServiceName());

        // got our actives and they're in the cache so now send out the command
        if (LNSPacketDemultiplexer.disableCommandRetransmitter) {
          handler.sendToClosestReplica(actives,
                  lnsRequestInfo.getCommandPacket().toJSONObject());
        } else {
          handler.getProtocolExecutor().schedule(new CommandRetransmitter(lnsRequestInfo.getLNSReqID(),
                  lnsRequestInfo.getCommandPacket().toJSONObject(), actives, handler));
        }
      } catch (JSONException | IOException e) {
        LOG.log(Level.SEVERE, "{0} unable to send command packet {1}", new Object[]{this.refreshKey(), e});
      }
      ProtocolExecutor.cancel(this);
    }
    LOG.log(Level.FINE, "~~~~~~~~~~~~~~~~~~~~~~~~{0} re-sending ", this.refreshKey());
    return start();
  }

  private boolean amObviated() {
    if (handler.getActivesIfValid(lnsRequestInfo.getServiceName()) != null) {
      return true;
    } else if (requestCount >= reconfigurators.size()) {
      // arun
      LOG.log(Level.WARNING, LocalNameServer.LNS_BAD_HACKY + "lnsRequestInfo = {0}", this.lnsRequestInfo);
      Set<InetSocketAddress> hackyActives = handler.getReplicatedActives(lnsRequestInfo.getServiceName());
      LOG.log(Level.FINE,
              "~~~~~~~~~~~~~~~~~~~~~~~~{0} No answer, using defaults for {1} :{2}",
              new Object[]{this.refreshKey(), lnsRequestInfo.getServiceName(), hackyActives});
      // no answer so we stuff in the default choices and return
      handler.updateCacheEntry(lnsRequestInfo.getServiceName(),
              hackyActives);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public GenericMessagingTask<InetSocketAddress, ?>[] start() {
    RequestActiveReplicas packet = new RequestActiveReplicas(handler.getNodeAddress(),
            lnsRequestInfo.getCommandType().isCreateDelete()
            //GNSCommandProtocol.CREATE_DELETE_COMMANDS.contains(lnsRequestInfo.getCommandName())
            ? Config.getGlobalString(RC.SPECIAL_NAME)
            : lnsRequestInfo.getServiceName(), 0);

    int reconfigIndex = requestCount % reconfigurators.size();
    LOG.log(Level.FINE, 
            "~~~~~~~~~~~~~~~~~~~~~~~~{0} Sending to {1} {2}", 
            new Object[]{this.refreshKey(), reconfigurators.get(reconfigIndex), packet});
    InetSocketAddress reconfiguratorAddress = reconfigurators.get(reconfigIndex);
    if (!LocalNameServerOptions.disableSSL) {
      // Use the client facing port for Server Auth
      reconfiguratorAddress = new InetSocketAddress(reconfiguratorAddress.getAddress(),
              ActiveReplica.getClientFacingPort(reconfiguratorAddress.getPort()));
    }
    GenericMessagingTask<InetSocketAddress, ?> mtasks[] = new GenericMessagingTask<>(reconfiguratorAddress, packet).toArray();
    requestCount++;
    return mtasks;
  }

  private String refreshKey() {
    return lnsRequestInfo.getServiceName() + " | " + Long.toString(lnsRequestInfo.getLNSReqID());
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
