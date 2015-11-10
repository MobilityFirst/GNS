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


import edu.umass.cs.gnsserver.main.GNS;
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
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;

import java.net.InetSocketAddress;
import org.json.JSONObject;

/**
 * Handles resending of packets to active replicas.
 * 
 * @author westy
 */
public class CommandRetransmitter implements SchedulableProtocolTask<InetSocketAddress, PacketType, String> {

  private final long RESTART_PERIOD = 10000;

  private final int requestId;
  private final RequestHandlerInterface handler;
  private final JSONObject json;
  private final String key;
  private final Set<InetSocketAddress> actives;
  private final Set<InetSocketAddress> activesAlreadyContacted = new HashSet<>();

  /**
   * The logger.
   */
  public static final Logger log = Logger.getLogger(Reconfigurator.class.getName());

  /**
   * Create an instance of the CommandRetransmitter.
   *
   * @param requestId
   * @param json
   * @param actives
   * @param handler
   */
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

  private String refreshKey() {
    return Integer.toString(requestId);
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
    return this.getClass().getSimpleName() + " " + requestId;
  }

  @Override
  public long getPeriod() {
    return RESTART_PERIOD;
  }
}
