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
import java.util.logging.Level;

import org.json.JSONObject;

/**
 * Handles resending of packets to active replicas.
 *
 * @author westy
 */
public class CommandRetransmitter implements SchedulableProtocolTask<InetSocketAddress, PacketType, String> {

  private final long RESTART_PERIOD = 10000;

  private final long requestId;
  private final RequestHandlerInterface handler;
  private final JSONObject json;
  private final String key;
  private final Set<InetSocketAddress> actives;
  private final Set<InetSocketAddress> activesAlreadyContacted = new HashSet<>();

  /**
   * The logger.
   */
  public static final Logger LOG = Logger.getLogger(Reconfigurator.class.getName());

  /**
   * Create an instance of the CommandRetransmitter.
   *
   * @param requestId
   * @param json
   * @param actives
   * @param handler
   */
  public CommandRetransmitter(long requestId, JSONObject json, Set<InetSocketAddress> actives,
          RequestHandlerInterface handler) {
    this.requestId = requestId;
    this.json = json;
    this.handler = handler;
    this.actives = actives;
    this.key = this.refreshKey();
    LOG.log(Level.FINE, "CommandSender starting: {0}", key);
  }

  /**
   *
   * @return the task
   */
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

  /**
   *
   * @return the task
   */
  @SuppressWarnings("deprecation")
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

  /**
   *
   * @return a set of packet types
   */
  @Override
  public Set<PacketType> getEventTypes() {
    return new HashSet<>();
  }

  /**
   *
   * @return the key
   */
  @Override
  public String getKey() {
    return this.key;
  }

  /**
   *
   * @param event
   * @param ptasks
   * @return an array of tasks
   */
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

  /**
   *
   * @return the period
   */
  @Override
  public long getPeriod() {
    return RESTART_PERIOD;
  }
}
