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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.AddRemove;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.Lookup;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.Select;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.Update;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsApp.packet.DNSPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

import edu.umass.cs.utils.MyLogger;
import java.io.IOException;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Processes incoming and outgoing incoming and outgoing lookup (DNS), updated
 * and select packets. Basically, all command packets are converted into a
 * one or more of the above packet types and dispatched thru this class which
 * invokes static methods from {@link Lookup}, {@link Update},
 * {@link CreateDelete}, {@link AddRemove} and {@link Select} classes.
 *
 * @author westy
 * @param <NodeIDType>
 */
// FIXME: Do we even need a Demultiplexer here anymore?
public class CCPPacketDemultiplexer<NodeIDType> extends AbstractJSONPacketDemultiplexer {

  private ClientRequestHandlerInterface handler;

  /**
   * Sets the handler.
   *
   * @param handler
   */
  public void setHandler(ClientRequestHandlerInterface handler) {
    this.handler = handler;
  }

  /**
   * Create an instance of the CCPPacketDemultiplexer.
   */
  public CCPPacketDemultiplexer() {
    // probably should get these from the event handler
    register(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME);
    register(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME);
    //register(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS);
    // From current LNS
    register(Packet.PacketType.DNS);
    register(Packet.PacketType.UPDATE);
    register(Packet.PacketType.ADD_RECORD);
    register(Packet.PacketType.ADD_BATCH_RECORD);
    register(Packet.PacketType.REMOVE_RECORD);
    register(Packet.PacketType.ADD_CONFIRM);
    register(Packet.PacketType.REMOVE_CONFIRM);
    register(Packet.PacketType.UPDATE_CONFIRM);
    //register(Packet.PacketType.REQUEST_ACTIVES);
    register(Packet.PacketType.SELECT_REQUEST);
    register(Packet.PacketType.SELECT_RESPONSE);
  }

  @Override
  public boolean handleMessage(JSONObject json) {
    handler.updateRequestStatistics();
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().log(Level.FINER, MyLogger.FORMAT[1],
              new Object[]{"*****************************> CCP RECEIVED: ", json});
    }
    try {
      if (ReconfigurationPacket.isReconfigurationPacket(json)) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().log(Level.INFO, MyLogger.FORMAT[1],
                  new Object[]{"*****************************> CCP RECEIVED PACKET TYPE: ",
                    ReconfigurationPacket.getReconfigurationPacketType(json)});
        }
        if (handler.handleEvent(json)) {
          return true;
        }
      }
      Packet.PacketType type = Packet.getPacketType(json);
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().finer("MsgType " + type + " Msg " + json);
        GNS.getLogger().info("MsgType " + type);
      }
      if (type != null) {
        switch (type) {
          case DNS:
            DNSPacket<String> dnsPacket = new DNSPacket<String>(json, handler.getGnsNodeConfig());
            Packet.PacketType incomingPacketType = Packet.getDNSPacketSubType(dnsPacket);
            switch (incomingPacketType) {
              // Lookup
              case DNS_SUBTYPE_QUERY:
                Lookup.handlePacketLookupRequest(dnsPacket, handler);
                return true;
              case DNS_SUBTYPE_RESPONSE:
                Lookup.handlePacketLookupResponse(dnsPacket, handler);
                return true;
              case DNS_SUBTYPE_ERROR_RESPONSE:
                Lookup.handlePacketLookupErrorResponse(dnsPacket, handler);
                return true;
              default:
                GNS.getLogger().warning("Unknown DNS packet subtype: " + incomingPacketType);
                return false;
            }
          case UPDATE:
            Update.handlePacketUpdate(json, handler);
            return true;
          case UPDATE_CONFIRM:
            Update.handlePacketConfirmUpdate(json, handler);
            return true;
          // Add/remove
          case ADD_RECORD:
            // New code which creates CreateServiceName packets and sends them to the Reconfigurator.
            CreateDelete.handleAddPacket(json, handler);
            return true;
          case ADD_BATCH_RECORD:
            // New code which creates CreateServiceName packets and sends them to the Reconfigurator.
            CreateDelete.handleAddBatchPacket(json, handler);
            return true;
          case REMOVE_RECORD:
            // New code which creates DeleteService packets and sends them to the Reconfigurator.
            CreateDelete.handleRemovePacket(json, handler);
            return true;
          case ADD_CONFIRM:
            AddRemove.handlePacketConfirmAdd(json, handler);
            return true;
          case REMOVE_CONFIRM:
            AddRemove.handlePacketConfirmRemove(json, handler);
            return true;
          case SELECT_REQUEST:
            Select.handlePacketSelectRequest(json, handler);
            return true;
          case SELECT_RESPONSE:
            Select.handlePacketSelectResponse(json, handler);
            return true;
          default:
            GNS.getLogger().warning("************************* CCP IGNORING: " + json);
            return false;
        }
      }
      GNS.getLogger().warning("************************* CCP CAN'T GET PACKET TYPE... IGNORING: " + json);
    } catch (IOException | JSONException e) {
      e.printStackTrace();
    }
    return false;
  }

}
