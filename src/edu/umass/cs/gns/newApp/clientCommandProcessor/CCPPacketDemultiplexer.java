/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor;

import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.AddRemove;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.Lookup;
//import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.PendingTasks;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.Select;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.Update;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.newApp.packet.DNSPacket;
import edu.umass.cs.gns.newApp.packet.Packet;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

import edu.umass.cs.utils.MyLogger;
import java.io.IOException;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 * @param <NodeIDType>
 */
public class CCPPacketDemultiplexer<NodeIDType> extends AbstractJSONPacketDemultiplexer {

  private ClientRequestHandlerInterface handler;

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
      GNS.getLogger().log(Level.INFO, MyLogger.FORMAT[1], 
              new Object[]{"*****************************> CCP RECEIVED: ", json});
    }
    try {
      if (ReconfigurationPacket.isReconfigurationPacket(json)) {
        if (handler.handleEvent(json)) {
          return true;
        }
      }
      Packet.PacketType type = Packet.getPacketType(json);
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("MsgType " + type + " Msg " + json);
      }
      if (type != null) {
        switch (type) {
          case DNS:
            DNSPacket<String> dnsPacket = new DNSPacket<String>(json, handler.getGnsNodeConfig());
            Packet.PacketType incomingPacketType = Packet.getDNSPacketSubType(dnsPacket);
            switch (incomingPacketType) {
              // Lookup
              case DNS_SUBTYPE_QUERY:
                Lookup.handlePacketLookupRequest(json, dnsPacket, handler);
                return true;
              case DNS_SUBTYPE_RESPONSE:
                Lookup.handlePacketLookupResponse(json, dnsPacket, handler);
                return true;
              case DNS_SUBTYPE_ERROR_RESPONSE:
                Lookup.handlePacketLookupErrorResponse(json, dnsPacket, handler);
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
          // Others
//          case REQUEST_ACTIVES:
//            PendingTasks.handleActivesRequestReply(json, handler);
//            return true;
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
