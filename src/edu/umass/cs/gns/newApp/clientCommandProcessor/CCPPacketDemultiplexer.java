/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor;

import edu.umass.cs.gns.clientsupport.CommandHandler;
import edu.umass.cs.gns.clientCommandProcessor.AddRemove;
import edu.umass.cs.gns.clientCommandProcessor.Lookup;
import edu.umass.cs.gns.clientCommandProcessor.PendingTasks;
import edu.umass.cs.gns.clientCommandProcessor.Select;
import edu.umass.cs.gns.clientCommandProcessor.Update;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.util.MyLogger;
import java.io.IOException;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 * @param <NodeIDType>
 */
public class CCPPacketDemultiplexer<NodeIDType> extends AbstractPacketDemultiplexer {

  private EnhancedClientRequestHandlerInterface<NodeIDType> handler;

  public void setHandler(EnhancedClientRequestHandlerInterface<NodeIDType> handler) {
    this.handler = handler;
  }

  public CCPPacketDemultiplexer() {
    // probably should get these from the event handler
    register(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME);
    register(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME);
    register(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS);
    // From current LNS
    register(Packet.PacketType.DNS);
    register(Packet.PacketType.UPDATE);
    register(Packet.PacketType.ADD_RECORD);
    register(Packet.PacketType.REMOVE_RECORD);
    register(Packet.PacketType.ADD_CONFIRM);
    register(Packet.PacketType.REMOVE_CONFIRM);
    register(Packet.PacketType.UPDATE_CONFIRM);
    register(Packet.PacketType.COMMAND);
    register(Packet.PacketType.REQUEST_ACTIVES);
    register(Packet.PacketType.SELECT_REQUEST);
    register(Packet.PacketType.SELECT_RESPONSE);
  }

  @Override
  public boolean handleJSONObject(JSONObject json) {
    handler.updateRequestStatistics();
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().log(Level.INFO, MyLogger.FORMAT[1], new Object[]{"************************* CPP received: ", json});
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
            DNSPacket<NodeIDType> dnsPacket = new DNSPacket<NodeIDType>(json, handler.getGnsNodeConfig());
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
          // Update // some of these have been converted to use handler
          case UPDATE:
            Update.handlePacketUpdate(json, handler);
            return true;
          case UPDATE_CONFIRM:
            Update.handlePacketConfirmUpdate(json, handler);
            return true;
          // Add/remove
          case ADD_RECORD:
             CreateDelete.handleAddPacket(json, handler);
            // New code which creates CreateServiceName packets and sends them to the Reconfigurator.
            // FIXME: Also a little bit of extra bookkeeping going on here so we can get back to the 
            // original AddRecordPacket packet. We could probably replace some of this once everything is working.
            //AddRecordPacket addRecordPacket = CreateDelete.registerPacketAddRecord(json, handler);
            //handler.addRequestNameToIDMapping(addRecordPacket.getName(), addRecordPacket.getLNSRequestID());
            //ValuesMap valuesMap = new ValuesMap();
            //valuesMap.putAsArray(addRecordPacket.getRecordKey(), addRecordPacket.getValue());
            //handler.sendRequestToRandomReconfigurator(new CreateServiceName(null, addRecordPacket.getName(), 0, valuesMap.toString()));
            // original code
            //AddRemove.handlePacketAddRecord(json, handler);
            return true;
          case REMOVE_RECORD:
            // New code which creates DeleteService packets and sends them to the Reconfigurator.
            CreateDelete.handleRemovePacket(json, handler);
            // FIXME: Also a little bit of extra bookkeeping going on here so we can get back to the 
            // original RemoveRecordPacket packet. We could probably replace some of this once everything is working.
            //RemoveRecordPacket removeRecordPacket = CreateDelete.registerPacketRemoveRecord(json, handler);
            //handler.addRequestNameToIDMapping(removeRecordPacket.getName(), removeRecordPacket.getLNSRequestID());
            //handler.sendRequestToRandomReconfigurator(new DeleteServiceName(null, removeRecordPacket.getName(), 0));
            // original code
            //AddRemove.handlePacketRemoveRecord(json, handler);
            return true;
          case ADD_CONFIRM:
            AddRemove.handlePacketConfirmAdd(json, handler);
            return true;
          case REMOVE_CONFIRM:
            AddRemove.handlePacketConfirmRemove(json, handler);
            return true;
          // Others
          case REQUEST_ACTIVES:
            PendingTasks.handleActivesRequestReply(json, handler);
            return true;
          case SELECT_REQUEST:
            Select.handlePacketSelectRequest(json, handler);
            return true;
          case SELECT_RESPONSE:
            Select.handlePacketSelectResponse(json, handler);
            return true;
          case COMMAND:
            CommandHandler.handlePacketCommandRequest(json, handler);
            return true;
          default:
            GNS.getLogger().warning("************************* CPP IGNORING: " + json);
            return false;
        }
      }
      GNS.getLogger().warning("************************* CPP CAN'T GET PACKET TYPE... IGNORING: " + json);
    } catch (IOException | JSONException e) {
      e.printStackTrace();
    }
    return false;
  }
}
