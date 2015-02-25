/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.localNameServer;

import edu.umass.cs.gns.clientsupport.CommandRequest;
import edu.umass.cs.gns.localnameserver.AddRemove;
import edu.umass.cs.gns.localnameserver.Lookup;
import edu.umass.cs.gns.localnameserver.PendingTasks;
import edu.umass.cs.gns.localnameserver.Select;
import edu.umass.cs.gns.localnameserver.Update;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.util.MyLogger;
import edu.umass.cs.gns.util.ValuesMap;
import java.io.IOException;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 * @param <NodeIDType>
 */
public class NewLNSPacketDemultiplexer<NodeIDType> extends AbstractPacketDemultiplexer {

  private EnhancedClientRequestHandlerInterface<NodeIDType> handler;

  public void setHandler(EnhancedClientRequestHandlerInterface<NodeIDType> handler) {
    this.handler = handler;
  }

  public NewLNSPacketDemultiplexer() {
    // probably should get these from the event handler
    this.register(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME);
    this.register(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME);
    // From current LNS
    register(Packet.PacketType.DNS);
    register(Packet.PacketType.UPDATE);
    register(Packet.PacketType.ADD_RECORD);
    register(Packet.PacketType.COMMAND);
    register(Packet.PacketType.ADD_CONFIRM);
    register(Packet.PacketType.REMOVE_CONFIRM);
    register(Packet.PacketType.UPDATE_CONFIRM);
    register(Packet.PacketType.GROUP_CHANGE_COMPLETE);
    register(Packet.PacketType.NAME_SERVER_LOAD);
    register(Packet.PacketType.NEW_ACTIVE_PROPOSE);
    register(Packet.PacketType.REMOVE_RECORD);
    register(Packet.PacketType.REQUEST_ACTIVES);
    register(Packet.PacketType.SELECT_REQUEST);
    register(Packet.PacketType.SELECT_RESPONSE);
  }

  @Override
  public boolean handleJSONObject(JSONObject json) {
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().log(Level.INFO, MyLogger.FORMAT[1], new Object[]{"************************* LNS received: ", json});
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
      // SOME OF THE CODE BELOW IS NOT APPLICABLE IN THE NEW APP AND IS INCLUDED JUST FOR DOC PURPOSES
      // UNTIL THE TRANSITION IS FINISHED
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
            // New code which creates CreateServiceName packets and sends them to the Reconfigurator.
            // FIXME: Also a little bit of extra bookkeeping going on here so we can get back to the 
            // original AddRecordPacket packet. We could probably replace some of this once everything is working.
            AddRecordPacket addRecordPacket = AddRemove.registerPacketAddRecord(json, handler);
            handler.addCreateMapping(addRecordPacket.getName(), addRecordPacket.getLNSRequestID());
            ValuesMap valuesMap = new ValuesMap();
            valuesMap.putAsArray(addRecordPacket.getRecordKey(), addRecordPacket.getValue());
            handler.sendRequest(new CreateServiceName(null, addRecordPacket.getName(), 0, valuesMap.toString()));
            // original code
            //AddRemove.handlePacketAddRecord(json, handler);
            return true;
          case REMOVE_RECORD:
            // New code which creates RemoveService packets and sends them to the Reconfigurator.
            // FIXME: Also a little bit of extra bookkeeping going on here so we can get back to the 
            // original RemoveRecordPacket packet. We could probably replace some of this once everything is working.
            RemoveRecordPacket removeRecordPacket = AddRemove.registerPacketRemoveRecord(json, handler);
            handler.addRemoveMapping(removeRecordPacket.getName(), removeRecordPacket.getLNSRequestID());
            handler.sendRequest(new DeleteServiceName(null, removeRecordPacket.getName(), 0));
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
          // Requests sent only during testing
//          case NEW_ACTIVE_PROPOSE:
//            LNSTestRequests.sendGroupChangeRequest(json, handler);
//            return true;
//          case GROUP_CHANGE_COMPLETE:
//            LNSTestRequests.handleGroupChangeComplete(json);
//            return true;
          case COMMAND:
            CommandRequest.handlePacketCommandRequest(json, handler);
            return true;
          default:
            GNS.getLogger().warning("************************* LNS IGNORING: " + json);
            return false;
        }
      }
      GNS.getLogger().warning("************************* LNS CAN'T GET PACKET TYPE... IGNORING: " + json);
    } catch (IOException | JSONException e) {
      e.printStackTrace();
    }
    return false;
  }
}
