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
import edu.umass.cs.gns.localnameserver.LNSTestRequests;
import edu.umass.cs.gns.localnameserver.Lookup;
import edu.umass.cs.gns.localnameserver.PendingTasks;
import edu.umass.cs.gns.localnameserver.Select;
import edu.umass.cs.gns.localnameserver.Update;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.util.MyLogger;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
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
    GNS.getLogger().log(Level.INFO, MyLogger.FORMAT[1], new Object[]{"************************* LNS received: ", json});
    try {
      if (ReconfigurationPacket.isReconfigurationPacket(json)) {
        if (handler.handleEvent(json)) {
          return true;
        }
      }
      Packet.PacketType type = Packet.getPacketType(json);
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("MsgType " + type + " Msg " + json);
      }
      if (type != null) {
        switch (type) {
          case DNS:
            DNSPacket<NodeIDType> dnsPacket = new DNSPacket<NodeIDType>(json, handler.getGnsNodeConfig());
            Packet.PacketType incomingPacketType = Packet.getDNSPacketSubType(dnsPacket);
            switch (incomingPacketType) {
              // Lookup // these have been converted to use handler
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
          case NAME_SERVER_LOAD:
            handler.handleNameServerLoadPacket(json);
            return true;
          // Add/remove
          case ADD_RECORD:
            AddRecordPacket addRecordPacket = new AddRecordPacket(json, handler.getGnsNodeConfig());
            handler.sendRequest(handler.makeCreateNameRequest(addRecordPacket.getName(), addRecordPacket.getValue().toString()));
            //
            //AddRemove.handlePacketAddRecord(json, handler);
            return true;
          case REMOVE_RECORD:
            RemoveRecordPacket removeRecord = new RemoveRecordPacket(json, handler.getGnsNodeConfig());
            handler.sendRequest(handler.makeDeleteNameRequest(removeRecord.getName()));
            //
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
          case NEW_ACTIVE_PROPOSE:
            LNSTestRequests.sendGroupChangeRequest(json, handler);
            return true;
          case GROUP_CHANGE_COMPLETE:
            LNSTestRequests.handleGroupChangeComplete(json);
            return true;
          case COMMAND:
            CommandRequest.handlePacketCommandRequest(json, handler);
            return true;
          default:
            GNS.getLogger().warning("************************* LNS IGNORING: " + json);
            return false;
        }
      }
      GNS.getLogger().warning("************************* LNS CANT GET PACKET TYPE AND IGNORING: " + json);
    } catch (IOException | JSONException e) {
      e.printStackTrace();
    }
    return false;
  }
}
