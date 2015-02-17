/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.test;

import edu.umass.cs.gns.clientsupport.CommandRequest;
import edu.umass.cs.gns.localnameserver.AddRemove;
import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.localnameserver.LNSTestRequests;
import edu.umass.cs.gns.localnameserver.Lookup;
import edu.umass.cs.gns.localnameserver.PendingTasks;
import edu.umass.cs.gns.localnameserver.Select;
import edu.umass.cs.gns.localnameserver.Update;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.util.MyLogger;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class DummyLNSPacketDemultiplexer<NodeIDType> extends AbstractPacketDemultiplexer {

  private EnhancedClientRequestHandlerInterface<NodeIDType> handler;

  public void setHandler(EnhancedClientRequestHandlerInterface<NodeIDType> handler) {
    this.handler = handler;
  }

  public DummyLNSPacketDemultiplexer() {
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
                break;
              case DNS_SUBTYPE_RESPONSE:
                Lookup.handlePacketLookupResponse(json, dnsPacket, handler);
                break;
              case DNS_SUBTYPE_ERROR_RESPONSE:
                Lookup.handlePacketLookupErrorResponse(json, dnsPacket, handler);
                break;
            }
            break;
          // Update // some of these have been converted to use handler
          case UPDATE:
            Update.handlePacketUpdate(json, handler);
            break;
          case UPDATE_CONFIRM:
            Update.handlePacketConfirmUpdate(json, handler);
            break;
          case NAME_SERVER_LOAD:
            handler.handleNameServerLoadPacket(json);
            break;
          // Add/remove
          case ADD_RECORD:
            AddRecordPacket addRecordPacket = new AddRecordPacket(json, handler.getGnsNodeConfig());
            handler.sendRequest(handler.makeCreateNameRequest(addRecordPacket.getName(), addRecordPacket.getValue().toString()));
            //
            AddRemove.handlePacketAddRecord(json, handler);
            break;
          case REMOVE_RECORD:
            RemoveRecordPacket removeRecord = new RemoveRecordPacket(json, handler.getGnsNodeConfig());
            handler.sendRequest(handler.makeDeleteNameRequest(removeRecord.getName()));
            //
            AddRemove.handlePacketRemoveRecord(json, handler);
            break;
          case ADD_CONFIRM:
            AddRemove.handlePacketConfirmAdd(json, handler);
            break;
          case REMOVE_CONFIRM:
            AddRemove.handlePacketConfirmRemove(json, handler);
            break;
          // Others
          case REQUEST_ACTIVES:
            PendingTasks.handleActivesRequestReply(json, handler);
            break;
          case SELECT_REQUEST:
            Select.handlePacketSelectRequest(json, handler);
            break;
          case SELECT_RESPONSE:
            Select.handlePacketSelectResponse(json, handler);
            break;
          // Requests sent only during testing
          case NEW_ACTIVE_PROPOSE:
            LNSTestRequests.sendGroupChangeRequest(json, handler);
            break;
          case GROUP_CHANGE_COMPLETE:
            LNSTestRequests.handleGroupChangeComplete(json);
            break;
          case COMMAND:
            CommandRequest.handlePacketCommandRequest(json, handler);
            break;
          default:
            //isPacketTypeFound = false;
            GNS.getLogger().log(Level.INFO, MyLogger.FORMAT[1], new Object[]{"************************* LNS IGNORING: ", json});
            break;
        }
      } else {
        switch (ReconfigurationPacket.getReconfigurationPacketType(json)) {
          case CREATE_SERVICE_NAME:
            CreateServiceName create = new CreateServiceName(json);
            GNS.getLogger().log(Level.INFO, MyLogger.FORMAT[2], new Object[]{"App", " created ", create.getServiceName()});
            break;
          // We never receive this, but then again neither does the 
          case DELETE_SERVICE_NAME:
            DeleteServiceName delete = new DeleteServiceName(json);
            GNS.getLogger().log(Level.INFO, MyLogger.FORMAT[1], new Object[]{"App deleted ", delete.getServiceName()});
            break;
          default:
            GNS.getLogger().log(Level.INFO, MyLogger.FORMAT[1], new Object[]{"************************* LNS IGNORING: ", json});
            break;
        }
      }
    } catch (IOException | JSONException | NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    return true;
  }
}
