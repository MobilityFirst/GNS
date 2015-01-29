/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.CommandRequest;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;

/**
 * Implements the <code>BasicPacketDemultiplexer</code> interface for using the {@link edu.umass.cs.gns.nio} package.
 *
 * Created by abhigyan on 2/24/14.
 */
public class LNSPacketDemultiplexer<NodeIDType> extends AbstractPacketDemultiplexer {

  ClientRequestHandlerInterface<NodeIDType> handler;

  public LNSPacketDemultiplexer(ClientRequestHandlerInterface<NodeIDType> handler) {
    this.handler = handler;
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

  /**
   * This is the entry point for all message received at a local name server. It de-multiplexes packets based on
   * their packet type and forwards to appropriate classes.
   *
   * @param json
   * @return false if and invalid packet type is received
   */
  @Override
  public boolean handleJSONObject(JSONObject json) {
    handler.updateRequestStatistics();
    if (StartLocalNameServer.debuggingEnabled) {
      GNS.getLogger().fine("******* Incoming packet: " + json);
    }
    boolean isPacketTypeFound = true;
    try {
      switch (Packet.getPacketType(json)) {
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
        // Add/remove // these have been converted to use handler
        case ADD_RECORD:
          AddRemove.handlePacketAddRecord(json, handler);
          break;
        case REMOVE_RECORD:
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
          isPacketTypeFound = false;
          break;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return isPacketTypeFound;
  }

}
