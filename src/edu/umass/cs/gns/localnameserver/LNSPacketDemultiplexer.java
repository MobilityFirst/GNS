/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nio.BasicPacketDemultiplexer;
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
public class LNSPacketDemultiplexer extends BasicPacketDemultiplexer {

  ClientRequestHandlerInterface handler;

  public LNSPacketDemultiplexer(ClientRequestHandlerInterface handler) {
    this.handler = handler;
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
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("******* Incoming packet: " + json);
    }
    boolean isPacketTypeFound = true;
    try {
      switch (Packet.getPacketType(json)) {
        case DNS:
          DNSPacket dnsPacket = new DNSPacket(json);
          Packet.PacketType incomingPacketType = Packet.getDNSPacketType(dnsPacket);
          switch (incomingPacketType) {
            // Lookup // these have been converted to use handler
            case DNS:
              Lookup.handlePacketLookupRequest(json, dnsPacket, handler);
              break;
            case DNS_RESPONSE:
              Lookup.handlePacketLookupResponse(json, dnsPacket, handler);
              break;
            case DNS_ERROR_RESPONSE:
              Lookup.handlePacketLookupErrorResponse(json, dnsPacket, handler);
              break;
          }
          break;
        // Update // these have NOT been converted to use handler
        case UPDATE:
          Update.handlePacketUpdate(json);
          break;
        case CONFIRM_UPDATE:
          Update.handlePacketConfirmUpdate(json);
          break;
        case NAME_SERVER_LOAD:
          LocalNameServer.handleNameServerLoadPacket(json);
          break;
        // Add/remove
        case ADD_RECORD:
          AddRemove.handlePacketAddRecord(json);
          break;
        case REMOVE_RECORD:
          AddRemove.handlePacketRemoveRecord(json);
          break;
        case CONFIRM_ADD:
          AddRemove.handlePacketConfirmAdd(json);
          break;
        case CONFIRM_REMOVE:
          AddRemove.handlePacketConfirmRemove(json);
          break;
        // Others
        case REQUEST_ACTIVES:
          PendingTasks.handleActivesRequestReply(json);
          break;
        case SELECT_REQUEST:
          Select.handlePacketSelectRequest(json);
          break;
        case SELECT_RESPONSE:
          Select.handlePacketSelectResponse(json);
          break;

        // Requests sent only during testing
        case NEW_ACTIVE_PROPOSE:
          LNSTestRequests.sendGroupChangeRequest(json);
          break;
        case GROUP_CHANGE_COMPLETE:
          LNSTestRequests.handleGroupChangeComplete(json);
          break;
        case LNS_TO_NS_COMMAND:
          LNSToNSCommandRequest.handlePacketCommandRequest(json);
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
