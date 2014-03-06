/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.Packet;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.TimerTask;

/**
 * Implements the <code>PacketDemultiplexer</code> interface for using the {@link edu.umass.cs.gns.nio} package.
 *
 * Created by abhigyan on 2/24/14.
 */
public class LNSPacketDemultiplexer extends PacketDemultiplexer {

  @Override
  public void handleJSONObjects(ArrayList jsonObjects) {
    for (Object o : jsonObjects) {
      LocalNameServer.getExecutorService().submit(new LNSTask((JSONObject) o));
    }
  }

  /**
   * This is the entry point for all message received at a local name server. It de-multiplexes packets based on
   * their packet type and forwards to appropriate classes.
   */
  public static void demultiplexLNSPackets(JSONObject json) {
    try {
      switch (Packet.getPacketType(json)) {
        case DNS:
          DNSPacket dnsPacket = new DNSPacket(json);
          Packet.PacketType incomingPacketType = Packet.getDNSPacketType(dnsPacket);
          switch (incomingPacketType) {
            // Lookup
            case DNS:
              Lookup.handlePacketLookupRequest(json, dnsPacket);
              break;
            case DNS_RESPONSE:
              Lookup.handlePacketLookupResponse(json, dnsPacket);
              break;
            case DNS_ERROR_RESPONSE:
              Lookup.handlePacketLookupErrorResponse(json, dnsPacket);
              break;
          }
          break;
        // Update
        case UPDATE_ADDRESS_LNS:
          Update.handlePacketUpdateAddressLNS(json);
          break;
        case CONFIRM_UPDATE_LNS:
          Update.handlePacketConfirmUpdateLNS(json);
          break;
        case NAME_SERVER_LOAD:
          LocalNameServer.handleNameServerLoadPacket(json);
          break;
        // Add/remove
        case ADD_RECORD_LNS:
          AddRemove.handlePacketAddRecordLNS(json);
          break;
        case REMOVE_RECORD_LNS:
          AddRemove.handlePacketRemoveRecordLNS(json);
          break;
        case CONFIRM_ADD_LNS:
          AddRemove.handlePacketConfirmAddLNS(json);
          break;
        case CONFIRM_REMOVE_LNS:
          AddRemove.handlePacketConfirmRemoveLNS(json);
          break;
        // Others
        case NAMESERVER_SELECTION:
          NameServerVoteThread.handleNameServerSelection(json);
          break;
        case REQUEST_ACTIVES:
          PendingTasks.handleActivesRequestReply(json);
          break;
        case SELECT_REQUEST:
          Select.handlePacketSelectRequest(json);
          break;
        case SELECT_RESPONSE:
          Select.handlePacketSelectResponse(json);
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
  }

  class LNSTask extends TimerTask {

    JSONObject json;

    public LNSTask(JSONObject jsonObject) {
      this.json = jsonObject;
    }

    @Override
    public void run() {

      try {
        LNSPacketDemultiplexer.demultiplexLNSPackets(json);
      } catch (Exception e) {
        GNS.getLogger().severe("Exception handling packets: " + e.getMessage());
        e.printStackTrace();
      }

    }
  }
}
