/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gns.gnsApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.utils.DelayProfiler;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;

/**
 * Class contains a few static methods for handling ADD, and REMOVE requests from clients
 * as well responses to these requests from name servers. Most functionality for handling request sent by clients
 * is implemented in <code>SendAddRemoveTask</code>. So also refer to its documentation.
 * <p>
 * The addition and removal of a name in GNS is handled by replica controllers, therefore we send ADD and REMOVE
 * to a replica controller. The replica controllers for a name are fixed and a local name server can compute the set of
 * replica controllers locally (see method {@link edu.umass.cs.gns.utils.ConsistentHashing#getReplicaControllerSet(String)}).
 * Like other requests, add/removes are also retransmitted to a different name server if no confirmation is received
 * until a timeout value.
 * <p>
 *
 * @author abhigyan
 */
public class AddRemove {

  /**
   * Handles confirmation of add request from NS
   * @param json
   * @param handler
   * @throws org.json.JSONException
   * @throws java.net.UnknownHostException
   */
  public static void handlePacketConfirmAdd(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException, UnknownHostException {
    ConfirmUpdatePacket<String> confirmAddPacket = new ConfirmUpdatePacket<String>(json, handler.getGnsNodeConfig());
    @SuppressWarnings("unchecked")
    UpdateInfo<String> addInfo = (UpdateInfo<String>) handler.removeRequestInfo(confirmAddPacket.getCCPRequestID());
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info("Confirm add packet for " + addInfo.getName() + ": " + confirmAddPacket.toString());
    }
    if (addInfo == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().warning("Add confirmation return info not found.: lns request id = "
                + confirmAddPacket.getCCPRequestID());
      }
    } else {
      DelayProfiler.updateDelay("serviceNameAdd", (System.currentTimeMillis() - addInfo.getStartTime()));
      Update.sendConfirmUpdatePacketBackToSource(confirmAddPacket, handler);
    }
  }

  /**
   * Handles confirmation of add request from NS
   * @param json
   * @param handler
   * @throws org.json.JSONException
   * @throws java.net.UnknownHostException
   */
  public static void handlePacketConfirmRemove(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException, UnknownHostException {
    ConfirmUpdatePacket<String> confirmRemovePacket = new ConfirmUpdatePacket<String>(json, handler.getGnsNodeConfig());
    @SuppressWarnings("unchecked")
    UpdateInfo<String> removeInfo = (UpdateInfo<String>) handler.removeRequestInfo(confirmRemovePacket.getCCPRequestID());
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Confirm remove packet for " + removeInfo.getName() + ": " + confirmRemovePacket.toString() + " remove info " + removeInfo);
    }
    if (removeInfo == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().warning("Remove confirmation return info not found.: lns request id = " + confirmRemovePacket.getCCPRequestID());
      }
    } else {
      DelayProfiler.updateDelay("serviceNameRemove", (System.currentTimeMillis() - removeInfo.getStartTime()));
      Update.sendConfirmUpdatePacketBackToSource(confirmRemovePacket, handler);
    }
  }
}
