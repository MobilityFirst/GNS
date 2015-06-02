/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor;

import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.UpdateInfo;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.newApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gns.newApp.packet.AddRecordPacket;
import edu.umass.cs.gns.newApp.packet.RemoveRecordPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.util.ValuesMap;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Some code to paper over the differences between the old Add/Remove protocol
 * and the new Create/Delete Protocol. HAndles retransmission of
 * Create and Delete packets. See also SendReconfiguratorPacketTask.
 *
 * The big issue that should be recoded is the
 * packet class hierarchy.
 *
 * @author westy
 */
public class CreateDelete {

  public static void handleAddPacket(JSONObject json, EnhancedClientRequestHandlerInterface handler) throws JSONException, IOException {
    AddRecordPacket addRecordPacket = CreateDelete.registerPacketAddRecord(json, handler);
    handler.addCreateRequestNameToIDMapping(addRecordPacket.getName(), addRecordPacket.getLNSRequestID());
    ValuesMap valuesMap = new ValuesMap();
    valuesMap.putAsArray(addRecordPacket.getRecordKey(), addRecordPacket.getValue());
    sendPacket(addRecordPacket.getName(),
            new CreateServiceName(null, addRecordPacket.getName(), 0, valuesMap.toString()),
            handler);
    }

  public static void handleRemovePacket(JSONObject json, EnhancedClientRequestHandlerInterface handler) throws JSONException, IOException {
    RemoveRecordPacket removeRecordPacket = CreateDelete.registerPacketRemoveRecord(json, handler);
    handler.addDeleteRequestNameToIDMapping(removeRecordPacket.getName(), removeRecordPacket.getLNSRequestID());
    sendPacket(removeRecordPacket.getName(),
            new DeleteServiceName(null, removeRecordPacket.getName(), 0),
            handler);
    }

  public static AddRecordPacket registerPacketAddRecord(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    AddRecordPacket addRecordPacket = new AddRecordPacket(json, handler.getGnsNodeConfig());
    int lnsReqID = handler.getUniqueRequestID();
    UpdateInfo info = new UpdateInfo(lnsReqID, addRecordPacket.getName(), null, addRecordPacket, handler);
    handler.addRequestInfo(lnsReqID, info);
    // not sure why this isn't done like this above
    addRecordPacket.setLNSRequestID(lnsReqID);
    return addRecordPacket;
  }

  public static RemoveRecordPacket registerPacketRemoveRecord(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    RemoveRecordPacket removeRecordPacket = new RemoveRecordPacket(json, handler.getGnsNodeConfig());
    int lnsReqID = handler.getUniqueRequestID();
    UpdateInfo info = new UpdateInfo(lnsReqID, removeRecordPacket.getName(), null, removeRecordPacket, handler);
    handler.addRequestInfo(lnsReqID, info);
    // not sure why this isn't done like this above
    removeRecordPacket.setLNSRequestID(lnsReqID);
    return removeRecordPacket;
  }

  public static void sendPacket(String name, BasicReconfigurationPacket packet, EnhancedClientRequestHandlerInterface handler) {
    SendReconfiguratorPacketTask task = new SendReconfiguratorPacketTask(name, packet, handler);
    handler.getExecutorService().scheduleAtFixedRate(task, 0, AppReconfigurableNodeOptions.queryTimeout, TimeUnit.MILLISECONDS);
  }

}
