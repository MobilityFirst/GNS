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
import edu.umass.cs.gns.newApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gns.newApp.NRState;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.AddRemove;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.Update;
import edu.umass.cs.gns.newApp.packet.AddRecordPacket;
import edu.umass.cs.gns.newApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.newApp.packet.RemoveRecordPacket;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ValuesMap;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;

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
    if (!AppReconfigurableNodeOptions.standAloneApp) {
      // do normal add which actually involves converting this into a CreateServiceName packet
      AddRecordPacket addRecordPacket = CreateDelete.registerPacketAddRecord(json, handler);
      handler.addCreateRequestNameToIDMapping(addRecordPacket.getName(), addRecordPacket.getCCPRequestID());
      ValuesMap valuesMap;
      if (addRecordPacket.getField() != null) {
        valuesMap = new ValuesMap();
        valuesMap.putAsArray(addRecordPacket.getField(), addRecordPacket.getFieldValue());
      } else {
        valuesMap = new ValuesMap(addRecordPacket.getValues());
      }
      sendPacketWithRetransmission(addRecordPacket.getName(),
              new CreateServiceName(null, addRecordPacket.getName(), 0, valuesMap.toString()),
              handler);
    } else {
      // If we're running standalone just add the record.
      AddRecordPacket addRecordPacket = new AddRecordPacket(json, handler.getGnsNodeConfig());
      ValuesMap valuesMap;
      if (addRecordPacket.getField() != null) {
        valuesMap = new ValuesMap();
        valuesMap.putAsArray(addRecordPacket.getField(), addRecordPacket.getFieldValue());
      } else {
        valuesMap = new ValuesMap(addRecordPacket.getValues());
      }
      NRState newState = new NRState(valuesMap, 0);
      handler.getApp().updateState(addRecordPacket.getName(), newState.toString());
      // and send a confirmation back
      ConfirmUpdatePacket confirmPacket = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, addRecordPacket);
      Update.sendConfirmUpdatePacketBackToSource(confirmPacket, handler);
    }
  }

  public static void handleRemovePacket(JSONObject json, EnhancedClientRequestHandlerInterface handler) throws JSONException, IOException {
    if (!AppReconfigurableNodeOptions.standAloneApp) {
      RemoveRecordPacket removeRecordPacket = CreateDelete.registerPacketRemoveRecord(json, handler);
      handler.addDeleteRequestNameToIDMapping(removeRecordPacket.getName(), removeRecordPacket.getCCPRequestID());
      sendPacketWithRetransmission(removeRecordPacket.getName(),
              new DeleteServiceName(null, removeRecordPacket.getName(), 0),
              handler);
    } else {
      // If we're running standalone just delete the record.
      RemoveRecordPacket removeRecordPacket = new RemoveRecordPacket(json, handler.getGnsNodeConfig());
      handler.getApp().updateState(removeRecordPacket.getName(), null);
      // and send a confirmation back
      ConfirmUpdatePacket confirmPacket = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, removeRecordPacket);
      Update.sendConfirmUpdatePacketBackToSource(confirmPacket, handler);
    }
  }

  public static AddRecordPacket registerPacketAddRecord(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    AddRecordPacket addRecordPacket = new AddRecordPacket(json, handler.getGnsNodeConfig());
    int lnsReqID = handler.getUniqueRequestID();
    UpdateInfo info = new UpdateInfo(lnsReqID, addRecordPacket.getName(), null, addRecordPacket, handler);
    handler.addRequestInfo(lnsReqID, info);
    // not sure why this isn't done like this above
    addRecordPacket.setCCPRequestID(lnsReqID);
    return addRecordPacket;
  }

  public static RemoveRecordPacket registerPacketRemoveRecord(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    RemoveRecordPacket removeRecordPacket = new RemoveRecordPacket(json, handler.getGnsNodeConfig());
    int lnsReqID = handler.getUniqueRequestID();
    UpdateInfo info = new UpdateInfo(lnsReqID, removeRecordPacket.getName(), null, removeRecordPacket, handler);
    handler.addRequestInfo(lnsReqID, info);
    // not sure why this isn't done like this above
    removeRecordPacket.setCCPRequestID(lnsReqID);
    return removeRecordPacket;
  }

  public static void sendPacketWithRetransmission(String name, BasicReconfigurationPacket packet, EnhancedClientRequestHandlerInterface handler) {
    SendReconfiguratorPacketTask task = new SendReconfiguratorPacketTask(name, packet, handler);
    handler.getExecutorService().scheduleAtFixedRate(task, 0, AppReconfigurableNodeOptions.queryTimeout, TimeUnit.MILLISECONDS);
  }

}
