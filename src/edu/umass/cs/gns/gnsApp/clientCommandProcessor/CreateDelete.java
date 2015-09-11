/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.UpdateInfo;
import edu.umass.cs.gns.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gns.gnsApp.NRState;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.Update;
import edu.umass.cs.gns.gnsApp.packet.AddRecordPacket;
import edu.umass.cs.gns.gnsApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.gnsApp.packet.RemoveRecordPacket;
import edu.umass.cs.gns.gnsApp.NSResponseCode;
import edu.umass.cs.gns.utils.ValuesMap;
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

  /**
   * Handles add packets coming in from the client.
   * 
   * @param json
   * @param handler
   * @throws JSONException
   * @throws IOException
   */
  public static void handleAddPacket(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException, IOException {
    if (!AppReconfigurableNodeOptions.standAloneApp) {
      // do normal add which actually involves converting this into a CreateServiceName packet
      AddRecordPacket addRecordPacket = registerPacketAddRecord(json, handler);
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
      AddRecordPacket<String> addRecordPacket = new AddRecordPacket<>(json, handler.getGnsNodeConfig());
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
      ConfirmUpdatePacket<String> confirmPacket = new ConfirmUpdatePacket<>(NSResponseCode.NO_ERROR, addRecordPacket);
      Update.sendConfirmUpdatePacketBackToSource(confirmPacket, handler);
    }
  }

  /**
   * Handles remove packets coming in from the client.
   * 
   * @param json
   * @param handler
   * @throws JSONException
   * @throws IOException
   */
  public static void handleRemovePacket(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException, IOException {
    if (!AppReconfigurableNodeOptions.standAloneApp) {
      RemoveRecordPacket removeRecordPacket = registerPacketRemoveRecord(json, handler);
      handler.addDeleteRequestNameToIDMapping(removeRecordPacket.getName(), removeRecordPacket.getCCPRequestID());
      sendPacketWithRetransmission(removeRecordPacket.getName(),
              new DeleteServiceName(null, removeRecordPacket.getName(), 0),
              handler);
    } else {
      // If we're running standalone just delete the record.
      RemoveRecordPacket<String> removeRecordPacket = new RemoveRecordPacket<>(json, handler.getGnsNodeConfig());
      handler.getApp().updateState(removeRecordPacket.getName(), null);
      // and send a confirmation back
      ConfirmUpdatePacket<String> confirmPacket = new ConfirmUpdatePacket<String>(NSResponseCode.NO_ERROR, removeRecordPacket);
      Update.sendConfirmUpdatePacketBackToSource(confirmPacket, handler);
    }
  }

  private static AddRecordPacket<String> registerPacketAddRecord(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    AddRecordPacket<String> addRecordPacket = new AddRecordPacket<>(json, handler.getGnsNodeConfig());
    int ccpReqId = handler.getUniqueRequestID();
    UpdateInfo<String> info = new UpdateInfo<String>(ccpReqId, addRecordPacket.getName(), null, addRecordPacket, handler);
    handler.addRequestInfo(ccpReqId, info);
    // not sure why this isn't done like this above
    addRecordPacket.setCCPRequestID(ccpReqId);
    return addRecordPacket;
  }

  private static RemoveRecordPacket<String> registerPacketRemoveRecord(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    RemoveRecordPacket<String> removeRecordPacket = new RemoveRecordPacket<String>(json, handler.getGnsNodeConfig());
    int ccpReqId = handler.getUniqueRequestID();
    UpdateInfo<String> info = new UpdateInfo<String>(ccpReqId, removeRecordPacket.getName(), null, removeRecordPacket, handler);
    handler.addRequestInfo(ccpReqId, info);
    // not sure why this isn't done like this above
    removeRecordPacket.setCCPRequestID(ccpReqId);
    return removeRecordPacket;
  }

  private static void sendPacketWithRetransmission(String name, BasicReconfigurationPacket packet, ClientRequestHandlerInterface handler) {
    SendReconfiguratorPacketTask task = new SendReconfiguratorPacketTask(name, packet, handler);
    handler.getExecutorService().scheduleAtFixedRate(task, 0, 
            //AppReconfigurableNodeOptions.queryTimeout,
            5000,
            TimeUnit.MILLISECONDS);
  }

}
