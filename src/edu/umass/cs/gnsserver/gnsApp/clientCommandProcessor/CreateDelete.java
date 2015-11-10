/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.UpdateInfo;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.NRState;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.Update;
import edu.umass.cs.gnsserver.gnsApp.packet.AddRecordPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.packet.RemoveRecordPacket;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsApp.packet.AddBatchRecordPacket;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;

import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
      handler.getApp().restore(addRecordPacket.getName(), newState.toString());
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
      handler.getApp().restore(removeRecordPacket.getName(), null);
      // and send a confirmation back
      ConfirmUpdatePacket<String> confirmPacket = new ConfirmUpdatePacket<String>(NSResponseCode.NO_ERROR, removeRecordPacket);
      Update.sendConfirmUpdatePacketBackToSource(confirmPacket, handler);
    }
  }

  private static AddRecordPacket<String> registerPacketAddRecord(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    AddRecordPacket<String> addRecordPacket = new AddRecordPacket<>(json, handler.getGnsNodeConfig());
    int ccpReqId = handler.getUniqueRequestID();
    UpdateInfo<String> info = new UpdateInfo<String>(ccpReqId, addRecordPacket.getName(), null,
            addRecordPacket, handler);
    handler.addRequestInfo(ccpReqId, info);
    // not sure why this isn't done like this above
    addRecordPacket.setCCPRequestID(ccpReqId);
    return addRecordPacket;
  }

  private static AddBatchRecordPacket<String> registerPacketAddBatchRecord(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    AddBatchRecordPacket<String> addbatchRecordPacket = new AddBatchRecordPacket<>(json, handler.getGnsNodeConfig());
    int ccpReqId = handler.getUniqueRequestID();
    UpdateInfo<String> info = new UpdateInfo<String>(ccpReqId, addbatchRecordPacket.getServiceName(), null,
            addbatchRecordPacket, handler);
    handler.addRequestInfo(ccpReqId, info);
    // not sure why this isn't done like this above
    addbatchRecordPacket.setCCPRequestID(ccpReqId);
    return addbatchRecordPacket;
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

  private static void sendPacketWithRetransmission(String name, BasicReconfigurationPacket packet, 
          ClientRequestHandlerInterface handler, int delay, TimeUnit delayUnit, 
          int maxWaitTime, int maxRetries) {
    SendReconfiguratorPacketTask task = new SendReconfiguratorPacketTask(name, packet, handler, 
            maxWaitTime, maxRetries);
    handler.getExecutorService().scheduleAtFixedRate(task, 0, delay, delayUnit);
  }
  
  private static void sendPacketWithRetransmission(String name, BasicReconfigurationPacket packet, 
          ClientRequestHandlerInterface handler) {
    sendPacketWithRetransmission(name, packet, handler, 5000, TimeUnit.MILLISECONDS, 
            handler.getParameters().getMaxQueryWaitTime(), 3);
  }
  
  /**
   * Handles packets from the client that are trying to created multiple records.
   * 
   * Similar to handleAddPacket.
   * 
   * @param json
   * @param handler
   * @throws JSONException
   * @throws IOException 
   */
  public static void handleAddBatchPacket(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException, IOException {
    if (!AppReconfigurableNodeOptions.standAloneApp) {
      AddBatchRecordPacket<String> packet = registerPacketAddBatchRecord(json, handler);
     
      CreateServiceName[] creates = makeBatchedCreateNameRequest(packet.getNames(), packet.getValues(), handler);
      for (CreateServiceName create : creates) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().severe("??????????????????????????? Sending recon packet for NAME = " 
                  + create.getServiceName());
        }
        handler.addCreateRequestNameToIDMapping(create.getServiceName(), packet.getCCPRequestID());
        // Batch create can be slow, defaults are 5 and 16 seconds
        sendPacketWithRetransmission(create.getServiceName(), create, handler, 
                20000, TimeUnit.MILLISECONDS, 60000, 2);
      }
    } else {
      // LATER
//      // If we're running standalone just add the record.
//      AddRecordPacket<String> AddBatchRecordPacket = new AddBatchRecordPacket<>(json, handler.getGnsNodeConfig());
//      ValuesMap valuesMap;
//      if (addRecordPacket.getField() != null) {
//        valuesMap = new ValuesMap();
//        valuesMap.putAsArray(addRecordPacket.getField(), addRecordPacket.getFieldValue());
//      } else {
//        valuesMap = new ValuesMap(addRecordPacket.getValues());
//      }
//      NRState newState = new NRState(valuesMap, 0);
//      handler.getApp().updateState(addRecordPacket.getName(), newState.toString());
//      // and send a confirmation back
//      ConfirmUpdatePacket<String> confirmPacket = new ConfirmUpdatePacket<>(NSResponseCode.NO_ERROR, addRecordPacket);
//      Update.sendConfirmUpdatePacketBackToSource(confirmPacket, handler);
    }
  }

  // based on edu.umass.cs.reconfiguration.testing.ReconfigurableClientCreateTester but this one
  // handles multiple states
  private static CreateServiceName[] makeBatchedCreateNameRequest(Set<String> names,
          JSONObject states, ClientRequestHandlerInterface handler) throws JSONException {
    Collection<Set<String>> batches = ConsistentReconfigurableNodeConfig
            .splitIntoRCGroups(names, handler.getGnsNodeConfig().getReconfigurators());

    Set<CreateServiceName> creates = new HashSet<CreateServiceName>();
    // each batched create corresponds to a different RC group
    for (Set<String> batch : batches) {
      Map<String, String> nameStates = new HashMap<String, String>();
      for (String name : batch) {
        nameStates.put(name, states.getJSONObject(name).toString());
      }
      // a single batched create
      creates.add(new CreateServiceName(null, nameStates));
    }
    return creates.toArray(new CreateServiceName[0]);
  }

}
