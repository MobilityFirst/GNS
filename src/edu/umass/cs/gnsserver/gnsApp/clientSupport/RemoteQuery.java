/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientSupport;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnscommon.asynch.ClientAsynchBase;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.GnsACLException;
import edu.umass.cs.gnscommon.exceptions.client.GnsDuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnscommon.exceptions.client.GnsFieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidFieldException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidGroupException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidGuidException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidUserException;
import edu.umass.cs.gnscommon.exceptions.client.GnsVerificationException;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnscommon.exceptions.client.GnsOperationNotSupportedException;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.SelectHandler.DEFAULT_MIN_REFRESH_INTERVAL;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsApp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectGroupBehavior;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectOperation;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectResponsePacket;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A synchronized version of ClientAsynchBase for sending requests to other servers.
 *
 * @author westy
 */
public class RemoteQuery extends ClientAsynchBase {

  // For synchronus replica messages
  private long replicaReadTimeout = 10000;
  private final ConcurrentMap<Long, ClientRequest> replicaResultMap
          = new ConcurrentHashMap<>(10, 0.75f, 3);
  private final Object replicaCommandMonitor = new Object();
  private boolean debuggingEnabled = true;

  // For synchronus recon messages
  private long recondReadTimeout = 10000;
  private final ConcurrentMap<String, ClientReconfigurationPacket> reconResultMap
          = new ConcurrentHashMap<>(10, 0.75f, 3);
  private final Object reconMonitor = new Object();

  public RemoteQuery() throws IOException {
  }

  /**
   * A callback that notifys any waits and records the response from a replica.
   */
  private RequestCallback replicaCommandCallback = (Request response) -> {
    ClientRequest packet = (ClientRequest) response;
    replicaResultMap.put(packet.getRequestID(), packet);
    synchronized (replicaCommandMonitor) {
      replicaCommandMonitor.notifyAll();
    }
  };

  /**
   * A callback that notifys any waits and records the response from a reconfigurator.
   */
  private RequestCallback reconCallback = (Request response) -> {
    reconResultMap.put(response.getServiceName(), (ClientReconfigurationPacket) response);
    synchronized (reconMonitor) {
      reconMonitor.notifyAll();
    }
  };

  private ClientRequest waitForReplicaResponse(long id) throws GnsClientException {
    try {
      synchronized (replicaCommandMonitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (!replicaResultMap.containsKey(id)
                && (replicaReadTimeout == 0 || System.currentTimeMillis() - monitorStartTime < replicaReadTimeout)) {
          replicaCommandMonitor.wait(replicaReadTimeout);
        }
        if (replicaReadTimeout != 0 && System.currentTimeMillis() - monitorStartTime >= replicaReadTimeout) {
          throw new GnsClientException("Timeout waiting for response packet for " + id);
        }
      }
    } catch (InterruptedException x) {
      throw new GnsClientException("Wait for return packet was interrupted " + x);
    }
    return replicaResultMap.remove(id);
  }

  private ClientReconfigurationPacket waitForReconResponse(String serviceName) throws GnsClientException {
    try {
      synchronized (reconMonitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (!reconResultMap.containsKey(serviceName)
                && (recondReadTimeout == 0 || System.currentTimeMillis() - monitorStartTime < recondReadTimeout)) {
          reconMonitor.wait(recondReadTimeout);
        }
        if (recondReadTimeout != 0 && System.currentTimeMillis() - monitorStartTime >= recondReadTimeout) {
          throw new GnsClientException("Timeout waiting for response packet for " + serviceName);
        }
      }
    } catch (InterruptedException x) {
      throw new GnsClientException("Wait for return packet was interrupted " + x);
    }
    return reconResultMap.remove(serviceName);
  }

  /**
   * Sends a ClientReconfigurationPacket to a reconfigurator.
   * Returns true if the request was successful.
   *
   * @param request
   * @return true if the request was successful
   * @throws IOException
   * @throws GnsClientException
   */
  private NSResponseCode sendReconRequest(ClientReconfigurationPacket request) throws IOException, GnsClientException {
    sendRequest(request, reconCallback);
    ClientReconfigurationPacket response = waitForReconResponse(request.getServiceName());
    // FIXME: return better error codes.
    return response.isFailed() ? NSResponseCode.ERROR : NSResponseCode.NO_ERROR;
  }

  public NSResponseCode createRecord(String name, JSONObject value) {
    try {
      CreateServiceName packet = new CreateServiceName(name, value.toString());
      return sendReconRequest(packet);
    } catch (GnsClientException | IOException e) {
      GNS.getLogger().severe("Problem creating " + name + " :" + e);
      // FIXME: return better error codes.
      return NSResponseCode.ERROR;
    }
  }

  public NSResponseCode createRecordBatch(Set<String> names, Map<String, JSONObject> values,
          ClientRequestHandlerInterface handler) {
    try {
      CreateServiceName[] creates = makeBatchedCreateNameRequest(names, values, handler);
      for (CreateServiceName create : creates) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().severe("??????????????????????????? Sending recon packet for NAME = "
                  + create.getServiceName());
        }
        sendReconRequest(create);
      }
      return NSResponseCode.NO_ERROR;
    } catch (JSONException | IOException | GnsClientException e) {
      GNS.getLogger().info("Problem creating " + names + " :" + e);
      // FIXME: return better error codes.
      return NSResponseCode.ERROR;
    }
//    try {
//      CreateServiceName packet = new CreateServiceName(name, value.toString());
//      return sendReconRequest(packet);
//    } catch (GnsClientException | IOException e) {
//      GNS.getLogger().info("Problem creating " + name + " :" + e);
//      // FIXME: return better error codes.
//      return  NSResponseCode.ERROR;
//    }
  }

  // based on edu.umass.cs.reconfiguration.testing.ReconfigurableClientCreateTester but this one
  // handles multiple states
  private static CreateServiceName[] makeBatchedCreateNameRequest(Set<String> names,
          Map<String, JSONObject> states, ClientRequestHandlerInterface handler) throws JSONException {
    Collection<Set<String>> batches = ConsistentReconfigurableNodeConfig
            .splitIntoRCGroups(names, handler.getGnsNodeConfig().getReconfigurators());

    Set<CreateServiceName> creates = new HashSet<CreateServiceName>();
    // each batched create corresponds to a different RC group
    for (Set<String> batch : batches) {
      Map<String, String> nameStates = new HashMap<String, String>();
      for (String name : batch) {
        nameStates.put(name, states.get(name).toString());
      }
      // a single batched create
      creates.add(new CreateServiceName(null, nameStates));
    }
    return creates.toArray(new CreateServiceName[0]);
  }

  public NSResponseCode deleteRecord(String name) {
    try {
      DeleteServiceName packet = new DeleteServiceName(name);
      return sendReconRequest(packet);
    } catch (GnsClientException | IOException e) {
      GNS.getLogger().severe("Problem creating " + name + " :" + e);
      // FIXME: return better error codes.
      return NSResponseCode.ERROR;
    }
  }

  public String fieldRead(String guid, String field) throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
      GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field read of " + guid + "/" + field);
    }
    long requestId = fieldRead(guid, field, replicaCommandCallback);
    CommandValueReturnPacket packet = (CommandValueReturnPacket) waitForReplicaResponse(requestId);
    if (packet == null) {
      throw new GnsClientException("Packet not found in table " + requestId);
    } else {
      String returnValue = packet.getReturnValue();
      if (debuggingEnabled) {
        GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field read of " + packet.getServiceName()
                + " got from " + packet.getResponder() + " this: " + returnValue);
      }
      return checkResponse(returnValue);
    }
  }

  public String fieldReadArray(String guid, String field) throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
      GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field read array of " + guid + "/" + field);
    }
    long requestId = fieldReadArray(guid, field, replicaCommandCallback);
    CommandValueReturnPacket packet = (CommandValueReturnPacket) waitForReplicaResponse(requestId);
    if (packet == null) {
      throw new GnsClientException("Packet not found in table " + requestId);
    } else {
      String returnValue = packet.getReturnValue();
      if (debuggingEnabled) {
        GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field read array of " + packet.getServiceName()
                + " got from " + packet.getResponder() + " this: " + returnValue);
      }
      return checkResponse(returnValue);
    }
  }

  public String fieldUpdate(String guid, String field, Object value)
          throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
      GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field update " + guid + " / " + field + " = " + value);
    }
    long requestId = fieldUpdate(guid, field, value, replicaCommandCallback);
    CommandValueReturnPacket packet = (CommandValueReturnPacket) waitForReplicaResponse(requestId);
    if (packet == null) {
      throw new GnsClientException("Packet not found in table " + requestId);
    } else {
      String returnValue = packet.getReturnValue();
      if (debuggingEnabled) {
        GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field update of " + packet.getServiceName()
                + " / " + field + " got from " + packet.getResponder() + " this: " + returnValue);
      }
      return checkResponse(returnValue);
    }
  }

  public String fieldReplaceOrCreateArray(String guid, String field, ResultValue value)
          throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
      GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field fieldReplaceOrCreateArray " + guid + " / " + field + " = " + value);
    }
    long requestId = fieldReplaceOrCreateArray(guid, field, value, replicaCommandCallback);
    CommandValueReturnPacket packet = (CommandValueReturnPacket) waitForReplicaResponse(requestId);
    if (packet == null) {
      throw new GnsClientException("Packet not found in table " + requestId);
    } else {
      String returnValue = packet.getReturnValue();
      if (debuggingEnabled) {
        GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field fieldReplaceOrCreateArray of " + packet.getServiceName()
                + " / " + field + " got from " + packet.getResponder() + " this: " + returnValue);
      }
      return checkResponse(returnValue);
    }
  }
  
  public String fieldAppendToArray(String guid, String field, ResultValue value)
          throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
      GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field fieldAppendToArray " + guid + " / " + field + " = " + value);
    }
    long requestId = fieldAppendToArray(guid, field, value, replicaCommandCallback);
    CommandValueReturnPacket packet = (CommandValueReturnPacket) waitForReplicaResponse(requestId);
    if (packet == null) {
      throw new GnsClientException("Packet not found in table " + requestId);
    } else {
      String returnValue = packet.getReturnValue();
      if (debuggingEnabled) {
        GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field fieldAppendToArray of " + packet.getServiceName()
                + " / " + field + " got from " + packet.getResponder() + " this: " + returnValue);
      }
      return checkResponse(returnValue);
    }
  }

  public String fieldRemove(String guid, String field, Object value)
          throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    assert value instanceof String || value instanceof Number;
    if (debuggingEnabled) {
      GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field update " + guid + " / " + field + " = " + value);
    }
    long requestId = fieldRemove(guid, field, value, replicaCommandCallback);
    CommandValueReturnPacket packet = (CommandValueReturnPacket) waitForReplicaResponse(requestId);
    if (packet == null) {
      throw new GnsClientException("Packet not found in table " + requestId);
    } else {
      String returnValue = packet.getReturnValue();
      if (debuggingEnabled) {
        GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field update of " + packet.getServiceName()
                + " / " + field + " got from " + packet.getResponder() + " this: " + returnValue);
      }
      return checkResponse(returnValue);
    }
  }

  public String fieldRemoveMultiple(String guid, String field, ResultValue value)
          throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
      GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field fieldRemoveMultiple " + guid + " / " + field + " = " + value);
    }
    long requestId = fieldRemoveMultiple(guid, field, value, replicaCommandCallback);
    CommandValueReturnPacket packet = (CommandValueReturnPacket) waitForReplicaResponse(requestId);
    if (packet == null) {
      throw new GnsClientException("Packet not found in table " + requestId);
    } else {
      String returnValue = packet.getReturnValue();
      if (debuggingEnabled) {
        GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field fieldRemoveMultiple of " + packet.getServiceName()
                + " / " + field + " got from " + packet.getResponder() + " this: " + returnValue);
      }
      return checkResponse(returnValue);
    }
  }

  // Select commands
  public JSONArray sendSelect(SelectOperation operation, String key, Object value, Object otherValue)
          throws IOException, GnsClientException {
    SelectRequestPacket<String> packet = new SelectRequestPacket<>(-1, operation,
            SelectGroupBehavior.NONE, key, value, otherValue);
    createRecord(packet.getServiceName(), new JSONObject());
    long requestId = sendSelectPacket(packet, replicaCommandCallback);
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> responsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(responsePacket.getResponseCode())) {
      return responsePacket.getGuids();
    } else {
      return null;
    }
  }

  public JSONArray sendSelectQuery(String query) throws IOException, GnsClientException {
    SelectRequestPacket<String> packet = SelectRequestPacket.MakeQueryRequest(-1, query);
    createRecord(packet.getServiceName(), new JSONObject());
    long requestId = sendSelectPacket(packet, replicaCommandCallback);
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> reponsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(reponsePacket.getResponseCode())) {
      return reponsePacket.getGuids();
    } else {
      return null;
    }
  }

  public JSONArray sendGroupGuidSetupSelectQuery(String query, String guid, int interval)
          throws IOException, GnsClientException {
    SelectRequestPacket<String> packet = SelectRequestPacket.MakeGroupSetupRequest(-1,
            query, guid, interval);
    createRecord(packet.getServiceName(), new JSONObject());
    long requestId = sendSelectPacket(packet, replicaCommandCallback);
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> responsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(responsePacket.getResponseCode())) {
      return responsePacket.getGuids();
    } else {
      return null;
    }
  }

  public JSONArray sendGroupGuidSetupSelectQuery(String query, String guid)
          throws IOException, GnsClientException {
    return sendGroupGuidSetupSelectQuery(query, guid, DEFAULT_MIN_REFRESH_INTERVAL);
  }

  public JSONArray sendGroupGuidLookupSelectQuery(String guid) throws IOException, GnsClientException {
    SelectRequestPacket<String> packet = SelectRequestPacket.MakeGroupLookupRequest(-1, guid);
    createRecord(packet.getServiceName(), new JSONObject());
    long requestId = sendSelectPacket(packet, replicaCommandCallback);
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> responsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(responsePacket.getResponseCode())) {
      return responsePacket.getGuids();
    } else {
      return null;
    }
  }

  /**
   * Checks the response from a command request for proper syntax as well as
   * converting error responses into the appropriate thrown GNS exceptions.
   *
   * @param command
   * @param response
   * @return the result of the command
   * @throws GnsClientException
   */
  //FIXME: With somae changes we could probably some existing version of this in the client.
  private String checkResponse(String response) throws GnsClientException {
    // System.out.println("response:" + response);
    if (response.startsWith(BAD_RESPONSE)) {
      String results[] = response.split(" ");
      // System.out.println("results length:" + results.length);
      if (results.length < 2) {
        throw new GnsClientException("Invalid bad response indicator: " + response);
      } else if (results.length >= 2) {
        // System.out.println("results[0]:" + results[0]);
        // System.out.println("results[1]:" + results[1]);
        String error = results[1];
        // deal with the rest
        StringBuilder parts = new StringBuilder();
        for (int i = 2; i < results.length; i++) {
          parts.append(" ");
          parts.append(results[i]);
        }
        String rest = parts.toString();

        if (error.startsWith(BAD_SIGNATURE)) {
          throw new EncryptionException();
        }
        if (error.startsWith(BAD_GUID) || error.startsWith(BAD_ACCESSOR_GUID)
                || error.startsWith(DUPLICATE_GUID) || error.startsWith(BAD_ACCOUNT)) {
          throw new GnsInvalidGuidException(error + rest);
        }
        if (error.startsWith(DUPLICATE_FIELD)) {
          throw new GnsInvalidFieldException(error + rest);
        }
        if (error.startsWith(BAD_FIELD) || error.startsWith(FIELD_NOT_FOUND)) {
          throw new GnsFieldNotFoundException(error + rest);
        }
        if (error.startsWith(BAD_USER) || error.startsWith(DUPLICATE_USER)) {
          throw new GnsInvalidUserException(error + rest);
        }
        if (error.startsWith(BAD_GROUP) || error.startsWith(DUPLICATE_GROUP)) {
          throw new GnsInvalidGroupException(error + rest);
        }

        if (error.startsWith(ACCESS_DENIED)) {
          throw new GnsACLException(error + rest);
        }

        if (error.startsWith(DUPLICATE_NAME)) {
          throw new GnsDuplicateNameException(error + rest);
        }

        if (error.startsWith(VERIFICATION_ERROR)) {
          throw new GnsVerificationException(error + rest);
        }

        if (error.startsWith(OPERATION_NOT_SUPPORTED)) {
          throw new GnsOperationNotSupportedException(error + rest);
        }
        throw new GnsClientException("General command failure: " + error + rest);
      }
    }
    if (response.startsWith(NULL_RESPONSE)) {
      return null;
    } else {
      return response;
    }
  }

}
