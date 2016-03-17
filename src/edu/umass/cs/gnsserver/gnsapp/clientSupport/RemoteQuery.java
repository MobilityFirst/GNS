/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

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
import edu.umass.cs.gnscommon.exceptions.client.GnsActiveReplicaException;
import edu.umass.cs.gnscommon.exceptions.client.GnsOperationNotSupportedException;
import edu.umass.cs.gnsserver.gnsapp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectGroupBehavior;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectOperation;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

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
  private static final long defaultReplicaReadTimeout = 5000;
  private static final long defaultReplicaUpdateTimeout = 8000;
  private final ConcurrentMap<Long, Request> replicaResultMap
          = new ConcurrentHashMap<>(10, 0.75f, 3);
  //private final Object replicaCommandMonitor = new Object();
  private boolean debuggingEnabled = true;

  // For synchronus recon messages
  private static final long defaultReconTimeout = 4000;
  private final ConcurrentMap<String, ClientReconfigurationPacket> reconResultMap
          = new ConcurrentHashMap<>(10, 0.75f, 3);
  //private final Object reconMonitor = new Object();
  private final String myID;
  private final InetSocketAddress myAddr;

  public RemoteQuery() throws IOException {
	  this(null,null);
  }
  public RemoteQuery(String myID, InetSocketAddress isa) throws IOException {
	  super();
	  this.myID=myID;
	  this.myAddr = isa;
	  GNSConfig.getLogger().info("Starting RemoteQuery " + this);
	  assert(this.myID!=null);
  }
  
	public String toString() {
		return super.toString()
				+ (this.myID != null ? ":" + this.myID : "");
	}

  /**
   * A callback that notifys any waits and records the response from a replica.
   */
  private RequestCallback replicaCommandCallback = (Request response) -> {

    long requestId;
    if (response instanceof ActiveReplicaError) {
      requestId = ((ActiveReplicaError) response).getRequestID();
    } else if (response instanceof ClientRequest) {
      requestId = ((ClientRequest) response).getRequestID();
    } else {
      GNSConfig.getLogger().severe("Bad response type: " + response.getClass());
      return;
    }

    replicaResultMap.put(requestId, response);
    //synchronized (replicaCommandMonitor) {
      //replicaCommandMonitor.notifyAll();
    //}
  };
  
  private RequestCallback getRequestCallback(Object monitor) {
	  return new RequestCallback() {

		@Override
		public void handleResponse(Request response) {
			replicaCommandCallback.handleResponse(response);
			synchronized(monitor) {
				monitor.notifyAll();
			}
		}
	  };
  }

  /**
   * A callback that notifys any waits and records the response from a reconfigurator.
   */
  private RequestCallback reconCallback = (Request response) -> {
    reconResultMap.put(response.getServiceName(), (ClientReconfigurationPacket) response);
    //synchronized (reconMonitor) {
      //reconMonitor.notifyAll();
    //}
  };
  
  private RequestCallback getReconfiguratoRequestCallback(Object monitor) {
	  return new RequestCallback() {

		@Override
		public void handleResponse(Request arg0) {
			reconCallback.handleResponse(arg0);
			synchronized(monitor) {
				monitor.notifyAll();
			}
		}
		  
	  };
  }

  private ClientRequest waitForReplicaResponse(long id, Object monitor)
          throws GnsClientException, GnsActiveReplicaException {
    return waitForReplicaResponse(id, monitor, defaultReplicaReadTimeout);
  }

  private ClientRequest waitForReplicaResponse(long id, Object monitor, long timeout)
          throws GnsClientException, GnsActiveReplicaException {
    try {
			synchronized (monitor) {
				long monitorStartTime = System.currentTimeMillis();
				while (!replicaResultMap.containsKey(id)
						&& (timeout == 0 || System.currentTimeMillis()
								- monitorStartTime < timeout)) {
					GNSConfig.getLogger().log(Level.FINE, "{0} waiting for id {1} with a timeout of {2}",
							new Object[] { this, id+"", timeout+"ms" });
					monitor.wait(1000);
				}
				if (timeout != 0
						&& System.currentTimeMillis() - monitorStartTime >= timeout) {
					// TODO: arun
					GnsClientException e = new GnsClientException(
							this +": Timed out on active replica response after waiting for " + timeout + "ms for response packet for " + id);
					GNSConfig.getLogger().warning("\n\n\n\n"  + e.getMessage());
					e.printStackTrace();
					throw e;
				} else
					GNSConfig
							.getLogger()
							.log(Level.INFO,
									"{0} successfully completed remote query {1}",
									new Object[] { this, id + "" });
			}
    } catch (InterruptedException x) {
      throw new GnsClientException("Wait for return packet was interrupted " + x);
    }
    Request response = replicaResultMap.remove(id);
    if (response instanceof ActiveReplicaError) {
      throw new GnsActiveReplicaException("No replicas found for " + ((ActiveReplicaError) response).getServiceName());
    } else if (response instanceof ClientRequest) {
      return (ClientRequest) response;
    } else { // shouldn't ever get here because callback can't find a request id
      throw new GnsClientException("Bad response type: " + response.getClass());
    }
  }

  private ClientReconfigurationPacket waitForReconResponse(String serviceName, Object monitor) throws GnsClientException {
    return waitForReconResponse(serviceName, monitor, defaultReconTimeout);
  }

  private ClientReconfigurationPacket waitForReconResponse(String serviceName, Object monitor, long timeout)
          throws GnsClientException {
    try {
      synchronized (monitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (!reconResultMap.containsKey(serviceName)
                && (timeout == 0 || System.currentTimeMillis() - monitorStartTime < timeout)) {
          monitor.wait(timeout);
        }
        if (timeout != 0 && System.currentTimeMillis() - monitorStartTime >= timeout) {
					GnsClientException e = new GnsClientException(
							this
									+ ": Timed out on reconfigurator response after waiting for "
									+ timeout + "ms response packet for "
									+ serviceName);
        	GNSConfig.getLogger().warning("\n\n\n\n" + e.getMessage());
        	e.printStackTrace();
        	throw e;
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
	  Object monitor = new Object();
    sendRequest(request, this.getReconfiguratoRequestCallback(monitor));//reconCallback);
    ClientReconfigurationPacket response = waitForReconResponse(request.getServiceName(), monitor);
    // FIXME: return better error codes.
    return response.isFailed() ? NSResponseCode.ERROR : NSResponseCode.NO_ERROR;
  }

  public NSResponseCode createRecord(String name, JSONObject value) {
    try {
      CreateServiceName packet = new CreateServiceName(name, value.toString());
      return sendReconRequest(packet);
    } catch (GnsClientException | IOException e) {
      GNSConfig.getLogger().severe("Problem creating " + name + " :" + e);
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
					GNSConfig
							.getLogger()
							.log(Level.INFO,
									"{0} sending create for NAME = ",
									new Object[] {this, create.getServiceName() });
        }
        sendReconRequest(create);
      }
      return NSResponseCode.NO_ERROR;
    } catch (JSONException | IOException | GnsClientException e) {
      GNSConfig.getLogger().info("Problem creating " + names + " :" + e);
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
      GNSConfig.getLogger().severe("Problem creating " + name + " :" + e);
      // FIXME: return better error codes.
      return NSResponseCode.ERROR;
    }
  }

  private String handleQueryResponse(long requestId, Object monitor, String notFoundReponse) throws GnsClientException {
    return handleQueryResponse(requestId, monitor, defaultReplicaReadTimeout, notFoundReponse);
  }

  private String handleQueryResponse(long requestId, Object monitor, long timeout, String notFoundReponse) throws GnsClientException {
    try {
      CommandValueReturnPacket packet = (CommandValueReturnPacket) waitForReplicaResponse(requestId, monitor, timeout);
      if (packet == null) {
        throw new GnsClientException("Packet not found in table " + requestId);
      } else {
        String returnValue = packet.getReturnValue();
        if (debuggingEnabled) {
					GNSConfig.getLogger().log(
							Level.INFO,
							"{0} {1} {2} got from {3} this: {4}",
							new Object[] { this, packet.getServiceName(),
									packet.getResponder(), Util.truncate(returnValue,16,16) });
        }
        // FIX ME: Tidy up all these error reponses for updates
        return checkResponse(returnValue);
      }
    } catch (GnsActiveReplicaException e) {
      return notFoundReponse;
    }
  }

  /**
   * Lookup the field on another server.
   * Will return null if it doesn't exist.
   *
   * @param guid
   * @param field
   * @return the value of the field as a string
   * @throws IOException
   * @throws JSONException
   * @throws GnsClientException
   */
  public String fieldRead(String guid, String field) throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
			GNSConfig.getLogger().log(Level.INFO,
					"{0} Field read of {1} / {2}",
					new Object[] { this, guid, Util.truncate(field, 16, 16) });
    }
    Object monitor = new Object();
    long requestId = fieldRead(guid, field, this.getRequestCallback(monitor));//replicaCommandCallback);
    return handleQueryResponse(requestId, monitor, null);
  }

  private static final String emptyJSONArrayString = new JSONArray().toString();

  /**
   * Lookup the field on another server.
   *
   * @param guid
   * @param field
   * @return the value of the field as a JSON array string
   * @throws IOException
   * @throws JSONException
   * @throws GnsClientException
   */
  public String fieldReadArray(String guid, String field) throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
			GNSConfig.getLogger().log(Level.INFO,
					"{0} Field read array of {1} / {2}",
					new Object[] {this, guid, field });
    }
    Object monitor = new Object();
    long requestId = fieldReadArray(guid, field, this.getRequestCallback(monitor));//replicaCommandCallback);
    return handleQueryResponse(requestId, monitor, emptyJSONArrayString);
  }

  public String fieldUpdate(String guid, String field, Object value)
          throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
			GNSConfig.getLogger().log(Level.FINE,
					"{0} Field update {1} / {2} = {3}",
					new Object[] { this, guid, field, value });
    }
    Object monitor = new Object();
    long requestId = fieldUpdate(guid, field, value, this.getRequestCallback(monitor));//replicaCommandCallback);
    if (debuggingEnabled) {
			GNSConfig.getLogger().log(Level.INFO,
					"{0} Field update {1} / {2} {3} = {4}",
					new Object[] {this, guid, field, requestId, Util.truncate(value,16,16) });
      }
    return handleQueryResponse(requestId, monitor, defaultReplicaUpdateTimeout, BAD_RESPONSE + " " + BAD_GUID + " " + guid + " " + BAD_GUID + " " + guid);
  }

  public String fieldReplaceOrCreateArray(String guid, String field, ResultValue value)
          throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
      GNSConfig.getLogger().log(Level.INFO, "{0} Field fieldReplaceOrCreateArray {1} / {2} = {3}",
    		  new Object[]{this, guid,field,value});
    }
    Object monitor = new Object();
    long requestId = fieldReplaceOrCreateArray(guid, field, value, this.getRequestCallback(monitor));//replicaCommandCallback);
    return handleQueryResponse(requestId, monitor, defaultReplicaUpdateTimeout, BAD_RESPONSE + " " + BAD_GUID + " " + guid);
  }

  public String fieldAppendToArray(String guid, String field, ResultValue value)
          throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
			GNSConfig
					.getLogger()
					.log(Level.INFO,
							"{0} Field fieldAppendToArray {1} / {2} = {3}",
							new Object[] {this, guid, field, Util.truncate(value, 64, 64) });
    }
    Object monitor = new Object();
    long requestId = fieldAppendToArray(guid, field, value, this.getRequestCallback(monitor));//replicaCommandCallback);
    return handleQueryResponse(requestId, monitor, defaultReplicaUpdateTimeout, BAD_RESPONSE + " " + BAD_GUID + " " + guid);
  }

  public String fieldRemove(String guid, String field, Object value)
          throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    assert value instanceof String || value instanceof Number;
    if (debuggingEnabled) {
			GNSConfig.getLogger().log(Level.INFO,
					"{0} Field update {1} / {2} = {3}",
					new Object[] {this, guid, field, Util.truncate(value, 16, 16) });
    }
    Object monitor = new Object();
    long requestId = fieldRemove(guid, field, value, this.getRequestCallback(monitor));//replicaCommandCallback);
    return handleQueryResponse(requestId, monitor, defaultReplicaUpdateTimeout, BAD_RESPONSE + " " + BAD_GUID + " " + guid);
  }

  public String fieldRemoveMultiple(String guid, String field, ResultValue value)
          throws IOException, JSONException, GnsClientException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    if (debuggingEnabled) {
			GNSConfig
					.getLogger()
					.log(Level.INFO,
							"{0} Field fieldRemoveMultiple {1} / {2} = {3}",
							new Object[] {this, guid, field, value });
    }
    Object monitor = new Object();
    long requestId = fieldRemoveMultiple(guid, field, value, this.getRequestCallback(monitor));//replicaCommandCallback);
    return handleQueryResponse(requestId, monitor, defaultReplicaUpdateTimeout, BAD_RESPONSE + " " + BAD_GUID + " " + guid);
  }

  // Select commands
  public JSONArray sendSelect(SelectOperation operation, String key, Object value, Object otherValue)
          throws IOException, GnsClientException {
    SelectRequestPacket<String> packet = new SelectRequestPacket<>(-1, operation,
            SelectGroupBehavior.NONE, key, value, otherValue);
    createRecord(packet.getServiceName(), new JSONObject());
    Object monitor = new Object();
    long requestId = sendSelectPacket(packet, this.getRequestCallback(monitor));//replicaCommandCallback);
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> responsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId, monitor);
    if (ResponseCode.NOERROR.equals(responsePacket.getResponseCode())) {
      return responsePacket.getGuids();
    } else {
      return null;
    }
  }

  public JSONArray sendSelectQuery(String query) throws IOException, GnsClientException {
    SelectRequestPacket<String> packet = SelectRequestPacket.MakeQueryRequest(-1, query);
    createRecord(packet.getServiceName(), new JSONObject());
    Object monitor = new Object();
    long requestId = sendSelectPacket(packet, this.getRequestCallback(monitor));//replicaCommandCallback);
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> reponsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId, monitor);
    if (ResponseCode.NOERROR.equals(reponsePacket.getResponseCode())) {
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
    Object monitor = new Object();
    long requestId = sendSelectPacket(packet, this.getRequestCallback(monitor));//replicaCommandCallback);
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> responsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId, monitor);
    if (ResponseCode.NOERROR.equals(responsePacket.getResponseCode())) {
      return responsePacket.getGuids();
    } else {
      return null;
    }
  }

  public JSONArray sendGroupGuidSetupSelectQuery(String query, String guid)
          throws IOException, GnsClientException {
    return sendGroupGuidSetupSelectQuery(query, guid, ClientAsynchBase.DEFAULT_MIN_REFRESH_INTERVAL);
  }

  public JSONArray sendGroupGuidLookupSelectQuery(String guid) throws IOException, GnsClientException {
    SelectRequestPacket<String> packet = SelectRequestPacket.MakeGroupLookupRequest(-1, guid);
    createRecord(packet.getServiceName(), new JSONObject());
    Object monitor = new Object();
    long requestId = sendSelectPacket(packet, this.getRequestCallback(monitor));//replicaCommandCallback);
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> responsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId, monitor);
    if (ResponseCode.NOERROR.equals(responsePacket.getResponseCode())) {
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
