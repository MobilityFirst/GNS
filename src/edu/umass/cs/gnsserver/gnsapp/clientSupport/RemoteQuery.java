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
import edu.umass.cs.gigapaxos.interfaces.RequestIdentifier;
import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.asynch.ClientAsynchBase;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.ActiveReplicaException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
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
import edu.umass.cs.utils.Config;
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

import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gnscommon.GNSProtocol;

/**
 * A synchronized version of ClientAsynchBase for sending requests to other servers.
 *
 * @author westy
 */
public class RemoteQuery extends ClientAsynchBase {
  
  private static final Logger LOGGER = Logger.getLogger(RemoteQuery.class.getName());

  // For synchronus replica messages
  private static final long REPLICA_READ_TIMEOUT = Config.getGlobalInt(GNSConfig.GNSC.REPLICA_READ_TIMEOUT);
  private static final long REPLICA_UPDATE_TIMEOUT = Config.getGlobalInt(GNSConfig.GNSC.REPLICA_UPDATE_TIMEOUT);
  private final ConcurrentMap<Long, Request> replicaResultMap = new ConcurrentHashMap<>(10, 0.75f, 3);
  // For synchronus recon messages
  private static final long RECON_TIMEOUT = Config.getGlobalInt(GNSConfig.GNSC.RECON_TIMEOUT);
  private final ConcurrentMap<String, ClientReconfigurationPacket> reconResultMap = new ConcurrentHashMap<>(10, 0.75f, 3);
  private final String myID;
  private final InetSocketAddress myAddr;

  /**
   *
   * @param myID
   * @param isa
   * @throws IOException
   */
  public RemoteQuery(String myID, InetSocketAddress isa) throws IOException {
    super();
    this.myID = myID;
    this.myAddr = isa;
    LOGGER.log(Level.INFO, "Starting RemoteQuery " + this
            + " READ_TIMEOUT = " + REPLICA_READ_TIMEOUT
            + " REPLICA_UPDATE_TIMEOUT = " + REPLICA_UPDATE_TIMEOUT
            + " RECON_TIMEOUT = " + RECON_TIMEOUT);
    assert (this.myID != null);
  }

  @Override
  public String toString() {
    return super.toString()
            + (this.myID != null ? ":" + this.myID : "");
  }

  /**
   * The RequestCallbackWithRequest.
   */
  public static interface RequestCallbackWithRequest extends RequestCallback {

    /**
     *
     * @param request
     * @return the request
     */
    public RequestCallbackWithRequest setRequest(Request request);

    /**
     *
     * @return the request
     */
    public Request getRequest();

    /**
     *
     * @return the request
     */
    public Request getResponse();
  }

  /**
   * A callback that notifys any waits and records the response from a replica.
   *
   * arun: a hack to know the damned request, not just the ID, when a timeout happens. This
   * whole RemoteQuery/ClientAsyncBase crap needs a major cleanup just like GNSClient and
   * ideally just reuse GNSClient.
   */
  private final RequestCallback replicaCommandCallback = new RequestCallback() {

    @Override
    public void handleResponse(Request response) {
      long requestId;
      if (response instanceof ActiveReplicaError) {
        requestId = ((ActiveReplicaError) response).getRequestID();
      } else if (response instanceof ClientRequest) {
        requestId = ((RequestIdentifier) response).getRequestID();
      } else {
        LOGGER.log(Level.SEVERE,
                "Bad response type: {0}", response.getClass());
        return;
      }

      replicaResultMap.put(requestId, response);
    }
  };

  private RequestCallbackWithRequest getRequestCallback(Object monitor) {
    return new RequestCallbackWithRequest() {
      Request request = null;
      Request response = null;

      @Override
      public void handleResponse(Request response) {
        this.response = response;
        replicaCommandCallback.handleResponse(response);
        synchronized (monitor) {
          monitor.notifyAll();
        }
      }

      @Override
      public RequestCallbackWithRequest setRequest(Request request) {
        this.request = request;
        return this;
      }

      @Override
      public Request getRequest() {
        return this.request;
      }

      @Override
      public Request getResponse() {
        return this.response;
      }

    };
  }

  /**
   * A callback that notifys any waits and records the response from a reconfigurator.
   */
  private final RequestCallback reconCallback = new RequestCallback() {

    @Override
    public void handleResponse(Request response) {
      reconResultMap.put(response.getServiceName(),
              (ClientReconfigurationPacket) response);
    }
  };

  private RequestCallback getReconfiguratoRequestCallback(Object monitor) {
    return new RequestCallback() {
      @Override
      public void handleResponse(Request arg0) {
        reconCallback.handleResponse(arg0);
        synchronized (monitor) {
          monitor.notifyAll();
        }
      }
    };
    // Use of lambda causes a problem in Android.
//    return (Request arg0) -> {
//      reconCallback.handleResponse(arg0);
//      synchronized (monitor) {
//        monitor.notifyAll();
//      }
//    };
  }

  private ClientRequest waitForReplicaResponse(long id, Object monitor, RequestCallbackWithRequest callback)
          throws ClientException, ActiveReplicaException {
    return waitForReplicaResponse(id, monitor, callback, REPLICA_READ_TIMEOUT);
  }

  private ClientRequest waitForReplicaResponse(long id, Object monitor, RequestCallbackWithRequest callback, long timeout)
          throws ClientException, ActiveReplicaException {
    try {
      synchronized (monitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (!replicaResultMap.containsKey(id) && (callback == null || callback.getResponse() == null)
                && (timeout == 0 || System.currentTimeMillis()
                - monitorStartTime < timeout)) {
          LOGGER.log(Level.FINE, "{0} waiting for id {1} with a timeout of {2}ms",
                  new Object[]{this, id, timeout});
          monitor.wait(WAIT_TIMESTEP);
        }
        if (timeout != 0
                && System.currentTimeMillis() - monitorStartTime >= timeout) {
          // TODO: arun
        	ClientException e = new ClientException(
        			ResponseCode.TIMEOUT,
        			this
        			+ ": Timed out on active replica response after waiting for "
        			+ timeout
        			+ "ms for response packet for response for "
        			+ (callback != null
        			&& callback.getRequest() != null ? callback
        					.getRequest().getSummary() : id));
          LOGGER.log(Level.WARNING, "\n\n\n\n{0}", e.getMessage());
          e.printStackTrace();
          throw e;
        } else {
          LOGGER.log(Level.FINE,
                  "{0} successfully completed remote query {1}",
                  new Object[]{this, id});
        }
      }
    } catch (InterruptedException x) {
      throw new ClientException("Wait for return packet was interrupted " + x);
    }
    Request response = replicaResultMap.remove(id);
    assert (!(response instanceof CommandPacket) || response.equals(callback != null ? callback.getResponse() : null));

    if (response instanceof ActiveReplicaError) {
      throw new ActiveReplicaException("ActiveReplicaException incurred for "
              + response.getServiceName() + " " + (callback != null ? callback.getRequest() : ""));
    } else if (response instanceof ClientRequest) {
      return (ClientRequest) response;
    } else { // shouldn't ever get here because callback can't find a request id
      throw new ClientException("Bad response type: " + response.getClass());
    }
  }

  private ClientReconfigurationPacket waitForReconResponse(Request request, Object monitor) throws ClientException {
    return waitForReconResponse(request, monitor, RECON_TIMEOUT);
  }

  private static final long WAIT_TIMESTEP = 1000;

  private ClientReconfigurationPacket waitForReconResponse(Request request, Object monitor, long timeout)
          throws ClientException {
    try {
      synchronized (monitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (!reconResultMap.containsKey(request.getServiceName())
                && (timeout == 0 || System.currentTimeMillis() - monitorStartTime < timeout)) {
          LOGGER.log(Level.FINE,
                  "{0} waiting for next time step for request {1}",
                  new Object[]{this, request.getSummary()});
          monitor.wait(WAIT_TIMESTEP);
        }
        if (timeout != 0 && System.currentTimeMillis() - monitorStartTime >= timeout) {
          ClientException e = new ClientException(ResponseCode.TIMEOUT,
                  this
                  + ": Timed out on reconfigurator response after waiting for "
                  + timeout + "ms for response packet for "
                  + request.getSummary());
          LOGGER.log(Level.WARNING, "\n\n\n\n{0}", e.getMessage());
          e.printStackTrace();
          throw e;
        }
      }
    } catch (InterruptedException x) {
      throw new ClientException("Wait for return packet was interrupted " + x);
    }
    return reconResultMap.remove(request.getServiceName());
  }

  /**
   * Sends a ClientReconfigurationPacket to a reconfigurator.
   * Returns true if the request was successful.
   *
   * @param request
   * @return true if the request was successful
   * @throws IOException
   * @throws ClientException
   */
  private ResponseCode sendReconRequest(ClientReconfigurationPacket request) throws IOException, ClientException {
    return sendReconRequest(request, RECON_TIMEOUT);
  }

  /**
   * Sends a ClientReconfigurationPacket to a reconfigurator.
   * Returns true if the request was successful.
   *
   * @param request
   * @return true if the request was successful
   * @throws IOException
   * @throws ClientException
   */
  private ResponseCode sendReconRequest(ClientReconfigurationPacket request, long timeout) throws IOException, ClientException {
    Object monitor = new Object();
    sendRequest(request, this.getReconfiguratoRequestCallback(monitor));
    ClientReconfigurationPacket response = waitForReconResponse(request, monitor, timeout);
    // FIXME: return better error codes.
    if (response.isFailed()) {
      // arun: return duplicate error if name already exists
      return (response instanceof CreateServiceName
              && response.getResponseCode() == ClientReconfigurationPacket.ResponseCodes.DUPLICATE_ERROR
                      ? ResponseCode.DUPLICATE_ID_EXCEPTION
                      : // else generic error
                      ResponseCode.UNSPECIFIED_ERROR).setMessage(response.getResponseMessage());
    } else {
      return ResponseCode.NO_ERROR;
    }
  }

  /**
   * Creates a record at an appropriate reconfigurator.
   *
   * @param name
   * @param value
   * @return a NSResponseCode
   * @throws ClientException
   */
  public ResponseCode createRecord(String name, JSONObject value) throws ClientException {
    try {
      CreateServiceName packet = new CreateServiceName(name, value.toString());
      return sendReconRequest(packet);
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "Problem creating {0} :{1}",
              new Object[]{name, e});
      throw new ClientException(ResponseCode.UNSPECIFIED_ERROR, e.getMessage());
    }
  }

  // Ballpark number of request we can do per second on a slow machine
  // Fixme: Could make this a config parameter.
  private static final long REQUESTS_PER_SECOND = 50;

  /**
   * Creates multiple records at the appropriate reconfigurators.
   *
   * @param names
   * @param values
   * @param handler
   * @return a NSResponseCode
   */
  public ResponseCode createRecordBatch(Set<String> names, Map<String, JSONObject> values,
          ClientRequestHandlerInterface handler) {
    try {
      CreateServiceName[] creates = makeBatchedCreateNameRequest(names, values, handler);
      for (CreateServiceName create : creates) {
        LOGGER.log(Level.FINE,
                "{0} sending create for NAME = ",
                new Object[]{this, create.getServiceName()});
        // Make a timeout that somewhat reflects the amount of work we're going to do.
        long timeout = Math.max(RECON_TIMEOUT, (create.getNameStates().size() / REQUESTS_PER_SECOND) * 1000);
        sendReconRequest(create, timeout);
      }
      return ResponseCode.NO_ERROR;
    } catch (JSONException | IOException | ClientException e) {
      LOGGER.log(Level.FINE, "Problem creating {0} :{1}",
              new Object[]{names, e});
      // FIXME: return better error codes.
      return ResponseCode.UNSPECIFIED_ERROR;
    }
  }

  /**
   * Based on edu.umass.cs.reconfiguration.testing.ReconfigurableClientCreateTester
   * but this one handles multiple states.
   *
   * @param names
   * @param states
   * @param handler
   * @return an array of packets
   * @throws JSONException
   */
  private static CreateServiceName[] makeBatchedCreateNameRequest(Set<String> names,
          Map<String, JSONObject> states, ClientRequestHandlerInterface handler) throws JSONException {
    Collection<Set<String>> batches = ConsistentReconfigurableNodeConfig
            .splitIntoRCGroups(names, handler.getGnsNodeConfig().getReconfigurators());

    Set<CreateServiceName> creates = new HashSet<>();
    // each batched create corresponds to a different RC group
    for (Set<String> batch : batches) {
      Map<String, String> nameStates = new HashMap<>();
      for (String name : batch) {
        nameStates.put(name, states.get(name).toString());
      }
      // a single batched create
      creates.add(new CreateServiceName(null, nameStates));
    }
    return creates.toArray(new CreateServiceName[0]);
  }

  /**
   * Deletes a record at the appropriate reconfigurators.
   *
   * @param name
   * @return ResponseCode
   * @throws ClientException
   */
  public ResponseCode deleteRecord(String name) throws ClientException {
    try {
      DeleteServiceName packet = new DeleteServiceName(name);
      return sendReconRequest(packet);
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "Problem creating {0} :{1}",
              new Object[]{name, e});
      throw new ClientException(ResponseCode.UNSPECIFIED_ERROR, e.getMessage());
    }
  }

  /**
   * @param name
   * @return GNSResponse code
   * @throws ClientException
   */
  public ResponseCode deleteRecordSuppressExceptions(String name)
          throws ClientException {
    try {
      DeleteServiceName packet = new DeleteServiceName(name);
      return sendReconRequest(packet);
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE,
              "Problem creating {0} :{1}", new Object[]{name, e});
      return ResponseCode.UNSPECIFIED_ERROR.setMessage(e.getMessage());
    } catch (ClientException ce) {
      return ce.getCode().setMessage(ce.getMessage());
    }
  }

  /**
   * Handles the reponse from some query with a default timeout period.
   *
   * @param requestId
   * @param monitor
   * @param notFoundReponse
   * @return the response from the query
   * @throws ClientException
   */
  private String handleQueryResponse(long requestId, Object monitor, String notFoundReponse) throws ClientException {
    return handleQueryResponse(requestId, monitor, null, REPLICA_READ_TIMEOUT, notFoundReponse);
  }

  private String handleQueryResponse(long requestId, Object monitor,
          RequestCallbackWithRequest callback, String notFoundReponse) throws ClientException {
    return handleQueryResponse(requestId, monitor, callback, REPLICA_READ_TIMEOUT, notFoundReponse);
  }

  /**
   * Handles the response from some query with a specified timeout period.
   *
   * @param requestId
   * @param monitor
   * @param timeout
   * @param notFoundReponse
   * @return the response from the query
   * @throws ClientException
   */
  private String handleQueryResponse(long requestId, Object monitor, RequestCallbackWithRequest callback,
          long timeout, String notFoundReponse) throws ClientException {
    try {
      ResponsePacket packet
              = (ResponsePacket) waitForReplicaResponse(requestId, monitor, callback, timeout);
      if (packet == null) {
        throw new ClientException("Packet not found in table " + requestId);
      } else {
        LOGGER.log(Level.FINE,
                "{0} received {1}", new Object[]{this, packet.getSummary()});
        return CommandUtils.checkResponse(packet, ((CommandPacket) callback.getRequest())).getReturnValue();
      }
    } catch (ActiveReplicaException e) {
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
   * @throws ClientException
   */
  public String fieldRead(String guid, String field) throws IOException, JSONException, ClientException {
    LOGGER.log(Level.FINE,
            "{0} Field read of {1} : {2}",
            new Object[]{this, guid, Util.truncate(field, 16, 16)});
    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = fieldRead(guid, field, callback = this.getRequestCallback(monitor));
    return handleQueryResponse(requestId, monitor, callback, null);
  }

  private static final String EMPTY_JSON_ARRAY_STRING = new JSONArray().toString();

  /**
   * Lookup the field that is an array on another server.
   *
   * @param guid
   * @param field
   * @return the value of the field as a JSON array string
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public String fieldReadArray(String guid, String field) throws IOException, JSONException, ClientException {

    LOGGER.log(Level.FINE,
            "{0} Field read array of {1} : {2}",
            new Object[]{this, guid, field});
    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = fieldReadArray(guid, field, callback = this.getRequestCallback(monitor));
    return handleQueryResponse(requestId, monitor, callback, EMPTY_JSON_ARRAY_STRING);
  }

  /**
   * Updates a field at a remote replica.
   *
   * @param guid
   * @param field
   * @param value
   * @return the response to the query
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public String fieldUpdate(String guid, String field, Object value)
          throws IOException, JSONException, ClientException {

    LOGGER.log(Level.FINE,
            "{0} Field update {1} / {2} : {3}",
            new Object[]{this, guid, field, value});
    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = fieldUpdate(guid, field, value, callback = this.getRequestCallback(monitor));
    return handleQueryResponse(requestId, monitor, callback, REPLICA_UPDATE_TIMEOUT,
            GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + guid);
  }

  /**
   * Updates or creates a field that is an array at a remote replica.
   *
   * @param guid
   * @param field
   * @param value
   * @return the response to the query
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public String fieldReplaceOrCreateArray(String guid, String field, ResultValue value)
          throws IOException, JSONException, ClientException {

    LOGGER.log(Level.FINE,
            "{0} Field fieldReplaceOrCreateArray {1} / {2} : {3}",
            new Object[]{this, guid, field, value});
    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = fieldReplaceOrCreateArray(guid, field, value, callback = this.getRequestCallback(monitor));
    return handleQueryResponse(requestId, monitor, callback, REPLICA_UPDATE_TIMEOUT,
            GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + guid);
  }

  /**
   * Appends a value to a field that is an array at a remote replica.
   *
   * @param guid
   * @param field
   * @param value
   * @return the response to the query
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public String fieldAppendToArray(String guid, String field, ResultValue value)
          throws IOException, JSONException, ClientException {
    GNSConfig.getLogger().log(Level.FINE,
            "{0} Field fieldAppendToArray {1} / {2} : {3}",
            new Object[]{this, guid, field, Util.truncate(value, 64, 64)});
    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = fieldAppendToArray(guid, field, value, callback = this.getRequestCallback(monitor));
    return handleQueryResponse(requestId, monitor, callback, REPLICA_UPDATE_TIMEOUT,
            GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + guid);
  }

  /**
   * Removes a value from a field that is an array at a remote replica.
   *
   * @param guid
   * @param field
   * @param value
   * @return the response to the query
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public String fieldRemove(String guid, String field, Object value)
          throws IOException, JSONException, ClientException {
    assert value instanceof String || value instanceof Number;
    LOGGER.log(Level.FINE,
            "{0} Field remove {1} / {2} : {3}",
            new Object[]{this, guid, field, value});
    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = fieldRemove(guid, field, value, callback = this.getRequestCallback(monitor));
    return handleQueryResponse(requestId, monitor, callback,
            REPLICA_UPDATE_TIMEOUT, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + guid);
  }

  /**
   * Removes all the values given from a field that is an array at a remote replica.
   *
   * @param guid
   * @param field
   * @param value
   * @return the response to the query
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public String fieldRemoveMultiple(String guid, String field, ResultValue value)
          throws IOException, JSONException, ClientException {
    GNSConfig.getLogger().log(Level.FINE,
            "{0} Field fieldRemoveMultiple {1} / {2} = {3}",
            new Object[]{this, guid, field, value});
    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = fieldRemoveMultiple(guid, field, value, callback = this.getRequestCallback(monitor));
    return handleQueryResponse(requestId, monitor, callback, REPLICA_UPDATE_TIMEOUT,
            GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + guid);
  }

  /**
   * Sends a select command to the remote replica.
   *
   * @param operation
   * @param key
   * @param value
   * @param otherValue
   * @return a JSONArray of guids that match the select query
   * @throws IOException
   * @throws ClientException
   */
  @Deprecated
  public JSONArray sendSelect(SelectOperation operation, String key, Object value, Object otherValue)
          throws IOException, ClientException {
    SelectRequestPacket<String> packet = new SelectRequestPacket<>(-1, operation,
            SelectGroupBehavior.NONE, key, value, otherValue);
    try {
      createRecord(packet.getServiceName(), new JSONObject());
    } catch (Exception e) {
      GNSConfig.getLogger().log(Level.WARNING, "{0} incurred name creation exception {1}", new Object[]{this, e});
    }

    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = sendSelectPacket(packet, callback = this.getRequestCallback(monitor));
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> responsePacket
            = (SelectResponsePacket<String>) waitForReplicaResponse(requestId, monitor, callback);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(responsePacket.getResponseCode())) {
      return responsePacket.getGuids();
    } else {
      return null;
    }
  }

  /**
   * Sends a select query to the remote replica.
   *
   * @param query
   * @return a JSONArray of guids that match the select query
   * @throws IOException
   * @throws ClientException
   */
  @Deprecated
  public JSONArray sendSelectQuery(String query) throws IOException, ClientException {
    SelectRequestPacket<String> packet = SelectRequestPacket.MakeQueryRequest(-1, query);
    try {
      createRecord(packet.getServiceName(), new JSONObject());
    } catch (Exception e) {
      GNSConfig.getLogger().log(Level.WARNING, "{0} incurred name creation exception {1}", new Object[]{this, e});
    }

    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = sendSelectPacket(packet, callback = this.getRequestCallback(monitor).setRequest(packet));
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> reponsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId, monitor, callback);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(reponsePacket.getResponseCode())) {
      return reponsePacket.getGuids();
    } else {
      return null;
    }
  }

  /**
   * Sends a setup select query to the remote replica with a specified refresh interval.
   *
   * @param query
   * @param guid
   * @param interval
   * @return a JSONArray of guids that match the select query
   * @throws IOException
   * @throws ClientException
   */
  @Deprecated
  public JSONArray sendGroupGuidSetupSelectQuery(String query, String guid, int interval)
          throws IOException, ClientException {
    SelectRequestPacket<String> packet = SelectRequestPacket.MakeGroupSetupRequest(-1,
            query, guid, interval);
    try {
      createRecord(packet.getServiceName(), new JSONObject());
    } catch (Exception e) {
      GNSConfig.getLogger().log(Level.WARNING, "{0} incurred name creation exception {1}", new Object[]{this, e});
    }

    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = sendSelectPacket(packet, callback = this.getRequestCallback(monitor));
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> responsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId, monitor, callback);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(responsePacket.getResponseCode())) {
      return responsePacket.getGuids();
    } else {
      return null;
    }
  }

  /**
   * Sends a setup select query to the remote replica with the default refresh interval.
   *
   * @param query
   * @param guid
   * @return a JSONArray of guids that match the select query
   * @throws IOException
   * @throws ClientException
   */
  @Deprecated
  public JSONArray sendGroupGuidSetupSelectQuery(String query, String guid)
          throws IOException, ClientException {
    return sendGroupGuidSetupSelectQuery(query, guid, ClientAsynchBase.DEFAULT_MIN_REFRESH_INTERVAL_FOR_SELECT);
  }

  /**
   * Sends a lookup select query to the remote replica.
   *
   * @param guid
   * @return a JSONArray of guids that match the original select query
   * @throws IOException
   * @throws ClientException
   */
  @Deprecated
  public JSONArray sendGroupGuidLookupSelectQuery(String guid) throws IOException, ClientException {
    SelectRequestPacket<String> packet = SelectRequestPacket.MakeGroupLookupRequest(-1, guid);
    try {
      createRecord(packet.getServiceName(), new JSONObject());
    } catch (Exception e) {
      GNSConfig.getLogger().log(Level.WARNING, "{0} incurred name creation exception {1}", new Object[]{this, e});
    }
    Object monitor = new Object();
    RequestCallbackWithRequest callback;
    long requestId = sendSelectPacket(packet, callback = this.getRequestCallback(monitor));
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> responsePacket = (SelectResponsePacket<String>) waitForReplicaResponse(requestId, monitor, callback);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(responsePacket.getResponseCode())) {
      return responsePacket.getGuids();
    } else {
      return null;
    }
  }
}
