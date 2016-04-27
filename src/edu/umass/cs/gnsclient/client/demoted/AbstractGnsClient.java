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
 *  Initial developer(s): Westy, arun, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.demoted;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONException;
import org.json.JSONObject;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import static edu.umass.cs.gnsclient.client.CommandUtils.*;
import edu.umass.cs.gnsclient.client.android.AndroidNIOTask;
import edu.umass.cs.gnsclient.client.CommandResult;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandValueReturnPacket;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.gnsclient.client.util.Util;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import static edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES.CLEAR;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandType;
import edu.umass.cs.utils.Config;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SignatureException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

/**
 * This class defines a basic client to communicate with a GNS instance over TCP.
 *
 * For a more complete set of commands see also {@link UniversalTcpClient} and {@link UniversalTcpClientExtended}.
 *
 */
public class AbstractGnsClient {

  // FOR NEW CLIENT
  // initialized from properties file
  private static final Set<InetSocketAddress> STATIC_RECONFIGURATOR = ReconfigurationConfig
          .getReconfiguratorAddresses();

  // initialized upon contsruction
  private Set<InetSocketAddress> reconfigurators;
  private AsyncClient asyncClient;

  // END OF FOR NEW CLIENT
  /**
   * Indicates whether we are on an Android platform or not
   */
  public static final boolean IS_ANDROID = System.getProperty("java.vm.name")
          .equalsIgnoreCase("Dalvik");

  /**
   * A string representing the GNS Server that we are connecting to.
   * NOTE THAT THIS STRING SHOULD BE DIFFERENT FOR DIFFERENT SERVERS (say
   * a local test server vs the one on EC2 otherwise the key pair storage
   * code overwrite keys with the same name that are being used for
   * different servers.
   */
  private final String GNSInstance;

  /**
   * The length of time we will wait for a command response from the server
   * before giving up.
   */
  // FIXME: We might need a separate timeout just for certain ops like 
  // gui creation that sometimes take a while
  // 10 seconds is too short on EC2 
  private int readTimeout = 20000; // 20 seconds... was 40 seconds

  /* Keeps track of requests that are sent out and the reponses to them */
  private final ConcurrentMap<Long, CommandResult> resultMap = new ConcurrentHashMap<>(
          10, 0.75f, 3);
  /* Instrumentation: Keeps track of transmission start times */
  private final ConcurrentMap<Long, Long> queryTimeStamp = new ConcurrentHashMap<>(10,
          0.75f, 3);
  /* Used to generate unique ids */
  private final Random randomID = new Random();
  /* Used by the wait/notify calls */
  private final Object monitor = new Object();

  // instrumentation
  private double movingAvgLatency;
  private int totalAsynchErrors;

  private boolean forceCoordinatedReads = false;

  /**
   * Creates a new client for communication with the GNS.
   *
   * @param anyReconfigurator
   * @param disableSSL
   * @throws IOException
   */
  public AbstractGnsClient(InetSocketAddress anyReconfigurator, boolean disableSSL)
          throws IOException {
    // First we initialize some of the old stuff
    this(disableSSL);
    // Now we do the new stuff
    this.reconfigurators = this.knowOtherReconfigurators(anyReconfigurator);
    if (this.reconfigurators == null || this.reconfigurators.isEmpty()) {
      throw new IOException(
              "Unable to find any reconfigurator addresses; "
              + "at least one needed to initialize client");
    }
    this.asyncClient = new AsyncClient(reconfigurators,
            !disableSSL ? ReconfigurationConfig.getClientSSLMode()
                    : SSLDataProcessingWorker.SSL_MODES.CLEAR,
            !disableSSL ? ReconfigurationConfig.getClientPortSSLOffset()
                    : ReconfigurationConfig.getClientPortClearOffset());
    this.checkConnectivity();
  }

  private static final String DEFAULT_INSTANCE = "server.gns.name";

  // ALl the old parts are in here waiting to be thrown out or moved.
  private AbstractGnsClient(boolean disableSSL) {
    // FIXME: This should be initalized to something better. See the doc for GNSInstance.
    this.GNSInstance = DEFAULT_INSTANCE;
    SSLDataProcessingWorker.SSL_MODES sslMode = disableSSL ? CLEAR
            : ReconfigurationConfig.getClientSSLMode();
    GNSClientConfig.getLogger().log(Level.FINE, "SSL Mode is {0}", sslMode.name());
    resetInstrumentation();
  }

  // FROM old GNSCLIENT
  /**
   * TODO: implement request/response to know of other reconfigurators. It is
   * also okay to just use a single reconfigurator address if it is an anycast
   * address (with the TCP error caveat under route changes).
   */
  private Set<InetSocketAddress> knowOtherReconfigurators(
          InetSocketAddress anyReconfigurator) throws IOException {
    return anyReconfigurator != null ? new HashSet<>(
            Arrays.asList(anyReconfigurator)) : STATIC_RECONFIGURATOR;
  }

  @Override
  public String toString() {
    return this.asyncClient.toString();
  }

  /**
   * Simpler async based implementation.
   *
   * @param packet
   * @throws IOException
   */
  private void sendCommandPacket(CommandPacket packet) throws IOException {
    RequestCallback callback = new RequestCallback() {
      @Override
      public void handleResponse(Request response) {
        try {
          AbstractGnsClient.this.handleCommandValueReturnPacket(response,
                  System.currentTimeMillis());
        } catch (JSONException e) {
          e.printStackTrace();
        }
      }
    };
    if (GNSCommandProtocol.CREATE_DELETE_COMMANDS
            .contains(packet.getCommandName())
            || packet.getCommandName().equals(GNSCommandProtocol.SELECT)) {
      this.asyncClient.sendRequestAnycast(packet, callback);
    } else {
      this.asyncClient.sendRequest(packet, callback);
    }
  }

  /**
   * @throws IOException
   */
  public final void checkConnectivity() throws IOException {
    this.asyncClient.checkConnectivity();
  }

  /**
   * Closes the underlying async client.
   */
  public void close() {
    this.asyncClient.close();
  }

  /**
   * @param packet
   * @param callback
   * @throws JSONException
   * @throws IOException
   */
  public void sendAsync(CommandPacket packet, RequestCallback callback)
          throws JSONException, IOException {
    if (packet.getServiceName().equals(
            Config.getGlobalString(ReconfigurationConfig.RC.SPECIAL_NAME))
            || packet.getCommandName().equals(GNSCommandProtocol.SELECT)) {
      this.asyncClient.sendRequestAnycast(packet, callback);
    } else {
      this.asyncClient.sendRequest(packet, callback);
    }
  }

  /**
   * @param packet
   * @param timeout
   * @return Response from the server or null if the timeout expires.
   * @throws IOException
   */
  public CommandValueReturnPacket sendSync(CommandPacket packet, Long timeout)
          throws IOException {
    Object monitorForAsync = new Object();
    CommandValueReturnPacket[] retval = new CommandValueReturnPacket[1];

    // send
    this.asyncClient.sendRequest(packet, new RequestCallback() {

      @Override
      public void handleResponse(Request response) {
        if (response instanceof CommandValueReturnPacket) {
          retval[0] = (CommandValueReturnPacket) response;
        }
        synchronized (monitorForAsync) {
          monitorForAsync.notify();
        }
      }
    });

    // wait for timeout
    if (retval[0] == null) {
      try {
        synchronized (monitorForAsync) {
          if (timeout != null) {
            monitorForAsync.wait(timeout);
          } else {
            monitorForAsync.wait();
          }
        }
      } catch (InterruptedException e) {
        throw new IOException(
                "sendSync interrupted while waiting for a response for "
                + packet.getSummary());
      }
    }
    return retval[0];
  }

  /**
   * @param packet
   * @return Same as {@link #sendSync(CommandPacket, Long)} but with an
   * infinite timeout.
   *
   * @throws IOException
   */
  public CommandValueReturnPacket sendSync(CommandPacket packet)
          throws IOException {
    return this.sendSync(packet, null);
  }

  // END OF FROM GNSCLIENT
  /**
   * Returns the timeout value (milliseconds) used when sending commands to the
   * server.
   *
   * @return value in milliseconds
   */
  public int getReadTimeout() {
    return readTimeout;
  }

  /**
   * Sets the timeout value (milliseconds) used when sending commands to the
   * server.
   *
   * @param readTimeout in milliseconds
   */
  public void setReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
  }

  /**
   * Sends a TCP get with given queryString to the host specified by the
   * {@link host} field.
   *
   * @param command
   * @return result of get as a string
   * @throws IOException if an error occurs
   */
  public String sendCommandAndWait(JSONObject command) throws IOException {
    if (IS_ANDROID) {
      return androidSendCommandAndWait(command);
    } else {
      return desktopSendCommmandAndWait(command);
    }
  }

  public long sendCommandNoWait(JSONObject command) throws IOException {
    if (IS_ANDROID) {
      return androidSendCommandNoWait(command).getId();
    } else {
      return desktopSendCommmandNoWait(command);
    }
  }

  /**
   * Sends a TCP get with given queryString to the host specified by the
   * {@link host} field. Waits for the response packet to come back.
   *
   * @param command
   * @return result of get as a string
   * @throws IOException if an error occurs
   */
  private String desktopSendCommmandAndWait(JSONObject command) throws IOException {
    long id = desktopSendCommmandNoWait(command);
    // now we wait until the correct packet comes back
    try {
      GNSClientConfig.getLogger().log(Level.FINE,
              "{0} waiting for query {1}",
              new Object[]{this, id + ""});
      synchronized (monitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (!resultMap.containsKey(id) && (readTimeout == 0 || System.currentTimeMillis() - monitorStartTime < readTimeout)) {
          monitor.wait(readTimeout);
        }
        if (readTimeout != 0 && System.currentTimeMillis() - monitorStartTime >= readTimeout) {
          GNSClientConfig.getLogger().log(Level.FINE,
                  "{0} timed out after {1}ms on {2}: {3}",
                  new Object[]{this, readTimeout, id + "", command});
          /* FIXME: arun: returning string errors like this is poor. You should
           * have error codes and systematic methods to automatically generate
           * error responses and be able to refactor them as needed easily.
           */
          return BAD_RESPONSE + " " + TIMEOUT;
        }
      }
      GNSClientConfig.getLogger().log(Level.FINE,
              "Response received for query {0}", new Object[]{id + ""});
    } catch (InterruptedException x) {
      GNSClientConfig.getLogger().log(Level.SEVERE, "Wait for return packet was interrupted {0}", x);
    }
    CommandResult result = resultMap.remove(id);
    GNSClientConfig.getLogger().log(Level.FINE,
            "Command name: {0} {1} {2} id: {3} " + "NS: {4} ",
            new Object[]{command.optString(COMMANDNAME, "Unknown"),
              command.optString(GUID, ""),
              command.optString(NAME, ""), id,
              result.getResponder()});
    return result.getResult();
  }

  private long desktopSendCommmandNoWait(JSONObject command) throws IOException {
    long startTime = System.currentTimeMillis();
    long id = getRandomRequestId();
    CommandPacket packet = new CommandPacket(id, command);
    GNSClientConfig.getLogger().log(Level.FINE, "{0} sending {1}:{2}",
            new Object[]{this, id + "", packet.getSummary()});
    queryTimeStamp.put(id, startTime);
    sendCommandPacket(packet);
    DelayProfiler.updateDelay("desktopSendCommmandNoWait", startTime);
    return id;
  }

  private String androidSendCommandAndWait(JSONObject command) throws IOException {
    final AndroidNIOTask sendTask = androidSendCommandNoWait(command);
    try {
      return sendTask.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  // FIXME: fix this
  private AndroidNIOTask androidSendCommandNoWait(JSONObject command) throws IOException {
    final AndroidNIOTask sendTask = new AndroidNIOTask();
    sendTask.setId(getRandomRequestId()); // so we can get it back from the task later
    sendTask.execute(
            //messenger, 
            command, sendTask.getId(),
            //localNameServerAddress, 
            monitor,
            queryTimeStamp, resultMap, readTimeout);
    return sendTask;
  }

  /**
   * Called when a command value return packet is received.
   *
   * @param json
   * @param receivedTime
   * @throws JSONException
   */
  public void handleCommandValueReturnPacket(JSONObject json, long receivedTime) throws JSONException {
    long methodStartTime = System.currentTimeMillis();
    CommandValueReturnPacket packet = new CommandValueReturnPacket(json);
    long id = packet.getClientRequestId();
    // *INSTRUMENTATION*
    GNSClientConfig.getLogger().log(Level.FINE, "{0} received response {1}",
            new Object[]{this, packet.getSummary()});
    long queryStartTime = queryTimeStamp.remove(id);
    long latency = receivedTime - queryStartTime;
    movingAvgLatency = Util.movingAverage(latency, movingAvgLatency);
    // *END OF INSTRUMENTATION*
    GNSClientConfig.getLogger().log(Level.FINE,
            "Handling return packet: {0}", new Object[]{json});
    // store the response away
    resultMap.put(id, new CommandResult(packet, receivedTime, latency));
    // This differentiates between packets sent synchronusly and asynchronusly
    if (!pendingAsynchPackets.containsKey(id)) {
      // for synchronus sends we notify waiting threads
      synchronized (monitor) {
        monitor.notifyAll();
      }
    } else {
      // Handle the asynchronus packets
      // note that we have recieved the reponse
      GNSClientConfig.getLogger().log(Level.FINE, "Removing async return packet: {0}",
              new Object[]{json});

      pendingAsynchPackets.remove(id);
      // * INSTRUMENTATION *
      // Record errors 
      if (packet.getErrorCode().isAnError()) {
        totalAsynchErrors++;
      }
    }
    DelayProfiler.updateCount("handleCommandValueReturnPacket", 1);
    DelayProfiler.updateDelay("handleCommandValueReturnPacket", methodStartTime);
  }

  /**
   * arun: Handles both command return values and active replica error
   * messages.
   *
   * @param response
   * @param receivedTime
   * @throws JSONException
   */
  protected void handleCommandValueReturnPacket(Request response,
          long receivedTime) throws JSONException {
    long methodStartTime = System.currentTimeMillis();
    CommandValueReturnPacket packet = response instanceof CommandValueReturnPacket ? (CommandValueReturnPacket) response
            : null;
    ActiveReplicaError error = response instanceof ActiveReplicaError ? (ActiveReplicaError) response
            : null;
    assert (packet != null || error != null);

    long id = packet != null ? packet.getClientRequestId() : error.getRequestID();
    GNSClientConfig.getLogger().log(
            Level.FINE,
            "{0} received response {1}:{2} from {3}",
            new Object[]{this, id + "", response.getSummary(),
              packet != null ? packet.getResponder() : error.getSender()});
    long queryStartTime = queryTimeStamp.remove(id);
    long latency = receivedTime - queryStartTime;
    movingAvgLatency = Util.movingAverage(latency, movingAvgLatency);
    GNSClientConfig.getLogger().log(Level.FINE,
            "Handling return packet: {0}", new Object[]{response.getSummary()});
    // store the response away
    if (packet != null) {
      resultMap.put(id, new CommandResult(packet, receivedTime, latency));
    } else {
      resultMap.put(id, new CommandResult(error, receivedTime, latency));
    }
    // differentiates between synchronusly and asynchronusly sent
    if (!pendingAsynchPackets.containsKey(id)) {
      // for synchronus sends we notify waiting threads
      synchronized (monitor) {
        monitor.notifyAll();
      }
    } else {
      // Handle the asynchronus packets
      // note that we have received the response
      pendingAsynchPackets.remove(id);
      // Record errors
      if (packet != null) {
        if (packet.getErrorCode().isAnError()) {
          totalAsynchErrors++;
        }
      }
    }
    DelayProfiler.updateDelay("handleCommandValueReturnPacket", methodStartTime);
  }

  /**
   * Returns true if a response has been received.
   *
   * @param id
   * @return
   */
  public boolean isAsynchResponseReceived(long id) {
    return resultMap.containsKey(id);
  }

  /**
   * Removes and returns the command result.
   *
   * @param id
   * @return
   */
  public CommandResult removeAsynchResponse(long id) {
    return resultMap.remove(id);
  }

  /**
   * Shuts down the NIOTransport thread.
   */
  public void stop() {
    this.asyncClient.close();
  }

// ASYNCHRONUS OPERATIONS
  /**
   * This contains all the command packets sent out asynchronously that have
   * not been acknowledged yet.
   */
  private final ConcurrentHashMap<Long, CommandPacket> pendingAsynchPackets
          = new ConcurrentHashMap<>();

  public int outstandingAsynchPacketCount() {
    return pendingAsynchPackets.size();
  }

  /**
   * Sends a command packet without waiting for a response.
   * Performs bookkeeping so we can retrieve the response.
   *
   * @param packet
   * @throws IOException
   */
  public void sendCommandPacketAsynch(CommandPacket packet) throws IOException {
    long startTime = System.currentTimeMillis();
    long id = packet.getClientRequestId();
    pendingAsynchPackets.put(id, packet);
    GNSClientConfig.getLogger().log(Level.FINE, "{0} sending request {1}:{2}",
            new Object[]{this, id + "", packet.getSummary()});
    queryTimeStamp.put(id, System.currentTimeMillis());
    sendCommandPacket(packet);
    DelayProfiler.updateDelay("sendAsynchTestCommand", startTime);
  }

  /**
   * Creates a command object from the given CommandType and a variable
   * number of key and value pairs with a signature parameter. The signature is
   * generated from the query signed by the given guid.
   *
   * @param commandType
   * @param privateKey
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws ClientException
   */
  // FIXME: Temporarily hacked version for adding new command type integer. Will clean up once
  // transition is done.
  // The action argument will be going away and be ultimately replaced by the
  // enum string.
  public JSONObject createAndSignCommand(CommandType commandType,
          PrivateKey privateKey, String action, Object... keysAndValues)
          throws ClientException {
    try {
      JSONObject result = createCommand(commandType, action, keysAndValues);
      result.put(GNSCommandProtocol.TIMESTAMP, Format.formatDateISO8601UTC(new Date()));
      result.put(GNSCommandProtocol.SEQUENCE_NUMBER, getRandomRequestId());

      String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
      String signatureString = signDigestOfMessage(privateKey, canonicalJSON);
      result.put(GNSCommandProtocol.SIGNATURE, signatureString);
      return result;
    } catch (JSONException | NoSuchAlgorithmException | InvalidKeyException | SignatureException | UnsupportedEncodingException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs.
   *
   * @param commandType
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  // Fixme: remove action parameter
  public JSONObject createCommand(CommandType commandType, String action,
          Object... keysAndValues)
          throws ClientException {
    try {
      JSONObject result = new JSONObject();
      String key;
      Object value;
      result.put(GNSCommandProtocol.COMMAND_INT, commandType.getInt());
      result.put(GNSCommandProtocol.COMMANDNAME, action);
      for (int i = 0; i < keysAndValues.length; i = i + 2) {
        key = (String) keysAndValues[i];
        value = keysAndValues[i + 1];
        result.put(key, value);
      }
      if (forceCoordinatedReads) {
        result.put(COORDINATE_READS, true);
      }
      return result;
    } catch (JSONException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  /**
   * Resets the instrumentation.
   *
   */
  public final void resetInstrumentation() {
    movingAvgLatency = 0;
  }

  /**
   * Instrumentation. Returns the moving average of request latency
   * as seen by the client.
   *
   * @return
   */
  public double getMovingAvgLatency() {
    return movingAvgLatency;
  }

  /**
   * Instrumentation. Currently only valid when asynch testing.
   *
   * @return
   */
  public int getTotalAsynchErrors() {
    return totalAsynchErrors;
  }

  /**
   * Return a string representing the GNS server that we are connecting to.
   *
   * @return
   */
  public String getGNSInstance() {
    return GNSInstance;
  }

  public boolean isForceCoordinatedReads() {
    return forceCoordinatedReads;
  }

  public void setForceCoordinatedReads(boolean forceCoordinatedReads) {
    this.forceCoordinatedReads = forceCoordinatedReads;
  }

}
