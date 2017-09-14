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
package edu.umass.cs.gnsclient.client.deprecated;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.utils.Util;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GNSClientConfig.GNSCC;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

/**
 * The base for all GNS clients.
 *
 * This class is almost useless and will be completely removed soon.
 *
 */
@Deprecated
public abstract class AbstractGNSClient {

  /**
   * Indicates whether we are on an Android platform or not
   */
  public static final boolean IS_ANDROID
          = System.getProperty("java.vm.name").equalsIgnoreCase("Dalvik");

  /**
   * The length of time we will wait for a command response from the server
   * before giving up.
   */
  protected long readTimeout = 8000; // 20 seconds... was 40 seconds

  /* Keeps track of requests that are sent out and the reponses to them */
  private final ConcurrentMap<Long, Request> resultMap = new ConcurrentHashMap<Long, Request>(
          10, 0.75f, 3);

  /* Used by the wait/notify calls */
  private final Object monitor = new Object();

  /**
   * Check that the connectivity with the host:port can be established
   *
   * @throws IOException throws exception if a communication error occurs
   */
  abstract void checkConnectivity() throws IOException;

  /**
   * Closes the underlying messenger.
   */
  abstract void close();

  /**
   * Sends a command to the server and returns a response.
   *
   * @param command
   * @return Result as CommandValueReturnPacket; or String if Android (FIXME)
   * @throws IOException if an error occurs
   */
  @Deprecated
  ResponsePacket sendCommandAndWait(JSONObject command) throws IOException {
    if (IS_ANDROID) {
      return androidSendCommandAndWait(command);
    } else {
      return desktopSendCommmandAndWait(command);
    }
  }

  private static final boolean USE_GLOBAL_MONITOR = Config.getGlobalBoolean(GNSCC.USE_GLOBAL_MONITOR);
  /**
   * Sends a command to the server.
   * Waits for the response packet to come back.
   *
   */
  private ConcurrentHashMap<Long, Object> monitorMap = new ConcurrentHashMap<Long, Object>();

  // arun: changed this to return CommandValueReturnPacket
  private ResponsePacket desktopSendCommmandAndWait(JSONObject command) throws IOException {
    Object myMonitor = new Object();
    long id;
    monitorMap.put(id = this.generateNextRequestID(), myMonitor);

    CommandPacket commandPacket = desktopSendCommmandNoWait(command, id);
    // now we wait until the correct packet comes back
    try {
      GNSClientConfig.getLogger().log(Level.FINE,
              "{0} waiting for query {1}",
              new Object[]{this, id + ""});

      long monitorStartTime = System.currentTimeMillis();
      if (!USE_GLOBAL_MONITOR) {
        synchronized (myMonitor) {
          while (monitorMap.containsKey(id) && (readTimeout == 0 || System.currentTimeMillis()
                  - monitorStartTime < readTimeout)) {
            myMonitor.wait(readTimeout);
          }
        }
      } else {
        synchronized (monitor) {
          while (!resultMap.containsKey(id)
                  && (readTimeout == 0 || System.currentTimeMillis()
                  - monitorStartTime < readTimeout)) {
            monitor.wait(readTimeout);
          }
        }
      }

      if (readTimeout != 0 && System.currentTimeMillis() - monitorStartTime >= readTimeout) {
        return getTimeoutResponse(this, commandPacket);
      }
      GNSClientConfig.getLogger().log(Level.FINE,
              "Response received for query {0}", new Object[]{id + ""});
    } catch (InterruptedException x) {
      GNSClientConfig.getLogger().severe("Wait for return packet was interrupted " + x);
    }
    //CommandResult 
    Request result = resultMap.remove(id);
    GNSClientConfig.getLogger().log(Level.FINE,
            "{0} received response {1} ",
            new Object[]{this, result.getSummary()
            });

    return result instanceof ResponsePacket ? ((ResponsePacket) result)
            : new ResponsePacket(result.getServiceName(), id,
                    ResponseCode.ACTIVE_REPLICA_EXCEPTION,
                    ((ActiveReplicaError) result).getResponseMessage());
  }

  /**
   *
   * @param me
   * @param commandPacket
   * @return a response packet
   */
  protected static ResponsePacket getTimeoutResponse(AbstractGNSClient me, CommandPacket commandPacket) {
    GNSClientConfig.getLogger().log(Level.INFO,
            "{0} timed out after {1}ms on {2}: {3}",
            new Object[]{me, me.readTimeout, commandPacket.getRequestID() + "", commandPacket.getSummary()});
    /* FIXME: Remove use of string reponse codes */
    return new ResponsePacket(commandPacket.getServiceName(), commandPacket.getRequestID(), ResponseCode.TIMEOUT,
            GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.TIMEOUT.toString() + " for command " + commandPacket.getSummary());
  }

  /**
   *
   * @return true if force coordinated reads is true
   */
  protected abstract boolean isForceCoordinatedReads();

  private CommandPacket desktopSendCommmandNoWait(JSONObject command, long id) throws IOException {
    long startTime = System.currentTimeMillis();
    CommandPacket packet = new CommandPacket(
            id,
            command);
    /* arun: moved this here from createCommand. This is the right place to
		 * put it because it is not easy to change "command" once it has been
		 * signed, and the command creation methods are and should be static. */
    packet.setForceCoordinatedReads(this.isForceCoordinatedReads());
    GNSClientConfig.getLogger().log(Level.FINE, "{0} sending {1}:{2}",
            new Object[]{this, id + "", packet.getSummary()});
    sendCommandPacket(packet);
    DelayProfiler.updateDelay("desktopSendCommmandNoWait", startTime);
    return packet;
  }

  private ResponsePacket androidSendCommandAndWait(JSONObject command) throws IOException {
    final AndroidNIOTask sendTask = androidSendCommandNoWait(command);
    try {
      return new ResponsePacket(new JSONObject(sendTask.get()));
    } catch (InterruptedException | ExecutionException | JSONException e) {
      throw new IOException(e);
    }
  }

  private AndroidNIOTask androidSendCommandNoWait(JSONObject command) throws IOException {
    final AndroidNIOTask sendTask = new AndroidNIOTask();
    sendTask.setId(generateNextRequestID()); // so we can get it back from the task later
    sendTask.execute(command, sendTask.getId(), monitor,
            resultMap, readTimeout);
    return sendTask;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  /**
   * arun: Handles both command return values and active replica error
   * messages.
   *
   * @param response
   * @param receivedTime
   * @throws JSONException
   */
  private void handleCommandValueReturnPacket(Request response
  ) {
    long methodStartTime = System.currentTimeMillis();
    ResponsePacket packet = response instanceof ResponsePacket ? (ResponsePacket) response
            : null;
    ActiveReplicaError error = response instanceof ActiveReplicaError ? (ActiveReplicaError) response
            : null;
    assert (packet != null || error != null);

    long id = packet != null ? packet.getClientRequestId() : error.getRequestID();
    GNSClientConfig.getLogger().log(
            Level.FINE,
            "{0} received response {1}:{2} from {3}",
            new Object[]{this, id + "", response.getSummary(),
              packet != null
                      ? "unknown"//packet.getResponder() 
                      : error.getSender()});
    // store the response away
    if (packet != null) {
      resultMap.put(id, packet);
    } else {
      resultMap.put(id, error);
    }

    // differentiates between synchronusly and asynchronusly sent
    if (!pendingAsynchPackets.containsKey(id)) {
      Object myMonitor = monitorMap.remove(id);
      assert (myMonitor != null) : Util.suicide("No monitor entry found for request " + id);
      if (!USE_GLOBAL_MONITOR && myMonitor != null) {
        synchronized (myMonitor) {
          myMonitor.notify();
        }
      }
      /* for synchronous sends we notify waiting threads.
       * arun: Needed now only for Android if at all.
       */
      synchronized (monitor) {
        monitor.notifyAll();
      }
    } else {
      // Handle the asynchronus packets
      // note that we have recieved the reponse
      pendingAsynchPackets.remove(id);
    }
    DelayProfiler.updateDelay("handleCommandValueReturnPacket", methodStartTime);
  }

  /**
   * @return random long not in map
   */
  private synchronized long generateNextRequestID() {
    long id;
    do {
      id = (long) (Math.random() * Long.MAX_VALUE);
      // this is actually wrong because we can still generate duplicate keys
      // because the resultMap doesn't contain pending requests until they come back
    } while (resultMap.containsKey(id));
    return id;
  }

// ASYNCHRONUS OPERATIONS
  /**
   * This contains all the command packets sent out asynchronously that have
   * not been acknowledged yet.
   */
  private final ConcurrentHashMap<Long, CommandPacket> pendingAsynchPackets
          = new ConcurrentHashMap<>();

  // arun: Made sendAsync abstract instead of sendCommandPacket

  /**
   *
   * @param packet
   * @param callback
   * @return returns a RequestFuture
   * @throws IOException
   */
  protected abstract RequestFuture<CommandPacket> sendAsync(CommandPacket packet,
          Callback<Request, CommandPacket> callback) throws IOException;

  /**
   * Overrides older implementation of
   * {@link #sendCommandPacket(CommandPacket)} with simpler async
   * implementation.
   *
   * @param packet
   * @throws IOException
   */
  private void sendCommandPacket(CommandPacket commandPacket) throws IOException {
    this.sendAsync(commandPacket, new Callback<Request, CommandPacket>() {
      @Override
      public CommandPacket processResponse(Request response) {
        handleCommandValueReturnPacket(response);
        return commandPacket;
      }
    });
// Lambdas were causing issues in Andriod - 9/16
//    this.sendAsync(packet, (response) -> {
//      this.handleCommandValueReturnPacket(response);
//      return packet;
//    });
  }

}
