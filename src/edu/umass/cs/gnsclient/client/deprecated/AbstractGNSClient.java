
package edu.umass.cs.gnsclient.client.deprecated;

import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GNSClientConfig.GNSCC;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;


@Deprecated
public abstract class AbstractGNSClient {


  public static final boolean IS_ANDROID
          = System.getProperty("java.vm.name").equalsIgnoreCase("Dalvik");


  protected long readTimeout = 8000; // 20 seconds... was 40 seconds


  private final ConcurrentMap<Long, Request> resultMap = new ConcurrentHashMap<Long, Request>(
          10, 0.75f, 3);


  private final Object monitor = new Object();


  abstract void checkConnectivity() throws IOException;


  abstract void close();


  @Deprecated
  ResponsePacket sendCommandAndWait(JSONObject command) throws IOException {
    if (IS_ANDROID) {
      return androidSendCommandAndWait(command);
    } else {
      return desktopSendCommmandAndWait(command);
    }
  }

  private static final boolean USE_GLOBAL_MONITOR = Config.getGlobalBoolean(GNSCC.USE_GLOBAL_MONITOR);

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


  protected static ResponsePacket getTimeoutResponse(AbstractGNSClient me, CommandPacket commandPacket) {
    GNSClientConfig.getLogger().log(Level.INFO,
            "{0} timed out after {1}ms on {2}: {3}",
            new Object[]{me, me.readTimeout, commandPacket.getRequestID() + "", commandPacket.getSummary()});

    return new ResponsePacket(commandPacket.getServiceName(), commandPacket.getRequestID(), ResponseCode.TIMEOUT,
            GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.TIMEOUT.toString() + " for command " + commandPacket.getSummary());
  }

  private CommandPacket desktopSendCommmandNoWait(JSONObject command) throws IOException {
    return this.desktopSendCommmandNoWait(command, generateNextRequestID());
  }


  protected abstract boolean isForceCoordinatedReads();

  private CommandPacket desktopSendCommmandNoWait(JSONObject command, long id) throws IOException {
    long startTime = System.currentTimeMillis();
    CommandPacket packet = new CommandPacket(
            id,
            command);

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

  private final ConcurrentHashMap<Long, CommandPacket> pendingAsynchPackets
          = new ConcurrentHashMap<>();

  // arun: Made sendAsync abstract instead of sendCommandPacket


  protected abstract RequestFuture<CommandPacket> sendAsync(CommandPacket packet,
          Callback<Request, CommandPacket> callback) throws IOException;


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
