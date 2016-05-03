package edu.umass.cs.gnsclient.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;

/**
 * @author arun
 *
 * Cleaner implementation of a GNS client using gigapaxos' async client.
 */
public class GNSClient extends AbstractGNSClient {

  // initialized from properties file
  private static final Set<InetSocketAddress> STATIC_RECONFIGURATORS = ReconfigurationConfig
          .getReconfiguratorAddresses();

  // initialized upon contsruction
  private final Set<InetSocketAddress> reconfigurators;
  private final AsyncClient asyncClient;

  private static final java.util.logging.Logger LOG = GNSConfig.getLogger();

  /**
   * @throws IOException
   */
  public GNSClient() throws IOException {
    this(STATIC_RECONFIGURATORS != null
            && !STATIC_RECONFIGURATORS.isEmpty() ? STATIC_RECONFIGURATORS
                    .iterator().next() : null);
  }

  /**
   * Bootstrap with a single, arbitrarily chosen valid reconfigurator address.
   * The client can enquire and know of other reconfigurator addresses from
   * this reconfigurator. If it is unable to do so, it will throw an
   * IOException.
   *
   * @param anyReconfigurator
   * @throws IOException
   */
  public GNSClient(InetSocketAddress anyReconfigurator)
          throws IOException {
    super(anyReconfigurator);
    this.reconfigurators = this.knowOtherReconfigurators(anyReconfigurator);
    if (this.reconfigurators == null || this.reconfigurators.isEmpty()) {
      throw new IOException(
              "Unable to find any reconfigurator addresses; "
              + "at least one needed to initialize client");
    }
    this.asyncClient = new AsyncClient(reconfigurators, ReconfigurationConfig.getClientSSLMode(), ReconfigurationConfig.getClientPortSSLOffset());
    this.checkConnectivity();
  }

  /**
   * TODO: implement request/response to know of other reconfigurators. It is
   * also okay to just use a single reconfigurator address if it is an anycast
   * address (with the TCP error caveat under route changes).
   */
  private Set<InetSocketAddress> knowOtherReconfigurators(
          InetSocketAddress anyReconfigurator) throws IOException {
    return anyReconfigurator != null ? new HashSet<>(
            Arrays.asList(anyReconfigurator)) : STATIC_RECONFIGURATORS;
  }

  @Override
  public String toString() {
    return this.asyncClient.toString();
  }

  /**
   * Overrides older implementation of
   * {@link #sendCommandPacket(CommandPacket)} with simpler async
   * implementation.
   *
   * @param packet
   * @throws IOException
   */
  @Override
  protected void sendCommandPacket(CommandPacket packet) throws IOException {
    RequestCallback callback = new RequestCallback() {
      @Override
      public void handleResponse(Request response) {
        try {
          GNSClient.this.handleCommandValueReturnPacket(response,
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
  @Override
  public void checkConnectivity() throws IOException {
    this.asyncClient.checkConnectivity();
  }

  /**
   * Closes the underlying async client.
   */
  @Override
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
            Config.getGlobalString(RC.SPECIAL_NAME))
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
    Object monitor = new Object();
    CommandValueReturnPacket[] retval = new CommandValueReturnPacket[1];

    // send
    this.asyncClient.sendRequest(packet, new RequestCallback() {

      @Override
      public void handleResponse(Request response) {
        if (response instanceof CommandValueReturnPacket) {
          retval[0] = (CommandValueReturnPacket) response;
        }
        synchronized (monitor) {
          monitor.notify();
        }
      }
    });

    // wait for timeout
    if (retval[0] == null) {
      try {
        synchronized (monitor) {
          if (timeout != null) {
            monitor.wait(timeout);
          } else {
            monitor.wait();
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

  /**
   * Straightforward async client implementation that expects only one packet
   * type, {@link Packet.PacketType.COMMAND_RETURN_VALUE}.
   */
  static class AsyncClient extends ReconfigurableAppClientAsync {

    private static Stringifiable<String> unstringer = new StringifiableDefault<>(
            "");

    static final Set<IntegerPacketType> clientPacketTypes = new HashSet<>(
            Arrays.asList(Packet.PacketType.COMMAND_RETURN_VALUE));

    public AsyncClient(Set<InetSocketAddress> reconfigurators,
            SSL_MODES sslMode, int clientPortOffset) throws IOException {
      super(reconfigurators, sslMode, clientPortOffset);
      this.enableJSONPackets();
    }

    @Override
    public Request getRequest(String msg) throws RequestParseException {
      Request response = null;
      JSONObject json = null;
      try {
        return this.getRequestFromJSON(new JSONObject(msg));
      } catch (JSONException e) {
        LOG.log(Level.WARNING, "Problem parsing packet from {0}: {1}", new Object[]{json, e});
      }
      return response;
    }

    @Override
    public Request getRequestFromJSON(JSONObject json) throws RequestParseException {
      Request response = null;
      try {
        Packet.PacketType type = Packet.getPacketType(json);
        if (type != null) {
          LOG.log(Level.FINER,
                  "{0} retrieving packet from received json {1}",
                  new Object[]{this, json});
          if (clientPacketTypes.contains(Packet.getPacketType(json))) {
            response = (Request) Packet.createInstance(json,
                    unstringer);
          }
          assert (response == null || response.getRequestType() == Packet.PacketType.COMMAND_RETURN_VALUE);
        }
      } catch (JSONException e) {
        LOG.log(Level.WARNING, "Problem parsing packet from {0}: {1}", new Object[]{json, e});
      }
      return response;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
      return clientPacketTypes;
    }
  }

  public static void main(String[] args) throws IOException {
    GNSClient client = new GNSClient(null);
    client.close();
    System.out.println("Client created, successfully checked connectivity, and closing");
  }
}
