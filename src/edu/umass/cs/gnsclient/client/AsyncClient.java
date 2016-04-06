/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Straightforward async client implementation that expects only one packet
 * type, {@link Packet.PacketType.COMMAND_RETURN_VALUE}.
 */
public class AsyncClient extends ReconfigurableAppClientAsync {

  private static final Stringifiable<String> unstringer = new StringifiableDefault<>("");
  private static final Set<IntegerPacketType> CLIENT_PACKET_TYPES = new HashSet<>(Arrays.asList(Packet.PacketType.COMMAND_RETURN_VALUE));

  public AsyncClient(Set<InetSocketAddress> reconfigurators, SSLDataProcessingWorker.SSL_MODES sslMode, int clientPortOffset) throws IOException {
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
      GNSClientConfig.getLogger().log(Level.WARNING, "Problem parsing packet from {0}: {1}", new Object[]{json, e});
    }
    return response;
  }

  @Override
  public Request getRequestFromJSON(JSONObject json) throws RequestParseException {
    Request response = null;
    try {
      Packet.PacketType type = Packet.getPacketType(json);
      if (type != null) {
        GNSClientConfig.getLogger().log(Level.INFO, "{0} retrieving packet from received json {1}", new Object[]{this, json});
        if (CLIENT_PACKET_TYPES.contains(Packet.getPacketType(json))) {
          response = (Request) Packet.createInstance(json, unstringer);
        }
        assert (response == null || response.getRequestType() == Packet.PacketType.COMMAND_RETURN_VALUE);
      }
    } catch (JSONException e) {
      GNSClientConfig.getLogger().log(Level.WARNING, "Problem parsing packet from {0}: {1}", new Object[]{json, e});
    }
    return response;
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return Collections.unmodifiableSet(CLIENT_PACKET_TYPES);
  }
  
}
