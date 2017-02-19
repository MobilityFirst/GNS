/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy, arun */
package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.contextservice.integration.ContextServiceGNSClient;
import edu.umass.cs.contextservice.integration.ContextServiceGNSInterface;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestIdentifier;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.database.MongoRecords;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnscommon.packets.AdminCommandPacket;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnsserver.database.NoSQLRecords;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.AdminListener;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithClientAddress;
import edu.umass.cs.gnsserver.gnamed.DnsTranslator;
import edu.umass.cs.gnsserver.gnamed.UdpDnsServer;
import edu.umass.cs.gnsserver.gnsapp.packet.InternalCommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import edu.umass.cs.gnsserver.gnsapp.packet.PacketInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.GNSRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.httpserver.GNSHttpServer;
import edu.umass.cs.gnsserver.httpserver.GNSHttpsServer;
import edu.umass.cs.gnsserver.localnameserver.LocalNameServer;
import edu.umass.cs.gnsserver.utils.Shutdownable;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageExtractor;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.GCConcurrentHashMap;
import edu.umass.cs.utils.GCConcurrentHashMapCallback;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.logging.Level;

/**
 * @author Westy, arun
 */
public class GNSApp extends AbstractReconfigurablePaxosApp<String> implements
        GNSApplicationInterface<String>, Replicable, Reconfigurable,
        ClientMessenger, AppRequestParserBytes, Shutdownable {

  private String nodeID;
  private InetSocketAddress nodeAddress;
  private NodeConfig<String> nodeConfig;
  private boolean constructed = false;
  /**
   * Object provides interface to the database table storing name records
   */
  private BasicRecordMap nameRecordDB;
  /**
   * The Nio server
   */
  private SSLMessenger<String, JSONObject> messenger;
  private ClientRequestHandlerInterface requestHandler;
  private static final long DEFAULT_REQUEST_TIMEOUT = 8000;
  private final GCConcurrentHashMap<Long, Request> outstanding = new GCConcurrentHashMap<>(
          new GCConcurrentHashMapCallback() {
    @Override
    public void callbackGC(Object key, Object value) {
    }
  }, DEFAULT_REQUEST_TIMEOUT);

  /* It's silly to enqueue requests when all GNS calls are blocking anyway. We
   * now use a simpler and more sensible sendToClient method that tracks the
   * original CommandPacket explicitly throughout the execution chain.
   */
  private static boolean enqueueCommand() {
    return false;
  }
  /**
   * Active code handler
   */
  private ActiveCodeHandler activeCodeHandler;

  /**
   * context service interface
   */
  private ContextServiceGNSInterface contextServiceGNSClient;

  /**
   * The non-secure http server
   */
  private GNSHttpServer httpServer = null;
  /**
   * The secure http server
   */
  private GNSHttpsServer httpsServer = null;
  /**
   *
   */
  private LocalNameServer localNameServer = null;
  /**
   * The UdpDnsServer that serves DNS requests through UDP.
   */
  private UdpDnsServer udpDnsServer = null;
  /**
   * The DnsTranslator that serves DNS requests through UDP.
   */
  private DnsTranslator dnsTranslator = null;

  /**
   * Handles admin requests from the client
   */
  private AdminListener adminListener = null;
  /**
   * Handles admin requests for each replica
   */
  private AppAdminServer appAdminServer = null;

  /**
   * Constructor invoked via reflection by gigapaxos.
   *
   * @param args
   * @throws IOException
   */
  public GNSApp(String[] args) throws IOException {
  }

  /**
   *
   * @param messenger
   */
  @Override
  @SuppressWarnings("unchecked")
  public void setClientMessenger(SSLMessenger<?, JSONObject> messenger) {
    this.messenger = (SSLMessenger<String, JSONObject>) messenger;
    this.nodeID = messenger.getMyID().toString();
    try {
      if (!constructed) {
        this.GnsAppConstructor((JSONMessenger<String>) messenger);
      }
    } catch (IOException e) {
      e.printStackTrace();
      GNSConfig.getLogger().severe("Unable to create GNS app: exiting");
      System.exit(1);
    }
  }

  private static final PacketType[] PACKET_TYPES = {PacketType.COMMAND,
    PacketType.SELECT_REQUEST, PacketType.SELECT_RESPONSE,
    PacketType.INTERNAL_COMMAND};

  private static final PacketType[] MUTUAL_AUTH_TYPES = {PacketType.ADMIN_COMMAND};

  /**
   * arun: The code below {@link #incrResponseCount(ClientRequest)} and
   * {@link #executeNoop(Request)} is for instrumentation only and will go
   * away soon.
   */
  private static final int RESPONSE_COUNT_THRESHOLD = 100;
  private static boolean doneOnce = false;

  private synchronized void incrResponseCount(ClientRequest response) {
    if (responseCount++ > RESPONSE_COUNT_THRESHOLD
            && response instanceof ResponsePacket
            && (!doneOnce && (doneOnce = true))) {
      try {
        this.cachedResponse = ((PacketInterface) response)
                .toJSONObject();
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
  }

  private static final boolean EXECUTE_NOOP_ENABLED = Config
          .getGlobalBoolean(GNSConfig.GNSC.EXECUTE_NOOP_ENABLED);
  private int responseCount = 0;
  private JSONObject cachedResponse = null;

  @SuppressWarnings("deprecation")
  private boolean executeNoop(Request request) {
    if (!EXECUTE_NOOP_ENABLED) {
      return false;
    }
    if (cachedResponse != null && request instanceof CommandPacket
            && ((CommandPacket) request).getCommandType().isRead()) {
      try {
        ((BasicPacketWithClientAddress) request)
                .setResponse(new ResponsePacket(cachedResponse)
                        .setClientRequestAndLNSIds(((ClientRequest) request)
                                .getRequestID()));
      } catch (JSONException e) {
        e.printStackTrace();
      }
      return true;
    }
    return false;
  }

  /**
   *
   * @param request
   * @param doNotReplyToClient
   * @return true if the command is successfully executed
   */
  @SuppressWarnings("unchecked")
  // we explicitly check type
  @Override
  public boolean execute(Request request, boolean doNotReplyToClient) {
    boolean executed = false;
    if (executeNoop(request)) {
      return true;
    }
    try {
      Packet.PacketType packetType = request.getRequestType() instanceof Packet.PacketType ? (Packet.PacketType) request
              .getRequestType() : null;
      GNSConfig.getLogger().log(Level.FINE, "{0} starting execute({1})",
              new Object[]{this, request.getSummary()});
      Request prev = null;
      // arun: enqueue request, dequeue before returning
      if (request instanceof RequestIdentifier) {
        if (enqueueCommand()) {
          prev = this.outstanding.putIfAbsent(
                  ((RequestIdentifier) request).getRequestID(), request);
        }
      } else {
        assert (false) : this
                + " should not be getting requests that do not implement "
                + RequestIdentifier.class;
      }

      switch (packetType) {
        case SELECT_REQUEST:
          Select.handleSelectRequest((SelectRequestPacket) request, this);
          break;
        case SELECT_RESPONSE:
          Select.handleSelectResponse((SelectResponsePacket) request, this);
          break;
        case COMMAND:
          CommandHandler.handleCommandPacket((CommandPacket) request, doNotReplyToClient, this);
          break;
        case ADMIN_COMMAND:
          CommandHandler.handleCommandPacket((AdminCommandPacket) request, doNotReplyToClient, this);
          break;
        default:
          assert (false) : (this
                  + " should not be getting packets of type "
                  + packetType + "; exiting");
          GNSConfig.getLogger().log(Level.SEVERE, " Packet type not found: {0}", request.getSummary());
          return false;
      }
      executed = true;

      // arun: always clean up all created state upon exiting
      if (request instanceof RequestIdentifier && prev == null) {
        GNSConfig.getLogger().log(Level.FINE,
                "{0} finished execute({1})  ->  {2}",
                new Object[]{
                  this,
                  request.getSummary(),
                  request instanceof ClientRequest
                  && ((ClientRequest) request)
                  .getResponse() != null ? ((ClientRequest) request)
                          .getResponse().getSummary()
                          : null});
        this.outstanding.remove(((RequestIdentifier) request).getRequestID());
      }

    } catch (JSONException | IOException | ClientException | InternalRequestException e) {
      e.printStackTrace();
    } catch (FailedDBOperationException e) {
      // all database operations throw this exception, therefore we keep
      // throwing this exception upwards and catch this
      // here.
      // A database operation error would imply that the application
      // hasn't been able to successfully execute
      // the request. therefore, this method returns 'false', hoping that
      // whoever calls handleDecision would retry
      // the request.
      GNSConfig.getLogger().log(Level.SEVERE,
              "Error handling request: {0}", request.toString());
      e.printStackTrace();
    }
    return executed;
  }

  @Override
  public void shutdown() {
    if (localNameServer != null) {
      localNameServer.shutdown();
    }
    if (udpDnsServer != null) {
      udpDnsServer.shutdown();
    }
    if (dnsTranslator != null) {
      dnsTranslator.shutdown();
    }
    if (adminListener != null) {
      adminListener.shutdown();
    }
    if (appAdminServer != null) {
      appAdminServer.shutdown();
    }
    if (httpServer != null) {
      httpServer.stop();
    }
    if (httpsServer != null) {
      httpsServer.stop();
    }
    if (this.requestHandler.getInternalClient() != null) {
      this.requestHandler.getInternalClient().close();
    }
  }

  /**
   * Actually creates the application. This strange way of constructing the application
   * is because of legacy code that used the createAppCoordinator interface.
   *
   * @param messenger
   * @throws java.io.IOException
   */
  private void GnsAppConstructor(JSONMessenger<String> messenger) throws IOException {
    this.nodeID = messenger.getMyID();
    this.nodeConfig = messenger.getNodeConfig();
    this.nodeAddress = new InetSocketAddress(nodeConfig.getNodeAddress(nodeID),
            nodeConfig.getNodePort(nodeID));
    GNSConfig.getLogger().log(Level.INFO, "=== Node {0} listening on {1}===",
            new Object[]{nodeID, nodeAddress});
    NoSQLRecords noSqlRecords;
    try {
      Class<?> clazz = GNSConfig.GNSC.getNoSqlRecordsClass();
      Constructor<?> constructor = clazz.getConstructor(String.class, int.class);
      noSqlRecords = (NoSQLRecords) constructor.newInstance(nodeID, Config.getGlobalInt(GNSConfig.GNSC.MONGO_PORT));
      GNSConfig.getLogger().log(Level.INFO, "Created noSqlRecords class: {0}", clazz.getName());
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InstantiationException | InvocationTargetException e) {
      // fallback plan
      GNSConfig.getLogger().log(Level.WARNING, "Problem creating noSqlRecords from config:{0}", e.getMessage());
      noSqlRecords = new MongoRecords(nodeID, Config.getGlobalInt(GNSConfig.GNSC.MONGO_PORT));
    }
    this.nameRecordDB = new GNSRecordMap<>(noSqlRecords, MongoRecords.DBNAMERECORD);
    GNSConfig.getLogger().log(Level.FINE, "App {0} created {1}",
            new Object[]{nodeID, nameRecordDB});
    this.messenger = messenger;
    GNSNodeConfig<String> gnsNodeConfig = new GNSNodeConfig<>();
    //
    // Start admin servers
    // 
    // Create the Admintercessor object which provides
    // client support for listener commands.
    Admintercessor admintercessor = new Admintercessor();
    // Create the request handler
    this.requestHandler = new ClientRequestHandler(
            admintercessor,
            this.nodeAddress,
            nodeID, this,
            gnsNodeConfig);
    // The AdminListener thread gets requests from the Admintercessor
    // sends them out to the servers and listens for the responses.
    adminListener = new AdminListener(requestHandler);
    adminListener.start();
    admintercessor.setAdminListener(adminListener);
    // The AppAdminServer gets requests from Admintercessor through the AdminListener
    // looks up stuff in the database and returns the results back to the AdminListener.
    appAdminServer = new AppAdminServer(this, gnsNodeConfig);
    appAdminServer.start();
    GNSConfig.getLogger().log(Level.INFO, "{0} Admin thread initialized", nodeID);
    // Start up some HTTP servers
    httpServer = new GNSHttpServer(
            messenger.getNodeConfig().getNodePort(this.nodeID)
            + Config.getGlobalInt(ReconfigurationConfig.RC.HTTP_PORT_OFFSET),
            requestHandler);
    httpsServer = new GNSHttpsServer(
            messenger.getNodeConfig().getNodePort(this.nodeID)
            + Config.getGlobalInt(ReconfigurationConfig.RC.HTTP_PORT_SSL_OFFSET),
            requestHandler);
    // Maybe start up an LNS server
    if (Config.getGlobalString(GNSConfig.GNSC.LOCAL_NAME_SERVER_NODES).contains("all")
            || Config.getGlobalString(GNSConfig.GNSC.LOCAL_NAME_SERVER_NODES).contains(nodeID)) {
      localNameServer = new LocalNameServer();
    }
    // Maybe start up an DNS server
    if (Config.getGlobalString(GNSConfig.GNSC.DNS_SERVER_NODES).contains("all")
            || Config.getGlobalString(GNSConfig.GNSC.DNS_SERVER_NODES).contains(nodeID)) {
      startDNS();
    }
    this.activeCodeHandler = !Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)
            ? new ActiveCodeHandler(nodeID) : null;

    // context service init
    if (Config.getGlobalBoolean(GNSConfig.GNSC.ENABLE_CNS)) {
      String nodeAddressString = Config.getGlobalString(GNSConfig.GNSC.CNS_NODE_ADDRESS);

      String[] parsed = nodeAddressString.split(":");

      assert (parsed.length == 2);

      String host = parsed[0];
      int port = Integer.parseInt(parsed[1]);
      GNSConfig.getLogger().fine("ContextServiceGNSClient initialization started");
      contextServiceGNSClient = new ContextServiceGNSClient(host, port);
      GNSConfig.getLogger().fine("ContextServiceGNSClient initialization completed");
    }

    constructed = true;
  }

  // For InterfaceApplication
  /**
   *
   * @param string
   * @return the request
   * @throws RequestParseException
   */
  @Override
  public Request getRequest(String string) throws RequestParseException {
    GNSConfig.getLogger().log(Level.FINEST, ">>>>>>>>>>>>>>> GET REQUEST: {0}", string);
    // Special case handling of NoopPacket packets
    if (Request.NO_OP.equals(string)) {
      throw new RuntimeException("Should never be here");//new NoopPacket();
    }
    try {
      long t = System.nanoTime();
      JSONObject json = new JSONObject(string);
      if (Util.oneIn(1000)) {
        DelayProfiler.updateDelayNano("jsonificationApp", t);
      }
      Request request = (Request) Packet.createInstance(json, nodeConfig);
      return request;
    } catch (JSONException e) {
      throw new RequestParseException(e);
    }
  }

  /**
   * This method avoids an unnecessary restringification (as is the case with
   * {@link #getRequest(String)} above) by decoding the JSON, stamping it with
   * the sender information, and then creating a packet out of it.
   *
   * @param msgBytes
   * @param header
   * @param unstringer
   * @return Request constructed from msgBytes.
   * @throws RequestParseException
   */
  public static Request getRequestStatic(byte[] msgBytes, NIOHeader header,
          Stringifiable<String> unstringer) throws RequestParseException {
    Request request = null;
    try {
      long t = System.nanoTime();
      if (JSONPacket.couldBeJSON(msgBytes)) {
        JSONObject json = new JSONObject(new String(msgBytes,
                NIOHeader.CHARSET));
        MessageExtractor.stampAddressIntoJSONObject(header.sndr,
                header.rcvr, json);
        request = (Request) Packet.createInstance(json, unstringer);
      } else {
        // parse non-JSON byteified form
        return fromBytes(msgBytes);
      }
      if (Util.oneIn(100)) {
        DelayProfiler.updateDelayNano(
                "getRequest." + request.getRequestType(), t);
      }
    } catch (JSONException | UnsupportedEncodingException e) {
      throw new RequestParseException(e);
    }
    return request;
  }

  /**
   * This method should invert the implementation of the
   * {@link Byteable#toBytes()} method for GNSApp packets.
   *
   * @param msgBytes
   * @return a request
   * @throws RequestParseException
   */
  private static Request fromBytes(byte[] msgBytes)
          throws RequestParseException {
    switch (Packet.PacketType.getPacketType(ByteBuffer.wrap(msgBytes)
            .getInt())) {
      case COMMAND:
        return new CommandPacket(msgBytes);
      /* Currently only CommandPacket is Byteable, so we shouldn't come
			 * here for anything else. */
      default:
        throw new RequestParseException(new RuntimeException(
                "Unrecognizable request type"));
    }
  }

  /**
   *
   * @param msgBytes
   * @param header
   * @return the request
   * @throws RequestParseException
   */
  @Override
  public Request getRequest(byte[] msgBytes, NIOHeader header)
          throws RequestParseException {
    return getRequestStatic(msgBytes, header, nodeConfig);
  }

  /**
   *
   * @return a set of packet types
   */
  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return new HashSet<>(Arrays.asList(PACKET_TYPES));
  }

  /**
   *
   * @return a set of packet types
   */
  @Override
  public Set<IntegerPacketType> getMutualAuthRequestTypes() {
    Set<IntegerPacketType> maTypes = new HashSet<>(Arrays.asList(MUTUAL_AUTH_TYPES));
    if (InternalCommandPacket.SEPARATE_INTERNAL_TYPE) {
      maTypes.add(PacketType.INTERNAL_COMMAND);
    }
    return maTypes;
  }

  /**
   *
   * @param request
   * @return true if the command successfully executes
   */
  @Override
  public boolean execute(Request request) {
    return this.execute(request, false);
  }

  private final static ArrayList<ColumnField> curValueRequestFields = new ArrayList<>();

  static {
    curValueRequestFields.add(NameRecord.VALUES_MAP);
  }

  /**
   *
   * @param name
   * @return the record
   */
  @Override
  public String checkpoint(String name) {
    try {
      NameRecord nameRecord = NameRecord
              .getNameRecord(nameRecordDB, name);
      GNSConfig.getLogger().log(
              Level.FINE,
              "{0} getting state for {1} : {2} ",
              new Object[]{this, name,
                nameRecord.getValuesMap().getSummary()});
      return nameRecord.getValuesMap().toString();
    } catch (RecordNotFoundException e) {
      // the above RecordNotFoundException is a normal result
    } catch (FieldNotFoundException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Field not found exception: {0}", e.getMessage());
      e.printStackTrace();
    } catch (FailedDBOperationException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "State not read from DB: {0}", e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Updates the state for the given named record.
   *
   * @param name
   * @param state
   * @return true if we were able to updateEntireRecord the state
   */
  @Override
  public boolean restore(String name, String state) {
    GNSConfig.getLogger().log(Level.FINE,
            "{0} updating {1} with state [{2}]",
            new Object[]{this, name, Util.truncate(state, 32, 32)});
    try {
      if (state == null) {
        // If state is null the only thing it means is that we need to
        // delete
        // the record. If the record does not exists this is just a
        // noop.
        NameRecord.removeNameRecord(nameRecordDB, name);
      } else // state does not equal null so we either create a new record
      // or update the existing one
       if (!NameRecord.containsRecord(nameRecordDB, name)) {
          // create a new record
          try {
            ValuesMap valuesMap = new ValuesMap(new JSONObject(state));
            NameRecord nameRecord = new NameRecord(nameRecordDB, name,
                    valuesMap);
            NameRecord.addNameRecord(nameRecordDB, nameRecord);
          } catch (RecordExistsException | JSONException e) {
            GNSConfig.getLogger().log(Level.SEVERE,
                    "Problem updating state: {0}", e.getMessage());
          }
        } else { // update the existing record
          try {
            NameRecord nameRecord = NameRecord.getNameRecord(
                    nameRecordDB, name);
            nameRecord
                    .updateState(new ValuesMap(new JSONObject(state)));
          } catch (JSONException | FieldNotFoundException | RecordNotFoundException | FailedDBOperationException e) {
            GNSConfig.getLogger().log(Level.SEVERE,
                    "Problem updating state: {0}", e.getMessage());
          }
        }
      return true;
    } catch (FailedDBOperationException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Failed update exception: {0}", e.getMessage());
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Returns a stop request packet.
   *
   * @param name
   * @param epoch
   * @return the stop request packet
   */
  @Override
  public ReconfigurableRequest getStopRequest(String name, int epoch) {
    /* A nontrivial stop request is not needed unless the app wants to do
		 * specific cleanup activities in case of a stop. The default action of
		 * restoring state to null is automatically done by gigapaxos. */
    return null; // new StopPacket(name, epoch);
  }

  /**
   *
   * @param name
   * @param epoch
   * @return the state
   */
  @Override
  public String getFinalState(String name, int epoch) {
    throw new RuntimeException("This method should not have been called");
  }

  /**
   *
   * @param name
   * @param epoch
   * @param state
   */
  @Override
  public void putInitialState(String name, int epoch, String state) {
    throw new RuntimeException("This method should not have been called");
  }

  /**
   *
   * @param name
   * @param epoch
   * @return the state
   */
  @Override
  public boolean deleteFinalState(String name, int epoch) {
    throw new RuntimeException("This method should not have been called");
  }

  /**
   *
   * @param name
   * @return the epoch
   */
  @Override
  public Integer getEpoch(String name) {
    throw new RuntimeException("This method should not have been called");
  }

  //
  // GnsApplicationInterface implementation
  //
  @Override
  public String getNodeID() {
    return nodeID;
  }

  @Override
  public InetSocketAddress getNodeAddress() {
    return nodeAddress;
  }

  @Override
  public BasicRecordMap getDB() {
    return nameRecordDB;
  }

  /**
   *
   */
  protected static final boolean DELEGATE_CLIENT_MESSAGING = true;

  /**
   * Delegates client messaging to gigapaxos.
   *
   * @param responseJSON
   * @throws java.io.IOException
   */
  @Override
  public void sendToClient(Request response, JSONObject responseJSON)
          throws IOException {

    if (DELEGATE_CLIENT_MESSAGING) {
      assert (response instanceof ClientRequest);
      Request originalRequest = this.outstanding
              .remove(((RequestIdentifier) response).getRequestID());
      assert (originalRequest != null && originalRequest instanceof BasicPacketWithClientAddress) : ((ClientRequest) response)
              .getSummary();
      if (originalRequest != null
              && originalRequest instanceof BasicPacketWithClientAddress) {
        ((BasicPacketWithClientAddress) originalRequest)
                .setResponse((ClientRequest) response);
        incrResponseCount((ClientRequest) response);
      }
      GNSConfig
              .getLogger()
              .log(Level.FINE,
                      "{0} set response {1} for requesting client {2} for request {3}",
                      new Object[]{
                        this,
                        response,
                        ((BasicPacketWithClientAddress) originalRequest)
                        .getClientAddress(),
                        originalRequest.getSummary()});
      return;
    } // else
  }

  /**
   * @param originalRequest
   * @param response
   * @param responseJSON
   * @throws IOException
   */
  public void sendToClient(CommandPacket originalRequest, Request response, JSONObject responseJSON)
          throws IOException {

    if (DELEGATE_CLIENT_MESSAGING) {
      if (enqueueCommand()) {
        this.outstanding.remove(((RequestIdentifier) response)
                .getRequestID());
      }

      assert (originalRequest != null && originalRequest instanceof BasicPacketWithClientAddress) : ((ClientRequest) response)
              .getSummary();

      ((BasicPacketWithClientAddress) originalRequest)
              .setResponse((ClientRequest) response);
      incrResponseCount((ClientRequest) response);

      GNSConfig
              .getLogger()
              .log(Level.FINE,
                      "{0} set response {1} for requesting client {2} for request {3}",
                      new Object[]{
                        this,
                        response.getSummary(),
                        ((BasicPacketWithClientAddress) originalRequest)
                        .getClientAddress(),
                        originalRequest.getSummary()});
      return;
    } // else
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + ":" + this.nodeID;
  }

  @Override
  public void sendToAddress(InetSocketAddress address, JSONObject msg) throws IOException {
    messenger.sendToAddress(address, msg);
  }

  @Override
  public ActiveCodeHandler getActiveCodeHandler() {
    return activeCodeHandler;
  }

  @Override
  public ClientRequestHandlerInterface getRequestHandler() {
    return requestHandler;
  }

  /**
   * @return ContextServiceGNSInterface
   */
  public ContextServiceGNSInterface getContextServiceGNSClient() {
    return contextServiceGNSClient;
  }

  private void startDNS() throws SecurityException, SocketException,
          UnknownHostException {
    try {
      if (Config.getGlobalBoolean(GNSConfig.GNSC.DNS_GNS_ONLY)) {
        dnsTranslator = new DnsTranslator(
                Inet4Address.getByName("0.0.0.0"), 53, requestHandler);
        dnsTranslator.start();
      } else if (Config.getGlobalBoolean(GNSConfig.GNSC.DNS_ONLY)) {
        if (Config.getGlobalString(GNSConfig.GNSC.GNS_SERVER_IP) == GNSConfig.NONE) {
          GNSConfig
                  .getLogger()
                  .severe("FAILED TO START DNS SERVER: GNS Server IP must be specified");
          return;
        }
        GNSConfig
                .getLogger()
                .info("GNS Server IP"
                        + Config.getGlobalString(GNSConfig.GNSC.GNS_SERVER_IP));
        udpDnsServer = new UdpDnsServer(
                Inet4Address.getByName("0.0.0.0"), 53, 
                Config.getGlobalString(GNSConfig.GNSC.DNS_UPSTREAM_SERVER_IP),
                Config.getGlobalString(GNSConfig.GNSC.GNS_SERVER_IP),
                requestHandler);
        udpDnsServer.start();
      } else {
        udpDnsServer = new UdpDnsServer(
                Inet4Address.getByName("0.0.0.0"), 53, 
                Config.getGlobalString(GNSConfig.GNSC.DNS_UPSTREAM_SERVER_IP),
                null, requestHandler);
        udpDnsServer.start();
      }
    } catch (BindException e) {
      GNSConfig.getLogger().warning(
              "Not running DNS Service because it needs root permission! "
              + "If you want DNS run the server using sudo.");
    }
  }

}
