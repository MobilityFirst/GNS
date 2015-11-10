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
package edu.umass.cs.gnsserver.gnsApp.noopTest;

import edu.umass.cs.reconfiguration.examples.*;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.ActiveReplica;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureRCNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.MyLogger;

/**
 * @author Westy adapted from code originally by V. Arun
 */
public class TestReconfigurableClient {

  private final ReconfigurableNodeConfig<String> nodeConfig;
  private final JSONMessenger<String> messenger;
  private final ConcurrentHashMap<String, Boolean> exists = new ConcurrentHashMap<String, Boolean>();
  private Set<InetSocketAddress> activeReplicas = null;

  private Logger log = Logger.getLogger(getClass().getName());

  TestReconfigurableClient(ReconfigurableNodeConfig<String> nc,
          JSONMessenger<String> messenger) {
    this.nodeConfig = nc;
    this.messenger = messenger;
    messenger.addPacketDemultiplexer(new ClientPacketDemultiplexer());
  }

  private AppRequest makeRequest(String name, String value) {
    return new AppRequest(name, value,
            AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
  }

  private CreateServiceName makeCreateNameRequest(String name, String state) {
    return new CreateServiceName(null, name, 0, state);
  }

  private DeleteServiceName makeDeleteNameRequest(String name, String state) {
    return new DeleteServiceName(null, name, 0);
  }

  private RequestActiveReplicas makeRequestActiveReplicas(String name) {
    return new RequestActiveReplicas(null, name, 0);
  }

  // active replicas should not be hard-coded
  private InetSocketAddress getRandomActiveReplica() {
    // int index =
    // (int)(this.nodeConfig.getActiveReplicas().size()*Math.random());
    // return
    // (String)(this.nodeConfig.getActiveReplicas().toArray()[index]);
    int index = (int) (this.getActiveReplicas().size() * Math.random());
    return (InetSocketAddress) (this.getActiveReplicas().toArray()[index]);

  }

  private InetSocketAddress getRandomRCReplica() {
    int index = (int) (this.nodeConfig.getReconfigurators().size() * Math
            .random());
    String id = (String) (this.nodeConfig.getReconfigurators().toArray()[index]);
    return new InetSocketAddress(this.nodeConfig.getNodeAddress(id),
            ActiveReplica.getClientFacingPort(this.nodeConfig
                    .getNodePort(id)));
  }

  private InetSocketAddress getFirstActiveReplica() {
    return this.getActiveReplicas().iterator().next();
  }

  private InetSocketAddress getFirstRCReplica() {
    String id = this.nodeConfig.getReconfigurators().iterator().next();
    return new InetSocketAddress(this.nodeConfig.getNodeAddress(id),
            ActiveReplica.getClientFacingPort(this.nodeConfig
                    .getNodePort(id)));
  }

  private void sendRequest(AppRequest req) throws JSONException, IOException,
          RequestParseException {
    InetSocketAddress id = (this.getRandomActiveReplica());
    log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[]{
      "Sending ", req.getRequestType(), " to ", id, ":", (id), ": ",
      req});
    this.exists.put(req.getServiceName(), true);
    this.sendRequest(id, req.toJSONObject());
  }

  private void sendRequest(BasicReconfigurationPacket<?> req)
          throws JSONException, IOException {
    InetSocketAddress id = (this.getRandomRCReplica());
    log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""),
            new Object[]{"Sending ", req.getSummary(), " to ", id, ":",
              (id), ": ", req});
    this.exists.put(req.getServiceName(), true);
    this.sendRequest(id, req.toJSONObject());
  }

  private void sendRequest(InetSocketAddress id, JSONObject json)
          throws JSONException, IOException {
    // modify
    this.messenger.sendToAddress(id, json);
  }

  private class ClientPacketDemultiplexer extends
          AbstractJSONPacketDemultiplexer {

    ClientPacketDemultiplexer() {
      this.register(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME);
      this.register(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME);
      this.register(AppRequest.PacketType.DEFAULT_APP_REQUEST);
      this.register(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS);
      this.register(ReconfigurationPacket.PacketType.RECONFIGURE_RC_NODE_CONFIG);
    }

    @Override
    public boolean handleMessage(JSONObject json) {
      log.log(Level.FINEST, MyLogger.FORMAT[1], new Object[]{
        "Client received: ", json});
      try {
        ReconfigurationPacket.PacketType rcType = ReconfigurationPacket
                .getReconfigurationPacketType(json);
        if (rcType != null) {
          switch (ReconfigurationPacket
                  .getReconfigurationPacketType(json)) {
            case CREATE_SERVICE_NAME:
              CreateServiceName create = new CreateServiceName(json);
              log.log(Level.INFO, MyLogger.FORMAT[2], new Object[]{
                "App", " created ", create.getServiceName()});
              exists.remove(create.getServiceName());
              break;
            case DELETE_SERVICE_NAME:
              DeleteServiceName delete = new DeleteServiceName(json);
              log.log(Level.INFO, MyLogger.FORMAT[1], new Object[]{
                "App deleted ", delete.getServiceName()});
              exists.remove(delete.getServiceName());
              break;
            case REQUEST_ACTIVE_REPLICAS:
              RequestActiveReplicas reqActives = new RequestActiveReplicas(
                      json);
              log.log(Level.INFO, MyLogger.FORMAT[3],
                      new Object[]{
                        "App received active replicas for",
                        reqActives.getServiceName(), ":",
                        reqActives.getActives()});
              activeReplicas = reqActives.getActives();
              exists.remove(reqActives.getServiceName());
              break;
            case RECONFIGURE_RC_NODE_CONFIG:
              ReconfigureRCNodeConfig<String> rcnc = new ReconfigureRCNodeConfig<String>(
                      json, new StringifiableDefault<String>(0));
              log.log(Level.INFO,
                      MyLogger.FORMAT[3],
                      new Object[]{
                        "Received node config change confirmation for adding",
                        rcnc.newlyAddedNodes, "and deleting",
                        rcnc.deletedNodes});
              exists.remove(rcnc.getServiceName());
              break;

            default:
              break;
          }
        }
        AppRequest.PacketType type = AppRequest.PacketType
                .getPacketType(JSONPacket.getPacketType(json));
        if (type != null) {
          switch (AppRequest.PacketType.getPacketType(JSONPacket
                  .getPacketType(json))) {
            case DEFAULT_APP_REQUEST:
              AppRequest request = new AppRequest(json);
              log.log(Level.INFO,
                      MyLogger.FORMAT[1],
                      new Object[]{
                        "App executed request",
                        request.getRequestID() + ":"
                        + request.getValue()});
              exists.remove(request.getServiceName());
              break;
            case ANOTHER_APP_REQUEST:
              throw new RuntimeException(
                      "Client received unexpected APP_COORDINATION message");
          }
        }
      } catch (JSONException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return true;
    }
  }

  Set<InetSocketAddress> getActiveReplicas() {
    return this.activeReplicas;
  }

  private static boolean includeAddDeleteReconfiguratorTest = false;

  /**
   * Simple test client for the reconfiguration package. Clients only know the
   * set of all reconfigurators, not active replicas for any name. All
   * information about active replicas for a name is obtained from
   * reconfigurators.
   *
   * @param args
   * @throws java.io.IOException
   */
  public static void main(String[] args) throws IOException {
    TestReconfigurableNodeConfig nc = new TestReconfigurableNodeConfig(args[0]);
    TestReconfigurableClient client = null;
    try {
      /*
       * Client can only send/receive clear text or do server-only
       * authentication
       */
      JSONMessenger<String> messenger = new JSONMessenger<String>(
              (new JSONNIOTransport<String>(null, nc,
                      new PacketDemultiplexerDefault(),
                      ReconfigurationConfig.getClientSSLMode())));
      client = new TestReconfigurableClient(nc, messenger);

      for (String rc : client.nodeConfig.getReconfigurators()) {
        System.out.println("Node " + rc
                + " address: " + client.nodeConfig.getNodeAddress(rc)
                + " port: " + client.nodeConfig.getNodePort(rc));
      }
      int numRequests = 2;
      String namePrefix = "name";
      String requestValuePrefix = "request_value";
      long sleepTime = 1600;

      String initValue = "initial_value";
      client.sendRequest(client.makeRequestActiveReplicas(namePrefix + 0));
      while (client.exists.containsKey(namePrefix + 0))
				;

      // active replicas for name initially don't exist
      assert (client.getActiveReplicas() == null || client
              .getActiveReplicas().isEmpty());

      // create name
      client.sendRequest(client.makeCreateNameRequest(namePrefix + 0,
              initValue));
      while (client.exists.containsKey(namePrefix + 0))
				;

      // verify active replicas for name now exist
      client.sendRequest(client.makeRequestActiveReplicas(namePrefix + 0));
      while (client.exists.containsKey(namePrefix + 0))
				;
      assert (client.getActiveReplicas() != null && !client
              .getActiveReplicas().isEmpty());

      // send a stream of app requests
      for (int i = 0; i < numRequests; i++) {
        client.sendRequest(client.makeRequest(namePrefix + 0,
                requestValuePrefix + i));
        while (client.exists.containsKey(namePrefix + 0))
					;
        Thread.sleep(sleepTime);
      }

      // request current active replicas (possibly reconfigured)
      client.sendRequest(client.makeRequestActiveReplicas(namePrefix + 0));
      while (client.exists.containsKey(namePrefix + 0))
				;

      // delete name
      client.sendRequest(client.makeDeleteNameRequest(namePrefix + 0,
              initValue));
      while (client.exists.containsKey(namePrefix + 0))
				;
      Thread.sleep(sleepTime);

      // verify that active replicas for name now don't exist
      client.sendRequest(client.makeRequestActiveReplicas(namePrefix + 0));
      while (client.exists.containsKey(namePrefix + 0))
				;
      assert (client.getActiveReplicas() == null || client
              .getActiveReplicas().isEmpty());

      // add RC node
      if (includeAddDeleteReconfiguratorTest) {
        client.sendRequest(new ReconfigureRCNodeConfig<Integer>(null, 1103,
                new InetSocketAddress(InetAddress.getByName("localhost"),
                        3103)));
        while (client.exists
                .containsKey(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
                        .toString()))
				;
        Thread.sleep(1000);

        // delete RC node
        HashSet<Integer> deleted = new HashSet<Integer>();
        deleted.add(1103);
        client.sendRequest(new ReconfigureRCNodeConfig<Integer>(null, null,
                deleted));
        while (client.exists
                .containsKey(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
                        .toString()));
        Thread.sleep(500);
      }

      client.messenger.stop();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } catch (JSONException je) {
      je.printStackTrace();
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } catch (RequestParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
