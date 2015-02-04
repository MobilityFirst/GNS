package edu.umass.cs.gns.newApp.test;

import edu.umass.cs.gns.reconfiguration.examples.*;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.JSONPacket;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.util.MyLogger;

/**
 * @author Westy
 * @param <NodeIDType>
 */
public class ReconfigurableClient<NodeIDType> {

  private final InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig;
  private final JSONMessenger<NodeIDType> messenger;
  private final ConcurrentHashMap<String, Boolean> exists = new ConcurrentHashMap<String, Boolean>();

  private Logger log = Logger.getLogger(getClass().getName());

  ReconfigurableClient(InterfaceReconfigurableNodeConfig<NodeIDType> nc, JSONMessenger<NodeIDType> messenger) {
    this.nodeConfig = nc;
    this.messenger = messenger;
    messenger.addPacketDemultiplexer(new ClientPacketDemultiplexer());
  }

  private AppRequest makeRequest(String name, String value) {
    AppRequest req = new AppRequest(name, 0, value, AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
    return req;
  }

  private CreateServiceName makeCreateNameRequest(String name, String state) {
    CreateServiceName create = new CreateServiceName(null, name, 0, state);
    return create;
  }

  private DeleteServiceName makeDeleteNameRequest(String name, String state) {
    DeleteServiceName delete = new DeleteServiceName(null, name, 0);
    return delete;
  }

  private NodeIDType getRandomReplica() {
    int index = (int) (this.nodeConfig.getActiveReplicas().size() * Math.random());
    return (NodeIDType) (this.nodeConfig.getActiveReplicas().toArray()[index]);
  }

  private NodeIDType getRandomRCReplica() {
    int index = (int) (this.nodeConfig.getReconfigurators().size() * Math.random());
    return (NodeIDType) (this.nodeConfig.getReconfigurators().toArray()[index]);
  }

  private NodeIDType getFirstReplica() {
    return this.nodeConfig.getActiveReplicas().iterator().next();
  }

  private NodeIDType getFirstRCReplica() {
    return this.nodeConfig.getReconfigurators().iterator().next();
  }

  private void sendRequest(AppRequest req) throws JSONException, IOException, RequestParseException {
    NodeIDType id = (TestConfig.serverSelectionPolicy == TestConfig.ServerSelectionPolicy.FIRST ? this.getFirstReplica() : this.getRandomReplica());
    log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[]{"Sending ", req.getRequestType(), " to ", id, ":", this.nodeConfig.getNodeAddress(id), ":", this.nodeConfig.getNodePort(id), ": ", req});
    this.sendRequest(id, req.toJSONObject());
  }

  private void sendRequest(CreateServiceName req) throws JSONException, IOException {
    NodeIDType id = (TestConfig.serverSelectionPolicy == TestConfig.ServerSelectionPolicy.FIRST ? this.getFirstRCReplica() : this.getRandomRCReplica());
    log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[]{"Sending ", req.getSummary(), " to ", id, ":", this.nodeConfig.getNodeAddress(id), ":", this.nodeConfig.getNodePort(id), ": ", req});
    this.sendRequest(id, req.toJSONObject());
  }

  private void sendRequest(DeleteServiceName req) throws JSONException, IOException {
    NodeIDType id = (TestConfig.serverSelectionPolicy == TestConfig.ServerSelectionPolicy.FIRST ? this.getFirstRCReplica() : this.getRandomRCReplica());
    log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[]{"Sending ", req.getSummary(), " to ", id, ":", this.nodeConfig.getNodeAddress(id), ":", this.nodeConfig.getNodePort(id), ": ", req});
    this.sendRequest(id, req.toJSONObject());
  }

  private void sendRequest(NodeIDType id, JSONObject json) throws JSONException, IOException {
    this.messenger.send(new GenericMessagingTask<NodeIDType, Object>(id, json));
  }

  private class ClientPacketDemultiplexer extends AbstractPacketDemultiplexer {

    ClientPacketDemultiplexer() {
      this.register(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME);
      this.register(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME);
      this.register(AppRequest.PacketType.DEFAULT_APP_REQUEST);
    }

    @Override
    public boolean handleJSONObject(JSONObject json) {
      log.log(Level.FINEST, MyLogger.FORMAT[1], new Object[]{"Client received: ", json});
      try {
        switch (ReconfigurationPacket.getReconfigurationPacketType(json)) {
          case CREATE_SERVICE_NAME:
            CreateServiceName create = new CreateServiceName(json);
            log.log(Level.INFO, MyLogger.FORMAT[2], new Object[]{"App", " created ", create.getServiceName()});
            exists.put(create.getServiceName(), true);
            break;
          case DELETE_SERVICE_NAME:
            DeleteServiceName delete = new DeleteServiceName(json);
            log.log(Level.INFO, MyLogger.FORMAT[1], new Object[]{"App deleted ", delete.getServiceName()});
            exists.remove(delete.getServiceName());
            break;
          default:
            break;
        }
        AppRequest.PacketType type = AppRequest.PacketType.getPacketType(JSONPacket.getPacketType(json));
        if (type != null) {
          switch (AppRequest.PacketType.getPacketType(JSONPacket.getPacketType(json))) {
            case DEFAULT_APP_REQUEST:
              break;
            case APP_COORDINATION:
              // FIXME:
              throw new RuntimeException("Functionality not yet implemented");
            //break;
          }
        }
      } catch (JSONException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return true;
    }
  }

  public static void main(String[] args) throws IOException {
    String filename = Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info";
    GNSNodeConfig nodeConfig = new GNSNodeConfig(filename, "billy");
    ReconfigurableClient client = null;
    try {
      JSONMessenger<String> messenger = new JSONMessenger<String>(
              (new JSONNIOTransport<String>(null, nodeConfig, new PacketDemultiplexerDefault(),
                      true)).enableStampSenderPort());
      client = new ReconfigurableClient(nodeConfig, messenger);
      int numRequests = 2;
      String namePrefix = "name";
      String requestValuePrefix = "request_value";
      String initValue = "initial_value";

      client.sendRequest(client.makeCreateNameRequest(namePrefix + 0, initValue));
      while (client.exists.containsKey(namePrefix + 0));
      Thread.sleep(2000);
      for (int i = 0; i < numRequests; i++) {
        client.sendRequest(client.makeRequest(namePrefix + 0, requestValuePrefix + i));
        Thread.sleep(2000);
      }
      client.sendRequest(client.makeDeleteNameRequest(namePrefix + 0, initValue));
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
