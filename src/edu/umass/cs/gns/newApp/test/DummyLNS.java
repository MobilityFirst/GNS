package edu.umass.cs.gns.newApp.test;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
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
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.util.MyLogger;
import edu.umass.cs.gns.util.ValuesMap;

/**
 * @author Westy
 * @param <NodeIDType>
 */
public class DummyLNS<NodeIDType> {

  private final InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig;
  private final JSONMessenger<NodeIDType> messenger;
  private final ConcurrentHashMap<String, Boolean> exists = new ConcurrentHashMap<String, Boolean>();

  private Logger log = Logger.getLogger(getClass().getName());

  DummyLNS(InterfaceReconfigurableNodeConfig<NodeIDType> nc, JSONMessenger<NodeIDType> messenger) {
    this.nodeConfig = nc;
    this.messenger = messenger;
    messenger.addPacketDemultiplexer(new DUmmyLNSPacketDemultiplexer());
  }

  private CreateServiceName makeCreateNameRequest(String name, String state) {
    CreateServiceName create = new CreateServiceName(null, name, 0, state);
    return create;
  }

  private DeleteServiceName makeDeleteNameRequest(String name, String state) {
    DeleteServiceName delete = new DeleteServiceName(null, name, 0);
    return delete;
  }

  private UpdatePacket makeUpdateRequest(String name, String value) throws JSONException {
    UpdatePacket packet = new UpdatePacket(-1, -1, -1, name, null, null, null, -1,
            new ValuesMap(new JSONObject(value)), UpdateOperation.USER_JSON_REPLACE,
            null, "", 0, null, null, null);
    return packet;
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

  private void sendUpdateRequest(UpdatePacket req) throws JSONException, IOException, RequestParseException {
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

  private class DUmmyLNSPacketDemultiplexer extends AbstractPacketDemultiplexer {

    DUmmyLNSPacketDemultiplexer() {
      this.register(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME);
      this.register(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME);
      // From current app
      register(Packet.PacketType.UPDATE);
      register(Packet.PacketType.DNS);
      register(Packet.PacketType.SELECT_REQUEST);
      register(Packet.PacketType.SELECT_RESPONSE);
    }

    @Override
    public boolean handleJSONObject(JSONObject json) {
      log.log(Level.INFO, MyLogger.FORMAT[1], new Object[]{"************************* LNS received: ", json});
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
        final Packet.PacketType type = Packet.getPacketType(json);
        if (Config.debuggingEnabled) {
          GNS.getLogger().fine("MsgType " + type + " Msg " + json);
        }
        if (type != null) {
          switch (type) {
            case DNS:
              break;
            case UPDATE:
              break;
          }
        } else {

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
    GNSNodeConfig nodeConfig = new GNSNodeConfig(filename, true);
    DummyLNS localNameServer = null;
    try {
      JSONMessenger<String> messenger = new JSONMessenger<String>(
              (new JSONNIOTransport<String>(null, nodeConfig, new PacketDemultiplexerDefault(),
                      true)).enableStampSenderPort());
      localNameServer = new DummyLNS(nodeConfig, messenger);
      int numRequests = 2;
      String namePrefix = "name";
      String updateValue = "{"
              + "  \"name\": \"John\","
              + "  \"number\": \"%d\"}";
      String initValue = "initial_value";

      localNameServer.sendRequest(localNameServer.makeCreateNameRequest(namePrefix + 0, initValue));
      while (localNameServer.exists.containsKey(namePrefix + 0));
      Thread.sleep(2000);
      for (int i = 0; i < numRequests; i++) {

        localNameServer.sendUpdateRequest(localNameServer.makeUpdateRequest(namePrefix + 0, String.format(updateValue, i)));
        Thread.sleep(2000);
      }
      localNameServer.sendRequest(localNameServer.makeDeleteNameRequest(namePrefix + 0, initValue));
      localNameServer.messenger.stop();
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
    System.exit(0);
  }
}
