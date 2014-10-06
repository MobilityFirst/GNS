package edu.umass.cs.gns.reconfiguration.examples;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.JSONPacket;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;

/**
@author V. Arun
 */
public class ReconfigurableClient {

	private final InterfaceReconfigurableNodeConfig<Integer> nodeConfig;
	private final JSONMessenger<Integer> messenger;
	private final ConcurrentHashMap<String,Boolean> exists = new ConcurrentHashMap<String,Boolean>();

	private Logger log = Logger.getLogger(getClass().getName());

	ReconfigurableClient(InterfaceReconfigurableNodeConfig<Integer> nc, JSONMessenger<Integer> messenger) {
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

	private int getRandomReplica() {
		int index = (int)(this.nodeConfig.getActiveReplicas().size()*Math.random());
		return (Integer)(this.nodeConfig.getActiveReplicas().toArray()[index]);
	}
	private int getFirstReplica() {
		return this.nodeConfig.getActiveReplicas().iterator().next();
	}
	private void sendRequest(AppRequest req) throws JSONException, IOException {
		int id = (TestConfig.serverSelectionPolicy==TestConfig.ServerSelectionPolicy.FIRST ? this.getFirstReplica() : this.getRandomReplica());
		System.out.println("Sending to " + id + ":" + this.nodeConfig.getNodeAddress(id) + ":"+this.nodeConfig.getNodePort(id) + ": "+ req);
		this.sendRequest(id, req.toJSONObject());
	}
	private void sendRequest(CreateServiceName req) throws JSONException, IOException {
		int id = (TestConfig.serverSelectionPolicy==TestConfig.ServerSelectionPolicy.FIRST ? this.getFirstReplica() : this.getRandomReplica());
		System.out.println("Sending to " + id + ":" + this.nodeConfig.getNodeAddress(id) + ": "+this.nodeConfig.getNodePort(id) + req);
		this.sendRequest(id, req.toJSONObject());
	}

	private void sendRequest(Integer id, JSONObject json) throws JSONException, IOException {
		this.messenger.send(new GenericMessagingTask<Integer,Object>(id, json));
	}

	private class ClientPacketDemultiplexer extends AbstractPacketDemultiplexer {

		ClientPacketDemultiplexer() {
			this.register(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME);
			this.register(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME);
			this.register(AppRequest.PacketType.DEFAULT_APP_REQUEST);
		}
		@Override
		public boolean handleJSONObject(JSONObject json) {
			log.info("Client received: " + json);
			try {
				switch(ReconfigurationPacket.getReconfigurationPacketType(json)) {
				case CREATE_SERVICE_NAME:
					CreateServiceName create = new CreateServiceName(json);
					System.out.println("App"+messenger.getMyID()+" created " +create.getServiceName());
					exists.put(create.getServiceName(), true);
					break;
				case DELETE_SERVICE_NAME:
					DeleteServiceName delete = new DeleteServiceName(json);
					System.out.println("App created " +delete.getServiceName());
					exists.remove(delete.getServiceName());
					break;
				default: break;
				}
				AppRequest.PacketType type = AppRequest.PacketType.getPacketType(JSONPacket.getPacketType(json));
				if(type!=null) {
					switch(AppRequest.PacketType.getPacketType(JSONPacket.getPacketType(json))) {
					case DEFAULT_APP_REQUEST:
						break;
					}
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return true;
		}
	}

	public static void main(String[] args) {
		ReconfigurableSampleNodeConfig nc = new ReconfigurableSampleNodeConfig();
		nc.localSetup(TestConfig.getNodes());
		ReconfigurableClient client = null;
		try {
			JSONMessenger<Integer> messenger = new JSONMessenger<Integer>(
					(new JSONNIOTransport<Integer>(null, nc, new PacketDemultiplexerDefault(), 
							true)).enableStampSenderPort());
			client = new ReconfigurableClient(nc, messenger);
			int numRequests = 1;
			String namePrefix = "name";
			String requestValuePrefix = "request_value";
			String initValue = "initial_value";

			client.sendRequest(client.makeCreateNameRequest(namePrefix+0, initValue));
			while(client.exists.containsKey(namePrefix+0));
			Thread.sleep(1000);
			for(int i=0; i<numRequests; i++) {
				client.sendRequest(client.makeRequest(namePrefix+i, requestValuePrefix+i));
				Thread.sleep(1000);
			}
			Thread.sleep(1000);
			client.messenger.stop();
		} catch(IOException ioe) {
			ioe.printStackTrace();
		} catch(JSONException je) {
			je.printStackTrace();
		} catch(InterruptedException ie) {
			ie.printStackTrace();
		}

	}
}
