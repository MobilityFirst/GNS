package edu.umass.cs.gns.reconfiguration.examples;

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
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.gns.util.MyLogger;

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
		return new AppRequest(name, value, AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
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

	private int getRandomReplica() {
		int index = (int)(this.nodeConfig.getActiveReplicas().size()*Math.random());
		return (Integer)(this.nodeConfig.getActiveReplicas().toArray()[index]);
	}
	private int getRandomRCReplica() {
		int index = (int)(this.nodeConfig.getReconfigurators().size()*Math.random());
		return (Integer)(this.nodeConfig.getReconfigurators().toArray()[index]);
	}

	private int getFirstReplica() {
		return this.nodeConfig.getActiveReplicas().iterator().next();
	}
	private int getFirstRCReplica() {
		return this.nodeConfig.getReconfigurators().iterator().next();
	}

	private void sendRequest(AppRequest req) throws JSONException, IOException, RequestParseException {
		int id = (TestConfig.serverSelectionPolicy==TestConfig.ServerSelectionPolicy.FIRST ? this.getFirstReplica() : this.getRandomReplica());
		log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[]{"Sending " , req.getRequestType(), " to ", id , ":" , this.nodeConfig.getNodeAddress(id) , ":" , this.nodeConfig.getNodePort(id) , ": ", req});
		this.exists.put(req.getServiceName(), true);
		this.sendRequest(id, req.toJSONObject());
	}
	private void sendRequest(BasicReconfigurationPacket<?> req) throws JSONException, IOException {
		int id = (TestConfig.serverSelectionPolicy==TestConfig.ServerSelectionPolicy.FIRST ? this.getFirstRCReplica() : this.getRandomRCReplica());
		log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[]{"Sending " , req.getSummary(), " to ", id , ":" , this.nodeConfig.getNodeAddress(id) , ":" , this.nodeConfig.getNodePort(id) , ": ", req});
		this.exists.put(req.getServiceName(), true);
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
			this.register(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS);
		}
		@Override
		public boolean handleJSONObject(JSONObject json) {
			log.log(Level.FINEST, MyLogger.FORMAT[1], new Object[]{"Client received: " , json});
			try {
				ReconfigurationPacket.PacketType rcType = ReconfigurationPacket.getReconfigurationPacketType(json);
				if(rcType!=null) {
					switch (ReconfigurationPacket
							.getReconfigurationPacketType(json)) {
					case CREATE_SERVICE_NAME:
						CreateServiceName create = new CreateServiceName(json);
						log.log(Level.INFO, MyLogger.FORMAT[2], new Object[] {
								"App", " created ", create.getServiceName() });
						exists.remove(create.getServiceName());
						break;
					case DELETE_SERVICE_NAME:
						DeleteServiceName delete = new DeleteServiceName(json);
						log.log(Level.INFO, MyLogger.FORMAT[1], new Object[] {
								"App deleted ", delete.getServiceName() });
						exists.remove(delete.getServiceName());
						break;
					case REQUEST_ACTIVE_REPLICAS:
						RequestActiveReplicas reqActives = new RequestActiveReplicas(
								json);
						log.log(Level.INFO, MyLogger.FORMAT[3],
								new Object[] { "App received active replicas for",
										reqActives.getServiceName(), ":",
										reqActives.getActives() });
						exists.remove(reqActives.getServiceName());
						break;
					default:
						break;
					}
				}
				AppRequest.PacketType type = AppRequest.PacketType.getPacketType(JSONPacket.getPacketType(json));
				if(type!=null) {
					switch(AppRequest.PacketType.getPacketType(JSONPacket.getPacketType(json))) {
					case DEFAULT_APP_REQUEST:
						AppRequest request = new AppRequest(json);
						log.log(Level.INFO,
								MyLogger.FORMAT[1],
								new Object[] {
										"App executed request",
										request.getRequestID() + ":"
												+ request.getValue() });
						exists.remove(request.getServiceName());
						break;
					case APP_COORDINATION:
						throw new RuntimeException("Client received unexpected APP_COORDINATION message");
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
			int numRequests = 2;
			String namePrefix = "name";
			String requestValuePrefix = "request_value";
			String initValue = "initial_value";

			client.sendRequest(client.makeCreateNameRequest(namePrefix+0, initValue));
			while(client.exists.containsKey(namePrefix+0));
			for(int i=0; i<numRequests; i++) {
				client.sendRequest(client.makeRequest(namePrefix+0, requestValuePrefix+i));
				while(client.exists.containsKey(namePrefix+0));
				Thread.sleep(1000);
			}
			client.sendRequest(client.makeRequestActiveReplicas(namePrefix+0));
			while(client.exists.containsKey(namePrefix+0));
			client.sendRequest(client.makeDeleteNameRequest(namePrefix+0, initValue));
			client.messenger.stop();
		} catch(IOException ioe) {
			ioe.printStackTrace();
		} catch(JSONException je) {
			je.printStackTrace();
		} catch(InterruptedException ie) {
			ie.printStackTrace();
		} catch (RequestParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
