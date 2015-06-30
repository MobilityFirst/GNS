package edu.umass.cs.reconfiguration.examples.noop;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.InterfaceSSLMessenger;
import edu.umass.cs.reconfiguration.InterfaceReconfigurable;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 */
public class NoopApp implements InterfaceReplicable, InterfaceReconfigurable {

	private static final String DEFAULT_INIT_STATE = "";

	private class AppData {
		final String name;
		String state = DEFAULT_INIT_STATE;

		AppData(String name, String state) {
			this.name = name;
			this.state = state;
		}

		void setState(String state) {
			this.state = state;
		}

		String getState() {
			return this.state;
		}
	}

	private final int myID;
	private final HashMap<String, AppData> appData = new HashMap<String, AppData>();
	// only address based communication needed in app
	private InterfaceSSLMessenger<?, JSONObject> messenger;

	/**
	 * @param id
	 */
	public NoopApp(int id) {
		this.myID = id;
	}

	// Need a messenger mainly to send back responses to the client.
	protected void setMessenger(InterfaceSSLMessenger<?, JSONObject> msgr) {
		this.messenger = msgr;
	}

	// FIXME: return response to client
	@Override
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient) {
		if(request.toString().equals(InterfaceRequest.NO_OP)) return true;
		try {
			switch ((AppRequest.PacketType) (request.getRequestType())) {
			case DEFAULT_APP_REQUEST:
				return processRequest((NoopAppRequest) request);
			default:
				break;
			}
		} catch (RequestParseException rpe) {
			rpe.printStackTrace();
		}
		return false;
	}

	private boolean processRequest(NoopAppRequest request) {
		if (request.getServiceName() == null)
			return true; // no-op
		if (request.isStop())
			return processStopRequest(request);
		AppData data = this.appData.get(request.getServiceName());
		if (data == null) {
			System.out.println("App-" + myID + " has no record for "
					+ request.getServiceName() + " for " + request);
			return false;
		}
		assert (data != null);
		data.setState(request.getValue());
		this.appData.put(request.getServiceName(), data);
		System.out.println("App-" + myID + " wrote " + data.name
				+ " with state " + data.getState());
		sendResponse(request);
		return true;
	}

	private void sendResponse(NoopAppRequest request) {
		assert (this.messenger != null && this.messenger.getClientMessenger() != null);
		if (this.messenger == null || request.getEntryReplica() != this.myID)
			return;
		InetSocketAddress sockAddr = new InetSocketAddress(
				request.getSenderAddress(), request.getSenderPort());
		try {
			// SSL: invoke clientMessenger here
			this.messenger.getClientMessenger().sendToAddress(sockAddr,
					request.toJSONObject());
		} catch (JSONException | IOException e) {
			e.printStackTrace();
		}
	}

	// no-op
	private boolean processStopRequest(NoopAppRequest request) {
		// AppData data = this.appData.get(request.getServiceName());
		// if (data == null) return false;
		return true;
	}

	@Override
	public InterfaceRequest getRequest(String stringified)
			throws RequestParseException {
		NoopAppRequest request = null;
		if (stringified.equals(InterfaceRequest.NO_OP)) {
			System.out.println("************** returning NO_OP");
			return this.getNoopRequest();
		}
		try {
			request = new NoopAppRequest(new JSONObject(stringified));
		} catch (JSONException je) {
			Reconfigurator.getLogger().info(
					myID + " unable to parse request " + stringified);
			throw new RequestParseException(je);
		}
		return request;
	}

	/*
	 * This is a special no-op request unlike any other NoopAppRequest.
	 */
	private InterfaceRequest getNoopRequest() {
		return new NoopAppRequest(null, 0, 0, InterfaceRequest.NO_OP,
				AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
	}

	private static AppRequest.PacketType[] types = {
			AppRequest.PacketType.DEFAULT_APP_REQUEST,
			AppRequest.PacketType.APP_COORDINATION };

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>(Arrays.asList(types));
	}

	@Override
	public boolean handleRequest(InterfaceRequest request) {
		return this.handleRequest(request, false);
	}

	@Override
	public String getState(String name) {
		AppData data = this.appData.get(name);
		return data != null ? data.getState() : null;
	}

	@Override
	public boolean updateState(String name, String state) {
		AppData data = this.appData.get(name);
		/*
		 * If no previous state, set epoch to initial epoch, otherwise
		 * putInitialState will be called.
		 */

		if (data == null && state != null) {
			data = new AppData(name, state);
			System.out.println(">>>App-" + myID + " creating " + name
					+ " with state " + state);
		} else if (state == null) {
			if(data!=null) System.out.println("App-" + myID + " deleting " + name
					+ " with state " + data.state);
			this.appData.remove(name);
			assert (this.appData.get(name) == null);
		} else if (data != null && state != null) {
			System.out.println("App-" + myID + " updating " + name
					+ " with state " + state);
			data.state = state;
		} else
			// do nothing when data==null && state==null
			;
		if (state != null)
			this.appData.put(name, data);

		return true;
	}

	@Override
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
		return new NoopAppRequest(name, epoch,
				(int) (Math.random() * Integer.MAX_VALUE), "",
				AppRequest.PacketType.DEFAULT_APP_REQUEST, true);
	}

	/*
	 * The methods below are unnecessary as is InterfaceReconfigurable when the
	 * underlying app coordinator is paxos.
	 */
	@Override
	public String getFinalState(String name, int epoch) {
		throw new RuntimeException("This method should not have been called");
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		throw new RuntimeException("This method should not have been called");
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		throw new RuntimeException("This method should not have been called");
	}

	@Override
	public Integer getEpoch(String name) {
		throw new RuntimeException("This method should not have been called");
	}
}
