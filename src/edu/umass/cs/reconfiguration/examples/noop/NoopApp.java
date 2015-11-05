/*
 * Copyright (c) 2015 University of Massachusetts
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
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.examples.noop;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.AppRequest.ResponseCodes;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 * 
 *         A simple no-op application example.
 */
public class NoopApp implements Replicable, Reconfigurable,
		ClientMessenger {

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

	private String myID; // used only for pretty printing
	private final HashMap<String, AppData> appData = new HashMap<String, AppData>();
	// only address based communication needed in app
	private SSLMessenger<?, JSONObject> messenger;

	/**
	 * Default constructor used to create app replica via reflection.
	 */
	public NoopApp() {
	}

	/**
	 * @param id
	 */
	public NoopApp(String id) {
		this.myID = id;
	}

	// Need a messenger mainly to send back responses to the client.
	@Override
	public void setClientMessenger(SSLMessenger<?, JSONObject> msgr) {
		this.messenger = msgr;
		this.myID = msgr.getMyID().toString();
	}

	@Override
	public boolean execute(Request request,
			boolean doNotReplyToClient) {
		if (request.toString().equals(Request.NO_OP))
			return true;
			switch ((AppRequest.PacketType) (request.getRequestType())) {
			case DEFAULT_APP_REQUEST:
				return processRequest((NoopAppRequest) request,
						doNotReplyToClient);
			default:
				break;
			}
		return false;
	}

	private static final boolean DELEGATE_RESPONSE_MESSAGING = true;

	private boolean processRequest(NoopAppRequest request,
			boolean doNotReplyToClient) {
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
				+ " with state " + data.getState() + " from " + request.getClientAddress());
		if (DELEGATE_RESPONSE_MESSAGING)
			this.sendResponse(request);
		else
			sendResponse(request, doNotReplyToClient);
		return true;
	}

	/**
	 * This method exemplifies one way of sending responses back to the client.
	 * A cleaner way of sending a simple, single-message response back to the
	 * client is to delegate it to the replica coordinator, as exemplified below
	 * in {@link #sendResponse(NoopAppRequest)} and supported by gigapaxos.
	 * 
	 * @param request
	 * @param doNotReplyToClient
	 */
	private void sendResponse(NoopAppRequest request, boolean doNotReplyToClient) {
		assert (this.messenger != null && this.messenger.getClientMessenger() != null);
		if (this.messenger == null || doNotReplyToClient)
			return;

		InetSocketAddress sockAddr = request.getSenderAddress();
		try {
			this.messenger.getClientMessenger().sendToAddress(sockAddr,
					request.toJSONObject());
		} catch (JSONException | IOException e) {
			e.printStackTrace();
		}
	}

	private void sendResponse(NoopAppRequest request) {
		// set to whatever response value is appropriate
		request.setResponse(ResponseCodes.ACK.toString());
	}

	// no-op
	private boolean processStopRequest(NoopAppRequest request) {
		return true;
	}

	@Override
	public Request getRequest(String stringified)
			throws RequestParseException {
		NoopAppRequest request = null;
		if (stringified.equals(Request.NO_OP)) {
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
	private Request getNoopRequest() {
		return new NoopAppRequest(null, 0, 0, Request.NO_OP,
				AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
	}

	private static AppRequest.PacketType[] types = {
			AppRequest.PacketType.DEFAULT_APP_REQUEST,
			AppRequest.PacketType.ANOTHER_APP_REQUEST };

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>(Arrays.asList(types));
	}

	@Override
	public boolean execute(Request request) {
		return this.execute(request, false);
	}

	@Override
	public String checkpoint(String name) {
		AppData data = this.appData.get(name);
		return data != null ? data.getState() : null;
	}

	@Override
	public boolean restore(String name, String state) {
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
			if (data != null)
				System.out.println("App-" + myID + " deleting " + name
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
	public ReconfigurableRequest getStopRequest(String name, int epoch) {
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
