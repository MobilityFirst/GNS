/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */

package edu.umass.cs.gigapaxos;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

/**
 * @author arun
 *
 */
public class PaxosClientAsync {

	final MessageNIOTransport<String, JSONObject> niot;
	final InetSocketAddress[] servers;
	final ConcurrentHashMap<Long, RequestCallback> callbacks = new ConcurrentHashMap<Long, RequestCallback>();
	private RequestCallback defaultCallback = null;

	// FIXME: eventually garbage collect old requests

	class ClientPacketDemultiplexer extends
			AbstractPacketDemultiplexer<JSONObject> {
		final PaxosClientAsync client;

		ClientPacketDemultiplexer(PaxosClientAsync client) {
			this.client = client;
			this.register(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
		}

		@Override
		public boolean handleMessage(JSONObject message) {
			RequestPacket response = null;
			try {
				response = new RequestPacket(message);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			if (response != null)
				if (callbacks.containsKey((long) response.requestID))
					callbacks.remove((long) response.requestID).handleResponse(
							response);
				else if(PaxosClientAsync.this.defaultCallback  !=null)
					PaxosClientAsync.this.defaultCallback.handleResponse(response);

			return true;
		}

		@Override
		protected Integer getPacketType(JSONObject message) {
			try {
				return JSONPacket.getPacketType(message);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return null;
		}

		@Override
		protected JSONObject getMessage(String message) {
			try {
				return new JSONObject(message);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return null;
		}

		@Override
		protected JSONObject processHeader(String message, NIOHeader header) {
			try {
				// don't care about sender server address
				return new JSONObject(message);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return null;
		}

		@Override
		protected boolean matchesType(Object message) {
			return message instanceof JSONObject;
		}

	}
	
	/**
	 * @param callback
	 * @return The previous value of the default callback if any.
	 */
	public RequestCallback setDefaultCallback(RequestCallback callback) {
		RequestCallback old = this.defaultCallback;
		this.defaultCallback = callback;
		return old;
	}

	/**
	 * @param paxosID
	 * @param value
	 * @param server
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 * @throws JSONException
	 */
	public Long sendRequest(String paxosID, String value,
			InetSocketAddress server, RequestCallback callback)
			throws IOException, JSONException {
		RequestPacket request = null;
		RequestCallback prev = null;
		do {
			request = new RequestPacket(value, false);
			request.putPaxosID(paxosID, 0);
			prev = this.callbacks.putIfAbsent((long) request.requestID,
					callback);
		} while (prev != null);
		return this.sendRequest(request, server, callback);
	}

	/**
	 * @param paxosID
	 * @param value
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 * @throws JSONException
	 */
	public long sendRequest(String paxosID, String value,
			RequestCallback callback) throws IOException, JSONException {
		Long reqID = null;
		do {
			RequestPacket request = new RequestPacket(value, false);
			request.putPaxosID(paxosID, 0);
			reqID = this.sendRequest(request, callback);
		} while (reqID == null);
		return reqID;
	}

	/**
	 * @param request
	 * @param server
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 * @throws JSONException
	 */
	public Long sendRequest(RequestPacket request, InetSocketAddress server,
			RequestCallback callback) throws IOException, JSONException {
		int sent = -1;
		assert (request.getPaxosID() != null);
		try {
			this.callbacks.putIfAbsent((long) request.requestID, callback);
			if (this.callbacks.get((long) request.requestID) == callback) {
				sent = this.niot.sendToAddress(server, request.toJSONObject());
			}
		} finally {
			if (sent <= 0) {
				this.callbacks.remove(request.requestID, callback);
				return null;
			}
		}
		return (long) request.requestID;
	}

	/**
	 * @param request
	 * @param callback
	 * @return Request ID.
	 * @throws IOException
	 * @throws JSONException
	 */
	public Long sendRequest(RequestPacket request, RequestCallback callback)
			throws IOException, JSONException {
		return this.sendRequest(request,
				servers[(int) (Math.random() * this.servers.length)], callback);
	}

	/**
	 * @param servers
	 * @throws IOException
	 */
	public PaxosClientAsync(Set<InetSocketAddress> servers) throws IOException {
		this.niot = (new MessageNIOTransport<String, JSONObject>(null, null,
				(new ClientPacketDemultiplexer(this)), true));
		this.servers = servers.toArray(new InetSocketAddress[0]);
	}

	/**
	 * @throws IOException
	 */
	public PaxosClientAsync() throws IOException {
		this(PaxosServer.getDefaultServers());
	}

}
