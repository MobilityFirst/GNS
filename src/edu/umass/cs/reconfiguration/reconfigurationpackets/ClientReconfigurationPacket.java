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
package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         This abstract class is the parent class for all packets that go back
 *         and forth between clients and reconfigurators. The current
 *         instantiations of this class include {@link CreateServiceName},
 *         {@link DeleteServiceName}, and {@link RequestActiveReplicas}. The
 *         same implementation of ClientReconfigurationPacket is used for both
 *         the request and the corresponding response. The method isRequest()
 *         says whether the packet is an incoming request or an outgoing
 *         response.
 */
public abstract class ClientReconfigurationPacket extends
		BasicReconfigurationPacket<InetSocketAddress> {

	private static enum Keys {
		INITIAL_STATE, RECONFIGURATORS, RESPONSE_MESSAGE, FAILED, RECURSIVE_REDIRECT, CREATOR, FORWARDER, MY_RECEIVER, IS_REQUEST
	};

	/**
	 * Unstringer needed to handle client InetSocketAddresses as opposed to
	 * NodeIDType.
	 */
	protected static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	// whether this request failed
	private boolean failed = false;
	// the set of responsible reconfigurators
	private Set<InetSocketAddress> hashRCs = null;
	// success or failure message
	private String responseMessage = null;
	// whether it should be redirected to an appropriate reconfigurator
	private boolean recursiveRedirect = true;
	// the original end-client that initiated this request
	private InetSocketAddress creator = null;
	// intermediate reconfigurator if any that forwarded this request
	private InetSocketAddress forwarder = null;
	// need this to keep track of my address on which received
	private InetSocketAddress myReceiver = null;
	// whether this is a request as opposed to a respose
	private boolean isRequest = true;

	/**
	 * @param initiator
	 * @param type
	 * @param name
	 * @param epochNumber
	 */
	public ClientReconfigurationPacket(InetSocketAddress initiator,
			ReconfigurationPacket.PacketType type, String name, int epochNumber) {
		super(initiator, type, name, epochNumber);
		this.creator = initiator;
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public ClientReconfigurationPacket(JSONObject json,
			Stringifiable<?> unstringer) throws JSONException {
		// ignores argument unstringer
		super(json, ClientReconfigurationPacket.unstringer); 
		this.setSender(JSONNIOTransport.getSenderAddress(json));
		this.myReceiver = (JSONNIOTransport.getReceiverAddress(json));
		
		this.failed = json.optBoolean(Keys.FAILED.toString());
		this.recursiveRedirect = json.optBoolean(Keys.RECURSIVE_REDIRECT
				.toString());
		this.responseMessage = json.has(Keys.RESPONSE_MESSAGE.toString()) ? json
				.getString(Keys.RESPONSE_MESSAGE.toString()) : null;

		JSONArray jsonArray = json.has(Keys.RECONFIGURATORS.toString()) ? json
				.getJSONArray(Keys.RECONFIGURATORS.toString()) : null;
		if (jsonArray != null) {
			this.hashRCs = new HashSet<InetSocketAddress>();
			for (int i = 0; jsonArray != null && i < jsonArray.length(); i++)
				this.hashRCs.add(RequestActiveReplicas.unstringer
						.valueOf(jsonArray.get(i).toString()));
		}
		this.forwarder = json.has(Keys.FORWARDER.toString()) ? Util
				.getInetSocketAddressFromString(json.getString(Keys.FORWARDER
						.toString())) : null;

		this.creator = json.has(Keys.CREATOR.toString()) ? Util
				.getInetSocketAddressFromString(json.getString(Keys.CREATOR
						.toString())) : null;

		/*
		 * Auto-insert if client sets creator to null. It is usually convenient
		 * for clients to set the initiator simply as null. But then we lose
		 * information about the original sender. There is no way in
		 * ProtocolPacket by design to explicitly set the initiator after
		 * creation time, so we explicitly maintain a creator here that has the
		 * same meaning as initiator but is guaranteed to be non-null at any
		 * node receiving this packet.
		 */
		if (this.creator == null)
			this.creator = this.getSender();

		this.isRequest = json.getBoolean(Keys.IS_REQUEST.toString());
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public ClientReconfigurationPacket(JSONObject json) throws JSONException {
		this(json, unstringer);
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		if (this.hashRCs != null)
			json.put(Keys.RECONFIGURATORS.toString(), new JSONArray(
					this.hashRCs));
		if (failed)
			json.put(Keys.FAILED.toString(), this.failed);
		if (this.recursiveRedirect)
			json.put(Keys.RECURSIVE_REDIRECT.toString(), this.recursiveRedirect);
		json.put(Keys.RESPONSE_MESSAGE.toString(), this.responseMessage);
		if (this.forwarder != null)
			json.put(Keys.FORWARDER.toString(), this.forwarder.toString());
		if (this.myReceiver != null)
			json.put(Keys.MY_RECEIVER.toString(), this.myReceiver.toString());
		if (this.creator != null)
			json.put(Keys.CREATOR.toString(), this.creator.toString());
		json.put(Keys.IS_REQUEST.toString(), this.isRequest);
		return json;
	}

	@Override
	public IntegerPacketType getRequestType() {
		return this.type;
	}

	/**
	 * Sets as failed and marks as response.
	 * 
	 * @return Returns this after setting as failed.
	 */
	public ClientReconfigurationPacket setFailed() {
		this.failed = true;
		this.isRequest = false;
		return this;
	}

	/**
	 * @return Whether this request failed.
	 */
	public boolean isFailed() {
		return this.failed;
	}

	/**
	 * Sets response message and marks as response.
	 * 
	 * @param msg
	 * @return {@code this}
	 */
	public ClientReconfigurationPacket setResponseMessage(String msg) {
		this.responseMessage = msg;
		this.isRequest = false;
		return this;
	}

	/**
	 * @return The success or failure message.
	 */
	public String getResponseMessage() {
		return this.responseMessage;
	}

	/**
	 * @param RCs
	 *            Set of consistently hashed reconfigurators for this name.
	 * @return {@code this}
	 */
	public ClientReconfigurationPacket setHashRCs(Set<InetSocketAddress> RCs) {
		this.hashRCs = RCs;
		return this;
	}

	/**
	 * @return Set of consistently hashed reconfigurators for this name.
	 */
	public Set<InetSocketAddress> getHashRCs() {
		return this.hashRCs;
	}

	/**
	 * @param b
	 */
	public void setRecursiveRedirect(boolean b) {
		this.recursiveRedirect = b;
	}

	/**
	 * @return True if recursive redirection enabled.
	 */
	public boolean isRecursiveRedirectEnabled() {
		return this.recursiveRedirect;
	}

	/**
	 * @return True if this is a request.
	 */
	public boolean isRequest() {
		return this.isRequest;
	}

	/**
	 * @return {@code this} marked as a response.
	 */
	public ClientReconfigurationPacket makeResponse() {
		this.isRequest = false;
		return this;
	}

	/**
	 * @return The socket address of the forwarding node.
	 */
	public InetSocketAddress getForwader() {
		return this.forwarder;
	}
	/**
	 * @return The socket address of the forwarding node.
	 */
	public InetSocketAddress getMyReceiver() {
		return this.myReceiver;
	}

	/**
	 * @return The socket address of the forwarding node.
	 */
	public InetSocketAddress getCreator() {
		return this.creator;
	}

	// there must be no option to set creator

	/**
	 * @param isa
	 * @return {@code this}
	 */
	public ClientReconfigurationPacket setForwader(InetSocketAddress isa) {
		this.forwarder = isa;
		return this;
	}

	/**
	 * @return True if it has not already been forwarded and recursive redirect
	 *         is enabled.
	 */
	public boolean isForwardable() {
		return this.isRecursiveRedirectEnabled() && this.getForwader() == null;
	}

	/**
	 * @return True if forwarded.
	 */
	public boolean isForwarded() {
		return this.forwarder != null;
	}

	/**
	 * The sender will be different from the forwarder. The initiator is the end
	 * client that initiated this request.
	 * 
	 * @return True if this is a response from a node that received a forwarded
	 *         client request.
	 */
	public boolean isRedirectedResponse() {
		return this.forwarder != null && !this.isRequest();
	}
	
	public String getSummary() {
		return super.getSummary() + ":" + (this.isRequest() ? "Q":"R")+":"+this.getCreator() + ":"+this.getForwader();
	}
}
