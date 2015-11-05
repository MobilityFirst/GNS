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
package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;

/**
 * @author V. Arun
 * 
 *         This class has no special fields in addition to a generic
 *         ClientReconfigurationPacket. It just needs information about
 *         isFailed(), the correct set of reconfigurators, and the response
 *         message, all of which are in ClientReconfigurationPacket anyway.
 */
public class DeleteServiceName extends ClientReconfigurationPacket {

	/**
	 * Needed for unstringing InetSocketAddresses.
	 */
	protected static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	/**
	 * @param name
	 */
	public DeleteServiceName(String name) {
		this(null, name, 0);
	}

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 */
	public DeleteServiceName(InetSocketAddress initiator, String name,
			int epochNumber) {
		super(initiator, ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME,
				name, epochNumber);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public DeleteServiceName(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, DeleteServiceName.unstringer); // ignores unstringer
		assert (this.getSender() != null);
		// this.setSender(JSONNIOTransport.getSenderAddress(json));
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public DeleteServiceName(JSONObject json) throws JSONException {
		this(json, unstringer);
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		return json;
	}
}
