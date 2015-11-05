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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BatchedCreateServiceName.BatchKeys;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         This class has a field to specify the initial state in addition to
 *         the default fields in ClientReconfigurationPacket.
 */
public class CreateServiceName extends ClientReconfigurationPacket {

	protected static enum Keys {
		NAME, STATE, NAME_STATE_ARRAY
	};

	/**
	 * Unstringer needed to handle client InetSocketAddresses as opposed to
	 * NodeIDType.
	 */
	public static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	/**
	 * Initial state.
	 */
	public final String initialState;

	/**
	 * Map of name,state pairs for batched creates.
	 */
	public final Map<String, String> nameStates;

	/**
	 * @param name
	 * @param state
	 */
	public CreateServiceName(String name,
			 String state) {
		this(null, name, 0, state);
	}

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param state
	 * @param nameStates
	 */
	public CreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state, Map<String, String> nameStates) {
		super(initiator, ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME,
				name, epochNumber);
		this.initialState = state;
		this.nameStates = nameStates;
	}

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param state
	 */
	public CreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state) {
		this(initiator, name, epochNumber, state, null);
	}

	/**
	 * @param initiator
	 * @param nameStates
	 */
	public CreateServiceName(InetSocketAddress initiator,
			Map<String, String> nameStates) {
		this(initiator, nameStates.keySet().iterator().next(), 0, nameStates
				.values().iterator().next(), nameStates);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public CreateServiceName(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, CreateServiceName.unstringer); // ignores unstringer
		// may not be true for String packet demultiplexers
		//assert (this.getSender() != null);
		this.initialState = json.optString(Keys.STATE.toString(), null);
		this.nameStates = getNameStateMap(json);
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public CreateServiceName(JSONObject json) throws JSONException {
		this(json, unstringer);
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		if (initialState != null)
			json.put(Keys.STATE.toString(), initialState);

		json.putOpt(BatchKeys.NAME_STATE_ARRAY.toString(),
				getNameStateJSONArray(this.nameStates));
		return json;
	}

	/**
	 * @return True if this is a batched create request or response.
	 */
	public boolean isBatched() {
		return this.nameStates != null && !this.nameStates.isEmpty();
	}

	protected static JSONArray getNameStateJSONArray(
			Map<String, String> nameStates) throws JSONException {
		if (nameStates != null && !nameStates.isEmpty()) {
			JSONArray jsonArray = new JSONArray();
			for (String name : nameStates.keySet()) {
				JSONObject nameState = new JSONObject();
				nameState.put(Keys.NAME.toString(), name);
				nameState.put(Keys.STATE.toString(), nameStates.get(name));
				jsonArray.put(nameState);
			}
			return jsonArray;
		}
		return null;
	}

	protected static Map<String, String> getNameStateMap(JSONObject json)
			throws JSONException {
		if (!json.has(BatchKeys.NAME_STATE_ARRAY.toString()))
			return null;
		JSONArray nameStateArray = json.getJSONArray(BatchKeys.NAME_STATE_ARRAY
				.toString());
		Map<String, String> nameStates = new HashMap<String, String>();
		for (int i = 0; i < nameStateArray.length(); i++) {
			JSONObject nameState = nameStateArray.getJSONObject(i);
			String name = nameState.getString(Keys.NAME.toString());
			String state = nameState.has(Keys.STATE.toString()) ? nameState
					.getString(Keys.STATE.toString()) : null;
			if (name == null)
				throw new JSONException("Parsed null name in batched request");
			nameStates.put(name, state);
		}
		return nameStates;
	}

	/**
	 * @return Initial state.
	 */
	public String getInitialState() {
		return initialState;
	}

	/**
	 * @return Name, state tuple map.
	 */
	public Map<String, String> getNameStates() {
		return this.nameStates;
	}

	/**
	 * @return Number of creates in this request or respose.
	 */
	public int size() {
		return this.isBatched() ? this.nameStates.size() : 1;
	}

	public String getSummary() {
		return super.getSummary()
				+ (this.isBatched() ? ":|batched|=" + this.size() : "");
	}

	public static void main(String[] args) {
		try {
			Util.assertAssertionsEnabled();
			InetSocketAddress isa = new InetSocketAddress(
					InetAddress.getByName("localhost"), 2345);
			int numNames = 1000;
			String[] reconfigurators = { "RC43", "RC22", "RC78", "RC21",
					"RC143" };
			String namePrefix = "someName";
			String defaultState = "default_initial_state";
			String[] names = new String[numNames];
			String[] states = new String[numNames];
			for (int i = 0; i < numNames; i++) {
				names[i] = namePrefix + i;
				states[i] = defaultState + i;
			}
			CreateServiceName bcreate1 = new CreateServiceName(isa, "random0",
					0, "hello");
			HashMap<String, String> nameStates = new HashMap<String, String>();
			for (int i = 0; i < names.length; i++)
				nameStates.put(names[i], states[i]);
			CreateServiceName bcreate2 = new CreateServiceName(isa, names[0],
					0, states[0], nameStates);
			System.out.println(bcreate1.toString());
			System.out.println(bcreate2.toString());

			// translate a batch into consistent constituent batches
			Collection<Set<String>> batches = ConsistentReconfigurableNodeConfig
					.splitIntoRCGroups(
							new HashSet<String>(Arrays.asList(names)),
							new HashSet<String>(Arrays.asList(reconfigurators)));
			int totalSize = 0;
			int numBatches = 0;
			for (Set<String> batch : batches)
				System.out.println("batch#" + numBatches++ + " of size "
						+ batch.size() + " (totalSize = "
						+ (totalSize += batch.size()) + ")" + " = " + batch);
			assert (totalSize == numNames);
			System.out.println(bcreate2.getSummary());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
