package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 */
public class BatchedCreateServiceName extends CreateServiceName {

	protected static enum BatchKeys {
		NAME_STATE_ARRAY
	};

	final String[] initialStates;
	final String[] names;

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param state
	 */
	public BatchedCreateServiceName(InetSocketAddress initiator, String name,
			int epochNumber, String state) {
		super(initiator, name, epochNumber, state);
		this.initialStates = new String[1];
		this.initialStates[0] = state;
		assert (epochNumber == 0);
		this.names = new String[1];
		this.names[0] = name;
	}

	/**
	 * @param initiator
	 * @param names
	 * @param epochNumber
	 * @param states
	 */
	public BatchedCreateServiceName(InetSocketAddress initiator,
			String[] names, int epochNumber, String[] states) {
		super(initiator, names[0], epochNumber, states[0]);
		assert (epochNumber == 0);
		this.initialStates = states;
		this.names = names;
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public BatchedCreateServiceName(JSONObject json) throws JSONException {
		this(json, unstringer);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public BatchedCreateServiceName(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException {
		super(json, unstringer);
		JSONArray nameStateArray = json.has(BatchKeys.NAME_STATE_ARRAY
				.toString()) ? json.getJSONArray(BatchKeys.NAME_STATE_ARRAY
				.toString()) : new JSONArray();
		this.names = new String[nameStateArray.length()];
		this.initialStates = new String[nameStateArray.length()];
		for (int i = 0; i < nameStateArray.length(); i++) {
			JSONObject nameState = nameStateArray.getJSONObject(i);
			this.names[i] = nameState
					.getString(BasicReconfigurationPacket.Keys.SERVICE_NAME
							.toString());
			this.initialStates[i] = nameState.getString(Keys.STATE
					.toString());
		}
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		JSONArray jsonArray = new JSONArray();
		for (int i = 0; i < names.length; i++) {
			JSONObject nameState = new JSONObject();
			nameState.put(
					BasicReconfigurationPacket.Keys.SERVICE_NAME.toString(),
					this.names[i]);
			nameState.put(Keys.STATE.toString(), this.initialStates[i]);
			jsonArray.put(nameState);
		}
		json.put(BatchKeys.NAME_STATE_ARRAY.toString(), jsonArray);
		return json;
	}
	
	public String getSummary() {
		return super.getSummary() + ":|batched|=" + this.names.length;
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
			BatchedCreateServiceName bcreate1 = new BatchedCreateServiceName(
					isa, "random0", 0, "hello");
			BatchedCreateServiceName bcreate2 = new BatchedCreateServiceName(
					isa, names, 0, states);
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
			assert(totalSize==numNames);
			System.out.println(bcreate2.getSummary());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
