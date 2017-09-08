package edu.umass.cs.gnsserver.gnsapp;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

/**
 * @author arun
 * 
 *         Abstract class to be implemented by custom select implementations.
 *         Implementations must support the default constructor.
 * 
 */
public abstract class AbstractSelector {

	protected static final Logger LOGGER = Logger.getLogger(Select.class
			.getName());

	/**
	 * Handles select request from a server.
	 * 
	 * @param packet
	 * @param replica
	 * @throws JSONException
	 * @throws UnknownHostException
	 * @throws FailedDBOperationException
	 */
	abstract public void handleSelectRequest(SelectRequestPacket packet,
			GNSApplicationInterface<String> replica) throws JSONException,
			UnknownHostException, FailedDBOperationException;

	/**
	 * Handles select response from a server.
	 * 
	 * @param packet
	 * @param replica
	 * @throws JSONException
	 * @throws ClientException
	 * @throws IOException
	 * @throws InternalRequestException
	 */
	abstract public void handleSelectResponse(SelectResponsePacket packet,
			GNSApplicationInterface<String> replica) throws JSONException,
			ClientException, IOException, InternalRequestException;

	/**
	 * Handles select request from a client.
	 * 
	 * @param header
	 * @param packet
	 * @param app
	 * @return SelectRequestPacket
	 * @throws JSONException
	 * @throws UnknownHostException
	 * @throws FailedDBOperationException
	 * @throws InternalRequestException
	 */
	abstract public SelectResponsePacket handleSelectRequestFromClient(
			InternalRequestHeader header, SelectRequestPacket packet,
			GNSApplicationInterface<String> app) throws JSONException,
			UnknownHostException, FailedDBOperationException,
			InternalRequestException;

	/**
	 * Returns true if a query contains operations that are not allowed.
	 * Currently $where is not allowed as well as attempts to use internal keys.
	 *
	 * @param query
	 * @return True if query is bad.
	 */
	public static boolean queryContainsEvil(String query) {
		try {
			JSONObject jsonQuery = new JSONObject("{" + query + "}");
			return jsonObjectKeyContains(NameRecord.VALUES_MAP.getName(),
					jsonQuery) || jsonObjectKeyContains("$where", jsonQuery);
		} catch (JSONException e) {
			return false;
		}
	}

	// Traverses the json looking for a key that contains the string
	private static boolean jsonObjectKeyContains(String key,
			JSONObject jsonObject) {
		LOGGER.log(Level.FINEST, "{0} {1}",
				new Object[] { key, jsonObject.toString() });
		String[] keys = JSONObject.getNames(jsonObject);
		if (keys != null) {
			for (String subKey : keys) {
				if (subKey.contains(key)) {
					return true;
				}
				JSONObject subJson = jsonObject.optJSONObject(subKey);
				if (subJson != null) {
					if (jsonObjectKeyContains(key, subJson)) {
						return true;
					}
				}
				JSONArray subArray = jsonObject.optJSONArray(subKey);
				if (subArray != null) {
					if (jsonArrayKeyContains(key, subArray)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	// Traverses the json looking for a key that contains the string
	private static boolean jsonArrayKeyContains(String key, JSONArray jsonArray) {
		LOGGER.log(Level.FINEST, "{0} {1}",
				new Object[] { key, jsonArray.toString() });
		for (int i = 0; i < jsonArray.length(); i++) {
			JSONObject subObject = jsonArray.optJSONObject(i);
			if (subObject != null) {
				if (jsonObjectKeyContains(key, subObject)) {
					return true;
				}
			}
			JSONArray subArray = jsonArray.optJSONArray(i);
			if (subArray != null) {
				if (jsonArrayKeyContains(key, subArray)) {
					return true;
				}
			}
		}
		return false;
	}
}
