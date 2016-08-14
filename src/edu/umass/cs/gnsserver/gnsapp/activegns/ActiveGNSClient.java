package edu.umass.cs.gnsserver.gnsapp.activegns;

import java.io.IOException;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.GNSCommandInternal;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author arun
 * 
 *         This client is designed to be used either by an active request worker
 *         process or by the main GNS process to issue read or write requests to
 *         the GNS on behalf of active request JS. This implementation is
 *         agnostic to whether the GUID being queried resides on the same
 *         machine or a remote machine.
 *
 */
public class ActiveGNSClient extends GNSClient implements ActiveDBInterface {

	/**
	 * Refer {@link GNSClient#GNSClient()}.
	 * 
	 * @throws IOException
	 */
	public ActiveGNSClient() throws IOException {
		super();
	}

	@Override
	public void write(InternalRequestHeader header, String targetGUID,
			String field, JSONObject value) throws ClientException {
		try {
			this.execute(GNSCommandInternal.fieldUpdate(targetGUID, field,
					value, header));
		} catch (IOException | JSONException e) {
			throw new ClientException(e);
		}
	}

	@Override
	public JSONObject read(InternalRequestHeader header, String targetGUID,
			String field) throws ClientException {
		try {
			return this.execute(
					GNSCommandInternal.fieldRead(targetGUID, field, header))
					.getResultJSONObject();
		} catch (IOException | JSONException e) {
			throw new ClientException(e);
		}
	}

	@Override
	public JSONObject read(InternalRequestHeader header, String targetGUID,
			ArrayList<String> fields) throws ClientException {
		try {
			return this.execute(
					GNSCommandInternal.fieldRead(targetGUID, fields, header))
					.getResultJSONObject();
		} catch (IOException | JSONException e) {
			throw new ClientException(e);
		}
	}
}
