package edu.umass.cs.gnsserver.gnsapp;

import static edu.umass.cs.gnscommon.GNSCommandProtocol.FIELD;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.READER;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;

/**
 * @author arun
 *
 */
public class GNSCommandInternal extends GNSCommand {
	protected GNSCommandInternal(JSONObject command) {
		super(command);
	}

	/**
	 * This magic makes the command obviate signature checks. This command is
	 * usable only at a server because only servers can know of or generate the
	 * correct {@link GNSConfig.GNSC#INTERNAL_OP_SECRET}.
	 * 
	 * @param command
	 * @return
	 * @throws JSONException
	 */
	private static JSONObject makeInternal(JSONObject command)
			throws JSONException {
		// internal commands can not and need not be signed
		assert (!command.has(GNSCommandProtocol.SIGNATURE));
		// only unsigned commands can be modified this way
		return command.put(GNSCommandProtocol.READER,
				Config.getGlobalString(GNSConfig.GNSC.INTERNAL_OP_SECRET));
	}

	private static GNSCommand getCommand(CommandType type, GuidEntry querier,
			Object... keysAndValues) throws JSONException {
		return new GNSCommandInternal(makeInternal(CommandUtils.createCommand(
				type, querier, keysAndValues)));
	}

	/**
	 * Identical to {@link GNSCommand#fieldReadArray(String, String, GuidEntry)}
	 * except that the last {@code querierGUID} argument is a String. For
	 * internal operations, we need the identity of {@code querierGUID} to
	 * perform ACL checks or to "charge" it an operation (for accounting
	 * purposes) but we don't need to generate or verify its signatures.
	 * 
	 * @param targetGUID
	 * @param field
	 * @param querierGUID
	 * @return Refer {@link GNSCommand#fieldRead(String, String, GuidEntry)}.
	 * @throws JSONException
	 */
	public static final CommandPacket fieldRead(String targetGUID,
			String field, String querierGUID) throws JSONException {
		return getCommand(CommandType.ReadUnsigned, null, GUID, targetGUID,
				FIELD, field, GNSCommandProtocol.ACTIVE_CODE_INITIATOR_GUID,
				querierGUID);
	}

	/**
	 * Identical to
	 * {@link GNSCommand#fieldUpdate(String, String, Object, GuidEntry)} except
	 * that the last {@code querierGUID} argument is a String. For internal
	 * operations, we need the identity of {@code querierGUID} to perform ACL
	 * checks or to "charge" it an operation (for accounting purposes) but we
	 * don't need to generate or verify its signatures.
	 * 
	 * @param targetGUID
	 * @param field
	 * @param value
	 * @param querierGUID
	 * @return Refer
	 *         {@link GNSCommand#fieldUpdate(String, String, Object, GuidEntry)}
	 *         .
	 */
	public static CommandPacket fieldUpdate(String targetGUID, String field,
			JSONObject value, String querierGUID) {
		return null;
	}
}
