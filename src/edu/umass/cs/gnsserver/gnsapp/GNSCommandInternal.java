package edu.umass.cs.gnsserver.gnsapp;

import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
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
	private static JSONObject makeInternal(CommandType type, JSONObject command)
			throws JSONException {
		// internal commands can not and need not be signed
		assert (!command.has(GNSCommandProtocol.SIGNATURE));
		// currently only read/write requests can be internal
		assert (type.isRead() || type.isUpdate());
		// only unsigned commands can be modified this way
		return command.put(type.isRead() ? GNSCommandProtocol.READER
				: GNSCommandProtocol.WRITER, Config
				.getGlobalString(GNSConfig.GNSC.INTERNAL_OP_SECRET));
	}

	private static GNSCommand getCommand(CommandType type,
			InternalRequestHeader header, Object... keysAndValues)
			throws JSONException {
		return new GNSCommandInternal(makeInternal(type,
				CommandUtils.createCommand(type, null, keysAndValues,

				GNSProtocol.ORIGINATING_GUID, header.getOriginatingRequestID(),
						GNSProtocol.ORIGINATING_QID,
						header.getOriginatingRequestID(),
						GNSProtocol.REQUEST_TTL, header.getTTL())));
	}

	/**
	 * Identical to {@link GNSCommand#fieldReadArray(String, String, GuidEntry)}
	 * except that the last {@code querierGUID} argument is replaced by an
	 * internal request {@code header}. For internal operations, we need the
	 * {@link GNSProtocol#ORIGINATING_GUID} to perform ACL checks or to "charge"
	 * it an operation (for accounting purposes) but we don't need to generate
	 * or verify its signatures. This and other information is in
	 * {@link InternalRequestHeader}.
	 * 
	 * @param targetGUID
	 * @param field
	 *            The queried field.
	 * @param header
	 *            The internal request header.
	 * @return Refer {@link GNSCommand#fieldRead(String, String, GuidEntry)}.
	 * @throws JSONException
	 */
	public static final CommandPacket fieldRead(String targetGUID,
			String field, InternalRequestHeader header) throws JSONException {
		return getCommand(CommandType.ReadUnsigned, header,
				GNSCommandProtocol.GUID, targetGUID, GNSCommandProtocol.FIELD,
				field);
	}

	/**
	 * @param targetGUID
	 * @param fields
	 *            The queried fields.
	 * @param header
	 *            The internal request header.
	 * @return Refer {@link GNSCommand#fieldRead(String, ArrayList, GuidEntry)}.
	 * @throws JSONException
	 */
	public static final CommandPacket fieldRead(String targetGUID,
			ArrayList<String> fields, InternalRequestHeader header)
			throws JSONException {
		return getCommand(CommandType.ReadUnsigned, header,
				GNSCommandProtocol.GUID, targetGUID, GNSCommandProtocol.FIELDS,
				fields);
	}

	/**
	 * Identical to
	 * {@link GNSCommand#fieldUpdate(String, String, Object, GuidEntry)} except
	 * that the last {@code querierGUID} argument is a String. For internal
	 * operations, we need the identity of {@code querierGUID} to perform ACL
	 * checks or to "charge" it an operation for accounting purposes, but we
	 * don't need to generate or verify its signatures.
	 * 
	 * @param header
	 * 
	 * @param field
	 * @param value
	 * @param targetGUID
	 * @return Refer
	 *         {@link GNSCommand#fieldUpdate(String, String, Object, GuidEntry)}
	 *         .
	 * @throws JSONException
	 */
	public static CommandPacket fieldUpdate(String targetGUID, String field,
			JSONObject value, InternalRequestHeader header)
			throws JSONException {
		return getCommand(CommandType.ReplaceUserJSONUnsigned, header,
				GNSCommandProtocol.GUID, targetGUID, GNSCommandProtocol.FIELD,
				field, GNSCommandProtocol.USER_JSON, makeJSON(field, value)
						.toString());
	}
}
