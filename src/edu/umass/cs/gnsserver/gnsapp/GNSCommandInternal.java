package edu.umass.cs.gnsserver.gnsapp;

import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.gnsapp.packet.InternalCommandPacket;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;

/**
 * @author arun
 *
 */
public class GNSCommandInternal extends InternalCommandPacket {

  /**
   *
   * @param header
   * @param command
   * @throws JSONException
   */
  protected GNSCommandInternal(InternalRequestHeader header,
          JSONObject command) throws JSONException {
    super(header, command);
  }

  /**
   * This magic makes the command obviate signature checks. This command is
   * usable only at a server because only servers can know of or generate the
   * correct {@link GNSConfig.GNSC#INTERNAL_OP_SECRET}.
   *
   * @param command
   * @return a JSON Object
   * @throws JSONException
   */
  private static JSONObject makeInternal(CommandType type,
          InternalRequestHeader header, JSONObject command)
          throws JSONException {
    // internal commands can not and need not be signed
    assert (!command.has(GNSProtocol.SIGNATURE.toString()));
    // currently only read/write requests can be internal
    assert (type.isRead() || type.isUpdate());
    // only unsigned commands can be modified this way
    return command.put(
            type.isRead() ? GNSProtocol.READER.toString()
                    : GNSProtocol.WRITER.toString(),
            DEFAULT_INTERNAL ? GNSProtocol.INTERNAL_QUERIER.toString()
                    : header.getQueryingGUID())
            // secure because this only works at servers
            .put(GNSProtocol.INTERNAL_PROOF.toString(),
                    GNSConfig.getInternalOpSecret());
  }

  /**
   * If this is true, the querier is internal by default. Active requests must
   * explicitly set the querier to
   * {@link InternalRequestHeader#getQueryingGUID()} so that ACL (but no
   * signature) checks can be performed on the querier.
   */
  private static boolean DEFAULT_INTERNAL = true;

  /**
   * The querier is always null in internal commands as it is implicitly a
   * trusted server or client.
   *
   * @param type
   * @param header
   * @param keysAndValues
   * @return GNSCommandInternal
   * @throws JSONException
   * @throws InternalRequestException
   */
  private static GNSCommandInternal getCommand(CommandType type,
          InternalRequestHeader header, Object... keysAndValues)
          throws JSONException, InternalRequestException {
    return enforceChecks(
            new GNSCommandInternal(header, makeInternal(
                    type,
                    header,
                    CommandUtils
                    .createCommand(type, keysAndValues)
                    .put(GNSProtocol.ORIGINATING_GUID.toString(),
                            header.getOriginatingGUID())
                    .put(GNSProtocol.ORIGINATING_QID.toString(),
                            header.getOriginatingRequestID())
                    .put(GNSProtocol.REQUEST_TTL.toString(),
                            header.getTTL())
                    .put(GNSProtocol.QUERIER_GUID.toString(),
                            header.getQueryingGUID()))), header);
  }

  private static GNSCommandInternal enforceChecks(
          GNSCommandInternal gnsCommandInternal, InternalRequestHeader header)
          throws InternalRequestException {
    if (header.getTTL() == 0) {
      throw new InternalRequestException(
              ResponseCode.INTERNAL_REQUEST_EXCEPTION, "TTL expired");
    }
    if (header.hasBeenCoordinatedOnce()
            && gnsCommandInternal.needsCoordination()) {
      throw new InternalRequestException(
              ResponseCode.INTERNAL_REQUEST_EXCEPTION,
              "Attempting a second coordinated request in a chain with "
              + gnsCommandInternal.getSummary());
    }
    return gnsCommandInternal;
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
   * The queried field.
   * @param header
   * The internal request header.
   * @return Refer {@link GNSCommand#fieldRead(String, String, GuidEntry)}.
   * @throws JSONException
   * @throws InternalRequestException
   */
  public static final InternalCommandPacket fieldRead(String targetGUID,
          String field, InternalRequestHeader header) throws JSONException,
          InternalRequestException {
    return getCommand(CommandType.ReadUnsigned, header,
            GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field);
  }

  /**
   * @param targetGUID
   * @param fields
   * The queried fields.
   * @param header
   * The internal request header.
   * @return InternalCommandPacket
   * @throws JSONException
   * @throws InternalRequestException
   */
  public static final InternalCommandPacket fieldRead(String targetGUID,
          ArrayList<String> fields, InternalRequestHeader header)
          throws JSONException, InternalRequestException {
    return getCommand(CommandType.ReadUnsigned, header,
            GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELDS.toString(), fields);
  }

  /**
   * Identical to
   * {@link GNSCommand#fieldUpdate(String, String, Object, GuidEntry)} except
   * that the last {@code querierGUID} argument is an InternalRequestHeader.
   * For internal operations, we need the identity of {@code querierGUID} to
   * perform ACL checks or to "charge" it an operation for accounting
   * purposes, but we don't need to generate or verify its signatures.
   *
   * @param header
   *
   * @param field
   * @param value
   * @param targetGUID
   * @return InternalCommandPacket
   *
   * @throws JSONException
   * @throws InternalRequestException
   */
  public static InternalCommandPacket fieldUpdate(String targetGUID,
          String field, Object value, InternalRequestHeader header)
          throws JSONException, InternalRequestException {
    return getCommand(CommandType.ReplaceUserJSONUnsigned, header,
            GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.USER_JSON.toString(), new JSONObject().put(field, value));
  }

  /**
   * Generalized update with configurable {@code CommandType} argument.
   *
   * @param type
   * @param header
   * @param keysAndValues
   * @return InternalCommandPacket
   * @throws JSONException
   * @throws InternalRequestException
   */
  public static InternalCommandPacket fieldUpdate(InternalRequestHeader header, CommandType type,
          Object... keysAndValues) throws JSONException,
          InternalRequestException {
    return getCommand(type, header, keysAndValues);
  }

  // Wrong implementation. Arbitrary command types have 
  // different arguments sets. This assumes GUID and USER_JSON.
//  /**
//   * Generalized update with configurable {@code CommandType} argument.
//   *
//   * @param type
//   * @param targetGUID
//   * @param field
//   * @param value
//   * @param header
//   * @return InternalCommandPacket
//   * @throws JSONException
//   * @throws InternalRequestException
//   */
//  public static InternalCommandPacket fieldUpdate(CommandType type,
//          String targetGUID, String field, Object value,
//          InternalRequestHeader header) throws JSONException,
//          InternalRequestException {
//    return getCommand(type, header, GNSProtocol.GUID.toString(),
//            targetGUID, GNSProtocol.USER_JSON.toString(),
//            new JSONObject().put(field, value));
//  }
  /**
   * @param memberGuid
   * @param groups
   * @param guid
   * @param header
   * @return InternalCommandPacket
   * @throws JSONException
   * @throws InternalRequestException
   */
  public static InternalCommandPacket fieldRemove(String memberGuid,
          String groups, String guid, InternalRequestHeader header)
          throws InternalRequestException, JSONException {
    return getCommand(CommandType.RemoveUnsigned, header,
            GNSProtocol.GUID.toString(), guid,
            GNSProtocol.FIELD.toString(), groups,
            GNSProtocol.VALUE.toString(), guid.toString());
  }
}
