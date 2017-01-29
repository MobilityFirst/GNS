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
import edu.umass.cs.gnsserver.utils.ResultValue;


public class GNSCommandInternal extends InternalCommandPacket {


  protected GNSCommandInternal(InternalRequestHeader header,
          JSONObject command) throws JSONException {
    super(header, command);
  }


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


  private static boolean DEFAULT_INTERNAL = true;


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


  public static final InternalCommandPacket fieldRead(String targetGUID,
          String field, InternalRequestHeader header) throws JSONException,
          InternalRequestException {
    return getCommand(CommandType.ReadUnsigned, header,
            GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field);
  }


  public static final InternalCommandPacket fieldRead(String targetGUID,
          ArrayList<String> fields, InternalRequestHeader header)
          throws JSONException, InternalRequestException {
    return getCommand(CommandType.ReadUnsigned, header,
            GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELDS.toString(), fields);
  }


  public static InternalCommandPacket fieldUpdate(String targetGUID,
          String field, Object value, InternalRequestHeader header)
          throws JSONException, InternalRequestException {
    return getCommand(CommandType.ReplaceUserJSONUnsigned, header,
            GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.USER_JSON.toString(), new JSONObject().put(field, value));
  }


  public static InternalCommandPacket fieldUpdate(InternalRequestHeader header, CommandType type,
          Object... keysAndValues) throws JSONException,
          InternalRequestException {
    return getCommand(type, header, keysAndValues);
  }


  public static InternalCommandPacket fieldRemove(String guid,
          String field, String value, InternalRequestHeader header)
          throws InternalRequestException, JSONException {
    return getCommand(CommandType.RemoveUnsigned, header,
            GNSProtocol.GUID.toString(), guid,
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.VALUE.toString(), value);
  }
  

public static InternalCommandPacket fieldRemoveList(String guid, String field, ResultValue value, 
          InternalRequestHeader header) throws InternalRequestException, JSONException {
    return getCommand(CommandType.RemoveListUnsigned, header,
            GNSProtocol.GUID.toString(), guid,
            GNSProtocol.FIELD.toString(), field, 
            GNSProtocol.VALUE.toString(), value);   
  }
}
