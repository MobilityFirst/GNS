
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ValuesMap;

import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;


public class ActiveCode {



  public static final String ON_READ = InternalField.makeInternalFieldString("on_read");

  public static final String ON_WRITE = InternalField.makeInternalFieldString("on_write");
  

  public static final String READ_ACTION = "read";

  public static final String WRITE_ACTION = "write";

  

  public static String getCodeField(String action) throws IllegalArgumentException {
    switch (action) {
      case READ_ACTION:
        return ON_READ;
      case WRITE_ACTION:
        return ON_WRITE;
      default:
        throw new IllegalArgumentException("action should be one of " + READ_ACTION + " or " + WRITE_ACTION);
    }
  }


  public static ResponseCode setCode(InternalRequestHeader header, 
          CommandPacket commandPacket, String guid, 
          String action, String code, String writer,
          String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler)
          throws JSONException, IllegalArgumentException {
    JSONObject json;
    json = new JSONObject();
    json.put(getCodeField(action), code); // getCodeField can throw IllegalArgumentException
    ResponseCode response = FieldAccess.updateUserJSON(header, commandPacket, guid, json,
            writer, signature, message, timestamp, handler);
    return response;
  }


  public static ResponseCode clearCode(InternalRequestHeader header, CommandPacket commandPacket, String guid, String action,
          String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) throws IllegalArgumentException {
    String field = getCodeField(action); // can throw IllegalArgumentException

    ResponseCode response = FieldAccess.update(header, commandPacket, guid, field, "", null, -1,
            UpdateOperation.SINGLE_FIELD_REMOVE_FIELD, writer, signature,
            message, timestamp, handler);
    return response;
  }


  public static String getCode(InternalRequestHeader header, CommandPacket commandPacket,
          String guid, String action, String reader,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler)
          throws IllegalArgumentException, FailedDBOperationException, JSONException {

    String field = getCodeField(action); // can throw IllegalArgumentException
    ResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(header, commandPacket, guid, field, 
            null, // fields
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return GNSProtocol.NULL_RESPONSE.toString();
    }
    ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null, guid, field,
            handler.getApp(), false); // the false disables active code handling which we obviously don't want here
    return result.getString(field);
  }
}
