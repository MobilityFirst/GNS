
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldMetaData;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ResultValue;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;


public class FieldMetaData {


  public static String makeFieldMetaDataKey(MetaDataTypeName metaDataType, String key) {
    return metaDataType.getFieldPath() + "." + key + ".MD";
  }


  public static ResponseCode add(InternalRequestHeader header, CommandPacket commandPacket, MetaDataTypeName type, String guid,
          String key, String value, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
    return FieldAccess.update(header, commandPacket, guid, makeFieldMetaDataKey(type, key), value, null, -1,
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message,
            timestamp, handler);
  }


  public static ResponseCode createField(InternalRequestHeader header, 
          CommandPacket commandPacket,
          MetaDataTypeName type, String guid,
          String key, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
    return FieldAccess.createField(header, commandPacket, guid, makeFieldMetaDataKey(type, key),
            new ResultValue(),
            writer, signature, message,
            timestamp, handler);
  }


  public static ResponseCode deleteField(InternalRequestHeader header, 
          CommandPacket commandPacket,
          MetaDataTypeName type, String guid,
          String key, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
    return FieldAccess.deleteField(header, commandPacket, guid, makeFieldMetaDataKey(type, key),
            writer, signature, message,
            timestamp, handler);
  }


  public static boolean fieldExists(MetaDataTypeName type, String guid,
          String key, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
    try {
      return NSFieldMetaData.fieldExists(type, guid, key, handler.getApp().getDB());
    } catch (FailedDBOperationException | FieldNotFoundException | RecordNotFoundException e) {
      return false;
    }
  }


  public static Set<String> lookup(InternalRequestHeader header, 
          CommandPacket commandPacket,
          MetaDataTypeName type, String guid, String key,
          String reader, String signature,
          String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    String field = makeFieldMetaDataKey(type, key);
    ResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(header, commandPacket, guid, field, 
            null, //fields
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return new HashSet<>();
    }
    ResultValue result = NSFieldAccess.lookupListFieldLocallySafe(guid, field,
            handler.getApp().getDB());
    return new HashSet<>(result.toStringSet());
  }


  public static ResponseCode removeValue(InternalRequestHeader header, 
          CommandPacket commandPacket,
          MetaDataTypeName type, String guid, String key, String value, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
    return FieldAccess.update(header, commandPacket, guid, makeFieldMetaDataKey(type, key), value, null, -1,
            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message, timestamp, handler);
  }

}
