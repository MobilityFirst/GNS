/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GNSProtocol;
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

/**
 * Implements metadata on fields.
 *
 * @author westy
 */
public class FieldMetaData {

  /**
   * Creates a key for looking up metadata in a guid record.
   *
   * @param metaDataType
   * @param key
   * @return a string
   */
  public static String makeFieldMetaDataKey(MetaDataTypeName metaDataType, String key) {
    return metaDataType.getFieldPath() + "." + key + ".MD";
  }

  /**
   * Adds a value to the metadata of the field in the guid.
   *
   * @param header
   * @param commandPacket
   * @param type
   * @param guid
   * @param key
   * @param value
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a {@link ResponseCode}
   */
  public static ResponseCode add(InternalRequestHeader header, CommandPacket commandPacket, MetaDataTypeName type, String guid,
          String key, String value, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
    return FieldAccess.update(header, commandPacket, guid, makeFieldMetaDataKey(type, key), value, null, -1,
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message,
            timestamp, handler);
  }

  /**
   * Create an empty metadata field in the guid.
   *
   * @param header
   * @param type
   * @param commandPacket
   * @param guid
   * @param key
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a {@link ResponseCode}
   */
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

  /**
   * Delete a metadata field in the guid.
   *
   * @param header
   * @param type
   * @param commandPacket
   * @param guid
   * @param key
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a {@link ResponseCode}
   */
  public static ResponseCode deleteField(InternalRequestHeader header,
          CommandPacket commandPacket,
          MetaDataTypeName type, String guid,
          String key, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
    return FieldAccess.deleteField(header, commandPacket, guid, makeFieldMetaDataKey(type, key),
            writer, signature, message,
            timestamp, handler);
  }

  /**
   * Return true if the field exists.
   *
   * @param type
   * @param guid
   * @param key
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return true if the field exists
   */
  public static boolean fieldExists(MetaDataTypeName type, String guid,
          String key, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
    try {
      return NSFieldMetaData.fieldExists(type, guid, key, handler.getApp().getDB());
    } catch (FailedDBOperationException | FieldNotFoundException | RecordNotFoundException e) {
      return false;
    }
  }

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   *
   * @param header
   * @param commandPacket
   * @param type
   * @param guid
   * @param key
   * @param reader
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a set of strings
   */
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

  /**
   *
   * @param header
   * @param commandPacket
   * @param type
   * @param guid
   * @param accessor
   * @param key
   * @param accessorPublicKey
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a {@link ResponseCode}
   */
  public static ResponseCode removeValue(InternalRequestHeader header,
          CommandPacket commandPacket,
          MetaDataTypeName type, String guid, String accessor, 
          String key, String accessorPublicKey, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
    // Special case check for removing read or write access for account guid from the ACL of a keyless subguid
    GuidInfo guidInfo = AccountAccess.lookupGuidInfoLocally(header, guid, handler);
    // If we're changing the read or write whitelist for the entire record for a keyless subguid
    if ((MetaDataTypeName.READ_WHITELIST.equals(type) || MetaDataTypeName.WRITE_WHITELIST.equals(type))
            && key.equals(GNSProtocol.ENTIRE_RECORD.toString())
            && guidInfo.isKeyless()) {
      String accountGuid = AccountAccess.lookupPrimaryGuid(header, guid, handler, false);
      // And the parent account guid is the guid accessor we're removing we return an error
      if (accountGuid != null && accountGuid.equals(accessor)) {
        return ResponseCode.NO_ERROR;
      }
    }
    return FieldAccess.update(header, commandPacket, guid, makeFieldMetaDataKey(type, key), 
            accessorPublicKey, null, -1,
            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message, timestamp, handler);
  }

}
