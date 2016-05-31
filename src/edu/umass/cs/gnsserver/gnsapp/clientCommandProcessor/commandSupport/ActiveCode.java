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

import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.utils.ValuesMap;

import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Contains static fields and methods that implement activecode.
 *
 */
public class ActiveCode {

  /**
   * Active code fields
   */
  /**
   * ON_READ - the string key for the field that stores the read information.
   */
  public static final String ON_READ = InternalField.makeInternalFieldString("on_read");
  /**
   * ON_WRITE - the string key for the field that stores the write information.
   */
  public static final String ON_WRITE = InternalField.makeInternalFieldString("on_write");

  public static final String READ_ACTION = "read";
  public static final String WRITE_ACTION = "write";

  /**
   * Returns the internal field corresponding to the given action.
   *
   * @param action
   * @return a string
   */
  public static String getCodeField(String action) {
    switch (action) {
      case READ_ACTION:
        return ON_READ;
      case WRITE_ACTION:
        return ON_WRITE;
      default:
        return null;
    }
  }

  /**
   * Sets active code for the guid and action.
   *
   * @param guid
   * @param action
   * @param code
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a {@link GNSResponseCode}
   */
  public static GNSResponseCode setCode(String guid, String action, String code, String writer,
          String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    JSONObject json;
    try {
      json = new JSONObject();
      json.put(getCodeField(action), code);
    } catch (JSONException e) {
      return GNSResponseCode.GENERIC_ERROR;
    }
    GNSResponseCode response = FieldAccess.updateUserJSON(guid, json,
            writer, signature, message, timestamp, handler);
    return response;
  }

  /**
   * Clears the active code for the guid and action.
   *
   * @param guid
   * @param action
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a {@link GNSResponseCode}
   */
  public static GNSResponseCode clearCode(String guid, String action,
          String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    String field = getCodeField(action);

    GNSResponseCode response = FieldAccess.update(guid, field, "", null, -1,
            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, 
            message, timestamp, handler);
    return response;
  }

  /**
   * Gets the currently set active code for the guid and action.
   *
   * @param guid
   * @param action
   * @param reader
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a string
   */
  public static String getCode(String guid, String action, String reader,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    String field = getCodeField(action);
    GNSResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(guid, field, null,
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isError()) {
      return GNSCommandProtocol.NULL_RESPONSE;
    }
    try {
      ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(guid, field,
              handler.getApp(), false);
      return result.getString(field);
    } catch (FailedDBOperationException | JSONException e) {
      return GNSCommandProtocol.NULL_RESPONSE;
    }
  }
}
