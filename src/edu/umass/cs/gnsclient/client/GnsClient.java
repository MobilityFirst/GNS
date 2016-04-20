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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client;

import static edu.umass.cs.gnscommon.GnsProtocol.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Arrays;
import static edu.umass.cs.gnsclient.client.CommandUtils.*;
import org.json.JSONArray;
import org.json.JSONObject;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandType;
import edu.umass.cs.gnsserver.main.GNSConfig;

/**
 * * Cleaner implementation of a GNS client using gigapaxos' async client.
 * 
 * This class defines a client to communicate with a GNS instance over TCP.
 * This class adds single field list based commands to the {@link BasicTcpClientV1}'s JSONObject based commands.
 *
 * This class contains a concise subset of all available server operations.
 * For a more complete set see {@link UniversalTcpClientExtended}.
 *
 * @author <a href="mailto:westy@cs.umass.edu">Westy</a>
 * @version 1.0
 */
public class GnsClient extends BasicGnsClient implements GNSClientInterface {

  /**
   * Creates a NewGnsClient. 
   *
   * @param anyReconfigurator
   * @param disableSSL
   * @throws java.io.IOException
   */
  public GnsClient(InetSocketAddress anyReconfigurator, boolean disableSSL)
          throws IOException {
    super(anyReconfigurator, disableSSL);
  }

  /**
   * Same as {@link #GnsClient(InetSocketAddress, InetSocketAddress, boolean)}
   * with a default reconfigurator. Either a default reconfigurator must be
   * provided or the gigapaxos properties file must contain at least one
   * legitimate reconfigurator.
   *
   * @param disableSSL
   * @throws IOException
   */
  public GnsClient(boolean disableSSL) throws IOException {
    this(null, disableSSL);
  }

  /**
   * Same as {@link #GnsClient(InetSocketAddress, boolean)}
   * with a default reconfigurator and ssl enabled. 
   * Either a default reconfigurator must be
   * provided or the gigapaxos properties file must contain at least one
   * legitimate reconfigurator.
   *
   * @throws IOException
   */
  public GnsClient() throws IOException {
    this(null, false);
  }
  
  /**
   * Same as {@link #GnsClient(InetSocketAddress, InetSocketAddress, boolean)}
   * with ssl enabled. 
   * Either a default reconfigurator must be
   * provided or the gigapaxos properties file must contain at least one
   * legitimate reconfigurator.
   *
   * @param anyReconfigurator
   * @throws IOException
   */
  public GnsClient(InetSocketAddress anyReconfigurator) throws IOException {
    this(anyReconfigurator, false);
  }

  /**
   * Creates a new field with value being the list. Allows a a different guid as
   * the writer. If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.CreateList,
            writer.getPrivateKey(), CREATE_LIST, GUID, targetGuid,
            FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
  }

  /**
   * Appends the values of the field onto list of values or creates a new field
   * with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldAppendOrCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.AppendOrCreateList,
            writer.getPrivateKey(), APPEND_OR_CREATE_LIST, GUID, targetGuid,
            FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Replaces the values of the field with the list of values or creates a new
   * field with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldReplaceOrCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.ReplaceOrCreateList,
            writer.getPrivateKey(), REPLACE_OR_CREATE_LIST, GUID, targetGuid,
            FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
  }

  /**
   * Appends a list of values onto a field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldAppend(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.AppendListWithDuplication,
            writer.getPrivateKey(), APPEND_LIST_WITH_DUPLICATION, GUID,
            targetGuid, FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Replaces all the values of field with the list of values.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldReplaceList(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.ReplaceList,
            writer.getPrivateKey(), REPLACE_LIST, GUID, targetGuid,
            FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Removes all the values in the list from the field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldClear(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.RemoveList,
            writer.getPrivateKey(), REMOVE_LIST, GUID, targetGuid,
            FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Removes all values from the field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldClear(String targetGuid, String field, GuidEntry writer) throws IOException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.Clear,
            writer.getPrivateKey(), CLEAR, GUID, targetGuid,
            FIELD, field, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Reads all the values for a key from the GNS server for the given guid. The
   * guid of the user attempting access is also needed. Signs the query using
   * the private key of the user associated with the reader guid (unsigned if
   * reader is null).
   *
   * @param guid
   * @param field
   * @param reader if null the field must be readable for all
   * @return a JSONArray containing the values in the field
   * @throws Exception
   */
  public JSONArray fieldReadArray(String guid, String field, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      command = createCommand(CommandType.ReadArrayUnsigned,
              READ_ARRAY, GUID, guid, FIELD, field);
    } else {
      command = createAndSignCommand(CommandType.ReadArray,
              reader.getPrivateKey(), READ_ARRAY, GUID, guid, FIELD, field,
              READER, reader.getGuid());
    }

    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Sets the nth value (zero-based) indicated by index in the list contained in
   * field to newValue. Index must be less than the current size of the list.
   *
   * @param targetGuid
   * @param field
   * @param newValue
   * @param index
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldSetElement(String targetGuid, String field, String newValue, int index, GuidEntry writer)
          throws IOException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.Set,
            writer.getPrivateKey(), SET, GUID, targetGuid, FIELD,
            field, VALUE, newValue, N, Integer.toString(index), WRITER,
            writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Sets a field to be null. That is when read field is called a null will be
   * returned.
   *
   * @param targetGuid
   * @param field
   * @param writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsClientException
   */
  public void fieldSetNull(String targetGuid, String field, GuidEntry writer) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.SetFieldNull,
            writer.getPrivateKey(), SET_FIELD_NULL, GUID, targetGuid,
            FIELD, field, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  //
  // SELECT
  //
  /**
   * Returns all GUIDs that have a field that contains the given value as a
   * JSONArray containing guids.
   *
   * @param field
   * @param value
   * @return a JSONArray containing the guids of all the matched records
   * @throws Exception
   */
  public JSONArray select(String field, String value) throws Exception {
    JSONObject command = createCommand(CommandType.Select,
            SELECT, FIELD, field, VALUE, value);
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * If field is a GeoSpatial field queries the GNS server return all the guids
   * that have fields that are within value which is a bounding box specified as a nested
   * JSONArrays of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   *
   * @param field
   * @param value - [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return a JSONArray containing the guids of all the matched records
   * @throws Exception
   */
  public JSONArray selectWithin(String field, JSONArray value) throws Exception {
    JSONObject command = createCommand(CommandType.SelectWithin,
            SELECT, FIELD, field, WITHIN,
            value.toString());
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * If field is a GeoSpatial field queries the GNS server and returns all
   * the guids that have fields that are near value which is a point specified
   * as a two element JSONArray: [LONG, LAT]. Max Distance is in meters.
   *
   * @param field
   * @param value - [LONG, LAT]
   * @param maxDistance - distance in meters
   * @return a JSONArray containing the guids of all the matched records
   * @throws Exception
   */
  public JSONArray selectNear(String field, JSONArray value, Double maxDistance) throws Exception {
    JSONObject command = createCommand(CommandType.SelectNear,
            SELECT, FIELD, field, NEAR,
            value.toString(), MAX_DISTANCE, Double.toString(maxDistance));
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Update the location field for the given GUID
   *
   * @param targetGuid
   * @param longitude the GUID longitude
   * @param latitude the GUID latitude
   * @param writer
   * @throws Exception if a GNS error occurs
   */
  public void setLocation(String targetGuid, double longitude, double latitude, GuidEntry writer) throws Exception {
    JSONArray array = new JSONArray(Arrays.asList(longitude, latitude));
    fieldReplaceOrCreateList(targetGuid, LOCATION_FIELD_NAME, array, writer);
  }

  /**
   * Update the location field for the given GUID
   *
   * @param longitude the GUID longitude
   * @param latitude the GUID latitude
   * @param guid the GUID to update
   * @throws Exception if a GNS error occurs
   */
  public void setLocation(GuidEntry guid, double longitude, double latitude) throws Exception {
    setLocation(guid.getGuid(), longitude, latitude, guid);
  }

  /**
   * Get the location of the target GUID as a JSONArray: [LONG, LAT]
   *
   * @param readerGuid the GUID issuing the request
   * @param targetGuid the GUID that we want to know the location
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws Exception if a GNS error occurs
   */
  public JSONArray getLocation(String targetGuid, GuidEntry readerGuid) throws Exception {
    return fieldReadArray(targetGuid, LOCATION_FIELD_NAME, readerGuid);
  }

  /**
   * Get the location of the target GUID as a JSONArray: [LONG, LAT]
   *
   * @param guid
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws Exception if a GNS error occurs
   */
  public JSONArray getLocation(GuidEntry guid) throws Exception {
    return fieldReadArray(guid.getGuid(), LOCATION_FIELD_NAME, guid);
  }

  // Active Code
  public void activeCodeClear(String guid, String action, GuidEntry writerGuid) throws GnsClientException, IOException {
    JSONObject command = createAndSignCommand(CommandType.ClearActiveCode,
            writerGuid.getPrivateKey(), AC_CLEAR,
            GUID, guid, AC_ACTION, action,
            WRITER, writerGuid.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  public void activeCodeSet(String guid, String action, byte[] code, GuidEntry writerGuid)
          throws GnsClientException, IOException {
    String code64 = Base64.encodeToString(code, true);
    JSONObject command = createAndSignCommand(CommandType.SetActiveCode,
            writerGuid.getPrivateKey(), AC_SET,
            GUID, guid, AC_ACTION, action,
            AC_CODE, code64, WRITER, writerGuid.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  public byte[] activeCodeGet(String guid, String action, GuidEntry readerGuid) throws Exception {
    JSONObject command = createAndSignCommand(CommandType.GetActiveCode,
            readerGuid.getPrivateKey(), AC_GET,
            GUID, guid, AC_ACTION, action,
            READER, readerGuid.getGuid());
    String response = sendCommandAndWait(command);

    String code64String = checkResponse(command, response);
    if (code64String != null) {
      return Base64.decode(code64String);
    } else {
      return null;
    }
  }

  // Extended commands
  /**
   * Creates a new field in the target guid with value being the list.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldCreateList(GuidEntry target, String field, JSONArray value) throws IOException,
          GnsClientException {
    fieldCreateList(target.getGuid(), field, value, target);
  }

  /**
   * Creates a new one element field with single element value being the string. Allows a a different guid as
   * the writer. If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldCreateOneElementList(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.Create,
            writer.getPrivateKey(), CREATE, GUID, targetGuid,
            FIELD, field, VALUE, value, WRITER, writer.getGuid());
    try {

      String response = sendCommandAndWait(command);

      checkResponse(command, response);
    } catch (NullPointerException ne) {
      GNSConfig.getLogger().severe("NPE in field create");
      ne.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Creates a new one element field in the target guid with single element value being the string.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldCreateOneElementList(GuidEntry target, String field, String value) throws IOException,
          GnsClientException {
    fieldCreateOneElementList(target.getGuid(), field, value, target);
  }

  /**
   * Appends the single value of the field onto list of values or creates a new field
   * with a single value list if it does not exist.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldAppendOrCreate(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.AppendOrCreate,
            writer.getPrivateKey(), APPEND_OR_CREATE, GUID, targetGuid,
            FIELD, field, VALUE, value, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
  }

  /**
   * Replaces the values of the field in targetGuid with the single value or creates a new
   * field with a single value list if it does not exist.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldReplaceOrCreate(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.ReplaceOrCreate,
            writer.getPrivateKey(), REPLACE_OR_CREATE, GUID, targetGuid,
            FIELD, field, VALUE, value, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
  }

  /**
   * Replaces the values of the field with the list of values or creates a new
   * field with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldReplaceOrCreateList(GuidEntry targetGuid, String field, JSONArray value)
          throws IOException, GnsClientException {
    fieldReplaceOrCreateList(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Replaces the values of the field in the target guid with the single value or creates a new
   * field with a single value list if it does not exist.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldReplaceOrCreateList(GuidEntry target, String field, String value)
          throws IOException, GnsClientException {
    fieldReplaceOrCreate(target.getGuid(), field, value, target);
  }

  /**
   * Replaces all the values of field with the single value.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value the new value
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldReplace(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.Replace,
            writer.getPrivateKey(), REPLACE, GUID, targetGuid,
            FIELD, field, VALUE, value, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Replaces all the values of field in target with with the single value.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldReplace(GuidEntry target, String field, String value) throws IOException,
          GnsClientException {
    fieldReplace(target.getGuid(), field, value, target);
  }

  /**
   * Replaces all the values of field in target with the list of values.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldReplace(GuidEntry target, String field, JSONArray value) throws IOException,
          GnsClientException {
    fieldReplaceList(target.getGuid(), field, value, target);
  }

  /**
   * Appends a single value onto a field.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldAppend(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.AppendWithDuplication,
            writer.getPrivateKey(), APPEND_WITH_DUPLICATION, GUID, targetGuid,
            FIELD, field, VALUE, value, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Appends a single value onto a field in the target guid.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldAppend(GuidEntry target, String field, String value) throws IOException,
          GnsClientException {
    fieldAppend(target.getGuid(), field, value, target);
  }

  /**
   * Appends a list of values onto a field but converts the list to set removing duplicates.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.AppendList,
            writer.getPrivateKey(), APPEND_LIST, GUID, targetGuid,
            FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Appends a list of values onto a field in target but converts the list to set removing duplicates.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldAppendWithSetSemantics(GuidEntry target, String field, JSONArray value) throws IOException,
          GnsClientException {
    fieldAppendWithSetSemantics(target.getGuid(), field, value, target);
  }

  /**
   * Appends a single value onto a field but converts the list to set removing duplicates.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.Append,
            writer.getPrivateKey(), APPEND, GUID, targetGuid,
            FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Appends a single value onto a field in target but converts the list to set removing duplicates.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldAppendWithSetSemantics(GuidEntry target, String field, String value) throws IOException,
          GnsClientException {
    fieldAppendWithSetSemantics(target.getGuid(), field, value, target);
  }

  /**
   * Replaces all the first element of field with the value.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field. If writer is null the command
   * is sent unsigned.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldReplaceFirstElement(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsClientException {
    JSONObject command;
    if (writer == null) {
      command = createCommand(CommandType.ReplaceUnsigned,
              REPLACE, GUID, targetGuid,
              FIELD, field, VALUE, value);
    } else {
      command = createAndSignCommand(CommandType.Replace,
              writer.getPrivateKey(), REPLACE, GUID, targetGuid,
              FIELD, field, VALUE, value, WRITER, writer.getGuid());
    }
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Replaces the first element of field in target with the value.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldReplaceFirstElement(GuidEntry target, String field, String value) throws IOException,
          GnsClientException {
    fieldReplaceFirstElement(target.getGuid(), field, value, target);
  }

  /**
   * Substitutes the value for oldValue in the list of values of a field.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param newValue
   * @param oldValue
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldSubstitute(String targetGuid, String field, String newValue,
          String oldValue, GuidEntry writer) throws IOException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.Substitute,
            writer.getPrivateKey(), SUBSTITUTE, GUID,
            targetGuid, FIELD, field, VALUE, newValue,
            OLD_VALUE, oldValue, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Substitutes the value for oldValue in the list of values of a field in the target.
   *
   * @param target
   * @param field
   * @param newValue
   * @param oldValue
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldSubstitute(GuidEntry target, String field, String newValue,
          String oldValue) throws IOException, GnsClientException {
    fieldSubstitute(target.getGuid(), field, newValue, oldValue, target);
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the list of values of a field.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   *
   * @param targetGuid GUID where the field is stored
   * @param field
   * @param newValue list of new values
   * @param oldValue list of old values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldSubstitute(String targetGuid, String field,
          JSONArray newValue, JSONArray oldValue, GuidEntry writer) throws IOException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.SubstituteList,
            writer.getPrivateKey(), SUBSTITUTE_LIST, GUID,
            targetGuid, FIELD, field, VALUE, newValue.toString(),
            OLD_VALUE, oldValue.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the list of values of a field in the target.
   *
   * @param target
   * @param field
   * @param newValue
   * @param oldValue
   * @throws IOException
   * @throws GnsClientException
   */
  public void fieldSubstitute(GuidEntry target, String field,
          JSONArray newValue, JSONArray oldValue) throws IOException, GnsClientException {
    fieldSubstitute(target.getGuid(), field, newValue, oldValue, target);
  }

  /**
   * Reads the first value for a key from the GNS server for the given
   * guid. The guid of the user attempting access is also needed. Signs the
   * query using the private key of the user associated with the reader guid
   * (unsigned if reader is null).
   *
   * @param guid
   * @param field
   * @param reader
   * @return
   * @throws Exception
   */
  public String fieldReadArrayFirstElement(String guid, String field, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      command = createCommand(CommandType.ReadArrayOneUnsigned,
              READ_ARRAY_ONE, GUID, guid, FIELD, field);
    } else {
      command = createAndSignCommand(CommandType.ReadArrayOne,
              reader.getPrivateKey(), READ_ARRAY_ONE, GUID, guid, FIELD, field,
              READER, reader.getGuid());
    }

    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }

  /**
   * Reads the first value for a key in the guid.
   *
   * @param guid
   * @param field
   * @return
   * @throws Exception
   */
  public String fieldReadArrayFirstElement(GuidEntry guid, String field) throws Exception {
    return fieldReadArrayFirstElement(guid.getGuid(), field, guid);
  }

  /**
   * Removes a field in the JSONObject record of the given guid.
   * Signs the query using the private key of the guid.
   * A convenience method™.
   *
   * @param guid
   * @param field
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsClientException
   */
  public void fieldRemove(GuidEntry guid, String field) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsClientException {
    fieldRemove(guid.getGuid(), field, guid);
  }

  public String adminEnable(String passkey) throws Exception {
    JSONObject command = createCommand(CommandType.Admin,
            ADMIN, PASSKEY, passkey);

    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }

  public void parameterSet(String name, Object value) throws Exception {
    JSONObject command = createCommand(CommandType.SetParameter,
            SET_PARAMETER, NAME, name, VALUE, value);

    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  public String parameterGet(String name) throws Exception {
    JSONObject command = createCommand(CommandType.GetParameter,
            GET_PARAMETER, NAME, name);

    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }
}
