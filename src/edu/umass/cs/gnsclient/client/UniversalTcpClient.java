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

import edu.umass.cs.gnscommon.GnsProtocol;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Arrays;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.exceptions.GnsException;

/**
 * This class defines a client to communicate with a GNS instance over TCP.
 * This class adds single field list based commands to the {@link BasicUniversalTcpClient}'s JSONObject based commands.
 *
 * This class contains a concise subset of all available server operations.
 * For a more complete set see {@link UniversalTcpClientExtended}.
 *
 * @author <a href="mailto:westy@cs.umass.edu">Westy</a>
 * @version 1.0
 */
public class UniversalTcpClient extends BasicUniversalTcpClient implements GNSClientInterface {

  public UniversalTcpClient(String remoteHost, int remotePort) {
    this(remoteHost, remotePort, false);
  }

  /**
   * Creates a new <code>UniversalTcpClient</code> object
   * Optionally disables SSL if disableSSL is true.
   *
   * @param remoteHost Host name of the remote GNS server.
   * @param remotePort Port number of the remote GNS server.
   * @param disableSSL
   */
  public UniversalTcpClient(String remoteHost, int remotePort, boolean disableSSL) {
    super(remoteHost, remotePort, disableSSL);
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
   * @throws GnsException
   */
  public void fieldCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.CREATE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);
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
   * @throws GnsException
   */
  public void fieldAppendOrCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.APPEND_OR_CREATE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

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
   * @throws GnsException
   */
  public void fieldReplaceOrCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.REPLACE_OR_CREATE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);
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
   * @throws GnsException
   */
  public void fieldAppend(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.APPEND_LIST_WITH_DUPLICATION, GnsProtocol.GUID,
            targetGuid, GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

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
   * @throws GnsException
   */
  public void fieldReplaceList(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.REPLACE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

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
   * @throws GnsException
   */
  public void fieldClear(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.REMOVE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes all values from the field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldClear(String targetGuid, String field, GuidEntry writer) throws IOException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.CLEAR, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

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
      command = createCommand(GnsProtocol.READ_ARRAY, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field);
    } else {
      command = createAndSignCommand(reader.getPrivateKey(), GnsProtocol.READ_ARRAY, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field,
              GnsProtocol.READER, reader.getGuid());
    }

    String response = sendCommand(command);

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
   * @throws GnsException
   */
  public void fieldSetElement(String targetGuid, String field, String newValue, int index, GuidEntry writer)
          throws IOException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.SET, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.VALUE, newValue, GnsProtocol.N, Integer.toString(index), GnsProtocol.WRITER,
            writer.getGuid());
    String response = sendCommand(command);

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
   * @throws GnsException
   */
  public void fieldSetNull(String targetGuid, String field, GuidEntry writer) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.SET_FIELD_NULL, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

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
    JSONObject command = createCommand(GnsProtocol.SELECT, GnsProtocol.FIELD, field, GnsProtocol.VALUE, value);
    String response = sendCommand(command);

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
    JSONObject command = createCommand(GnsProtocol.SELECT, GnsProtocol.FIELD, field, GnsProtocol.WITHIN,
            value.toString());
    String response = sendCommand(command);

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
    JSONObject command = createCommand(GnsProtocol.SELECT, GnsProtocol.FIELD, field, GnsProtocol.NEAR,
            value.toString(), GnsProtocol.MAX_DISTANCE, Double.toString(maxDistance));
    String response = sendCommand(command);

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
    fieldReplaceOrCreateList(targetGuid, GnsProtocol.LOCATION_FIELD_NAME, array, writer);
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
    return fieldReadArray(targetGuid, GnsProtocol.LOCATION_FIELD_NAME, readerGuid);
  }

  /**
   * Get the location of the target GUID as a JSONArray: [LONG, LAT]
   *
   * @param guid
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws Exception if a GNS error occurs
   */
  public JSONArray getLocation(GuidEntry guid) throws Exception {
    return fieldReadArray(guid.getGuid(), GnsProtocol.LOCATION_FIELD_NAME, guid);
  }

  public void activeCodeClear(String guid, String action, GuidEntry writerGuid) throws GnsException, IOException {
    JSONObject command = createAndSignCommand(writerGuid.getPrivateKey(), GnsProtocol.AC_CLEAR,
            GnsProtocol.GUID, guid, GnsProtocol.AC_ACTION, action,
            GnsProtocol.WRITER, writerGuid.getGuid());
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  public void activeCodeSet(String guid, String action, String code64, GuidEntry writerGuid) throws GnsException, IOException {
    JSONObject command = createAndSignCommand(writerGuid.getPrivateKey(), GnsProtocol.AC_SET,
            GnsProtocol.GUID, guid, GnsProtocol.AC_ACTION, action,
            GnsProtocol.AC_CODE, code64, GnsProtocol.WRITER, writerGuid.getGuid());
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  public String activeCodeGet(String guid, String action, GuidEntry readerGuid) throws Exception {
    JSONObject command = createAndSignCommand(readerGuid.getPrivateKey(), GnsProtocol.AC_GET, 
            GnsProtocol.GUID, guid, GnsProtocol.AC_ACTION, action,
            GnsProtocol.READER, readerGuid.getGuid());
    String response = sendCommand(command);

    return checkResponse(command, response);
  }
}
