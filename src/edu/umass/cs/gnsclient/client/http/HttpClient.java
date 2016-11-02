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
package edu.umass.cs.gnsclient.client.http;

import edu.umass.cs.gnsclient.client.CommandUtils;
import static edu.umass.cs.gnsclient.client.CommandUtils.commandResponseToJSONArray;
import static edu.umass.cs.gnsclient.client.CommandUtils.createCommandWithTimestampAndNonce;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnscommon.GNSCommandProtocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gnsclient.client.http.android.DownloadTask;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.Password;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.utils.URIEncoderDecoder;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import edu.umass.cs.gnscommon.CommandType;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ENTIRE_RECORD;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.FIELD;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.FIELDS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.READER;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.USER_JSON;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.VALUE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.WRITER;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

/**
 * This class defines a HttpClient to communicate with a GNS instance
 * over HTTP. This class works on both Android and Desktop platforms.
 * This class contains a subset of all available server operations.
 * For a more complete set see UniversalGnsClientExtended.
 *
 * MANY USER ACCESIBLE METHODS IN HERE ARE EXACT DUPLICATES OF THE
 * ONES IN GNS CLIENT. THERE COULD REALLY BE A GNSClientInterface class.
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class HttpClient {

  private static final java.util.logging.Logger LOGGER = GNSConfig.getLogger();
  /**
   * Check whether we are on an Android platform or not
   */
  public static final boolean IS_ANDROID = System.getProperty("java.vm.name").equalsIgnoreCase("Dalvik");

  private final static String QUERYPREFIX = "?";
  private final static String VALSEP = "=";
  private final static String KEYSEP = "&";
  /**
   * The host address used when attempting to connect to the HTTP service.
   * Initialized in the default constructor.
   */
  private final String host;
  /**
   * The port number used when attempting to connect to the HTTP service.
   * Initialized in the default constructor.
   */
  private final int port;
  /**
   * The timeout used when attempting to connect to the HTTP service.
   */
  private int readTimeout = 10000;
  /**
   * The number of retries on timeout attempted when connecting to the HTTP
   * service.
   */
  private int readRetries = 1;

  /**
   * Creates a new <code>AbstractGnrsClient</code> object
   *
   * @param host Hostname of the GNS instance
   * @param port Port number of the GNS instance
   */
  public HttpClient(String host, int port) {

    this.host = host;
    this.port = port;
  }

  /**
   * Returns the host value.
   *
   * @return Returns the host.
   */
  public String getGNSProvider() {
    return host + ":" + port;
  }

  /**
   * Returns the timeout value (milliseconds) used when sending commands to the
   * server.
   *
   * @return value in milliseconds
   */
  public int getReadTimeout() {
    return readTimeout;
  }

  /**
   * Sets the timeout value (milliseconds) used when sending commands to the
   * server.
   *
   * @param readTimeout in milliseconds
   */
  public void setReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
  }

  /**
   * Returns the number of potential retries used when sending commands to the
   * server.
   *
   * @return the number of retries
   */
  public int getReadRetries() {
    return readRetries;
  }

  /**
   * Sets the number of potential retries used when sending commands to the
   * server.
   *
   * @param readRetries
   */
  public void setReadRetries(int readRetries) {
    this.readRetries = readRetries;
  }

  /**
   * Return the help message of the GNS. Can be used to check connectivity
   *
   * @return the help string
   * @throws IOException
   */
  public String getHelp() throws IOException {
    return sendGetCommand("help");
  }

  /**
   * Obtains the guid of the alias from the GNS server.
   *
   * @param alias
   * @return guid
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws ClientException
   */
  public String lookupGuid(String alias) throws UnsupportedEncodingException, IOException, ClientException {
    return getResponse(CommandType.LookupGuid,
            GNSCommandProtocol.NAME, URIEncoderDecoder.quoteIllegal(alias, ""));
  }

  /**
   * If this is a sub guid returns the account guid it was created under.
   *
   * @param guid
   * @return the guid
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws ClientException
   */
  public String lookupPrimaryGuid(String guid) throws UnsupportedEncodingException, IOException, ClientException {
    return getResponse(
            CommandType.LookupPrimaryGuid, GNSCommandProtocol.GUID, guid);
  }

  /**
   * Returns a JSON object containing all of the guid information.
   *
   * @param guid
   * @return the JSONObject containing the guid record
   * @throws IOException
   * @throws ClientException
   */
  public JSONObject lookupGuidRecord(String guid) throws IOException, ClientException {
    try {
      return new JSONObject(getResponse(
              CommandType.LookupGuidRecord, GNSCommandProtocol.GUID, guid));
    } catch (JSONException e) {
      throw new ClientException("Failed to parse LOOKUP_GUID_RECORD response", e);
    }
  }

  /**
   * Returns a JSON object containing all of the account information for an
   * account guid.
   *
   * @param gaccountGuid
   * @return the JSON Object containing the account record
   * @throws IOException
   * @throws ClientException
   */
  public JSONObject lookupAccountRecord(String gaccountGuid) throws IOException, ClientException {
    try {
      return new JSONObject(getResponse(
              CommandType.LookupAccountRecord, GNSCommandProtocol.GUID, gaccountGuid));
    } catch (JSONException e) {
      throw new ClientException("Failed to parse LOOKUP_ACCOUNT_RECORD response", e);
    }
  }

  /**
   * Get the public key for a given alias
   *
   * @param alias
   * @return the public key registered for the alias
   * @throws InvalidGuidException
   * @throws ClientException
   * @throws IOException
   */
  public PublicKey publicKeyLookupFromAlias(String alias) throws InvalidGuidException, ClientException, IOException {
    String guid = lookupGuid(alias);
    return publicKeyLookupFromGuid(guid);
  }

  /**
   * Get the public key for a given GUID
   *
   * @param guid
   * @return the publickey
   * @throws InvalidGuidException
   * @throws ClientException
   * @throws IOException
   */
  public PublicKey publicKeyLookupFromGuid(String guid) throws InvalidGuidException, ClientException, IOException {
    JSONObject guidInfo = lookupGuidRecord(guid);
    try {
      String key = guidInfo.getString(GNSCommandProtocol.GUID_RECORD_PUBLICKEY);
      byte[] encodedPublicKey = Base64.decode(key);
      KeyFactory keyFactory = KeyFactory.getInstance(GNSCommandProtocol.RSA_ALGORITHM);
      X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
      return keyFactory.generatePublic(publicKeySpec);
    } catch (JSONException e) {
      throw new ClientException("Failed to parse LOOKUP_USER response", e);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new EncryptionException("Public key encryption failed", e);
    }

  }

  /**
   * Register a new account guid with the corresponding alias on the GNS server.
   * This generates a new guid and a public / private key pair. Returns a
   * GuidEntry for the new account which contains all of this information.
   *
   * @param alias - a human readable alias to the guid - usually an email
   * address
   * @param password
   * @return the GuidEntry for the new account
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.security.NoSuchAlgorithmException
   */
  public GuidEntry accountGuidCreate(String alias, String password)
          throws IOException, ClientException, NoSuchAlgorithmException {

    KeyPair keyPair = KeyPairGenerator.getInstance(GNSCommandProtocol.RSA_ALGORITHM).generateKeyPair();
    String guid = accountGuidCreate(alias, keyPair.getPublic(), password);

    KeyPairUtils.saveKeyPair(host + ":" + port, alias, guid, keyPair);

    GuidEntry entry = new GuidEntry(alias, guid, keyPair.getPublic(), keyPair.getPrivate());

    return entry;
  }

  /**
   * Verify an account by sending the verification code back to the server.
   *
   * @param guid the account GUID to verify
   * @param code the verification code
   * @return ?
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public String accountGuidVerify(GuidEntry guid, String code) throws IOException, ClientException {
    return getResponse(CommandType.VerifyAccount, guid, GNSCommandProtocol.GUID, guid.getGuid(),
            GNSCommandProtocol.CODE, code);
  }

  /**
   * Deletes the account given by name
   *
   * @param guid GuidEntry
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public void accountGuidRemove(GuidEntry guid) throws IOException, ClientException {
    getResponse(CommandType.RemoveAccount, guid,
            GNSCommandProtocol.GUID, guid.getGuid(),
            GNSCommandProtocol.NAME, guid.getEntityName());
  }

  /**
   * Creates an new GUID associated with an account on the GNS server.
   *
   * @param accountGuid
   * @param alias the alias
   * @return the newly created GUID entry
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.security.NoSuchAlgorithmException
   */
  public GuidEntry guidCreate(GuidEntry accountGuid, String alias)
          throws IOException, ClientException, NoSuchAlgorithmException {

    KeyPair keyPair = KeyPairGenerator.getInstance(GNSCommandProtocol.RSA_ALGORITHM).generateKeyPair();
    String newGuid = guidCreate(accountGuid, alias, keyPair.getPublic());

    KeyPairUtils.saveKeyPair(host + ":" + port, alias, newGuid, keyPair);

    GuidEntry entry = new GuidEntry(alias, newGuid, keyPair.getPublic(), keyPair.getPrivate());

    return entry;
  }

  /**
   * Removes a guid (not for account Guids - use removeAccountGuid for them).
   *
   * @param guid the guid to remove
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public void guidRemove(GuidEntry guid) throws IOException, ClientException {
    getResponse(CommandType.RemoveGuid, guid,
            GNSCommandProtocol.GUID, guid.getGuid());
  }

  /**
   * Removes a guid given the guid and the associated account guid.
   *
   * @param accountGuid
   * @param guidToRemove
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public void guidRemove(GuidEntry accountGuid, String guidToRemove) throws IOException, ClientException {
    getResponse(CommandType.RemoveGuid, accountGuid,
            GNSCommandProtocol.ACCOUNT_GUID, accountGuid.getGuid(),
            GNSCommandProtocol.GUID, guidToRemove);
  }

  //
  // BASIC READS AND WRITES
  //
  /**
   * Updates the JSONObject associated with targetGuid using the given
   * JSONObject. Top-level fields not specified in the given JSONObject are
   * not modified. The writer is the guid of the user attempting access. Signs
   * the query using the private key of the user associated with the writer
   * guid.
   *
   * @param targetGuid
   * @param json
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  public void update(String targetGuid, JSONObject json, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.ReplaceUserJSON, writer, GUID, targetGuid,
            USER_JSON, json, WRITER, writer.getGuid());
  }

  /**
   * Replaces the JSON in guid with JSONObject. Signs the query using the
   * private key of the given guid.
   *
   * @param guid
   * @param json
   * @throws IOException
   * @throws ClientException
   */
  public void update(GuidEntry guid, JSONObject json) throws IOException,
          ClientException {
    update(guid.getGuid(), json, guid);
  }

  /**
   * Updates the field in the targetGuid. The writer is the guid of the user
   * attempting access. Signs the query using the private key of the writer
   * guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws ClientException
   * @throws JSONException
   */
  public void fieldUpdate(String targetGuid, String field, Object value,
          GuidEntry writer) throws IOException, ClientException,
          JSONException {
    getResponse(CommandType.ReplaceUserJSON, writer, GUID, targetGuid,
            USER_JSON, new JSONObject().put(field, value), WRITER, writer.getGuid());
  }

  /**
   * Creates an index for a field. The guid is only used for authentication
   * purposes.
   *
   * @param guid
   * @param field
   * @param index
   * @throws IOException
   * @throws ClientException
   * @throws JSONException
   */
  public void fieldCreateIndex(GuidEntry guid, String field, String index)
          throws IOException, ClientException, JSONException {
    getResponse(CommandType.CreateIndex, guid, GUID, guid.getGuid(), FIELD,
            field, VALUE, index, WRITER, guid.getGuid());
  }

  /**
   * Updates the field in the targetGuid. Signs the query using the private
   * key of the given guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
   * @throws JSONException
   */
  public void fieldUpdate(GuidEntry targetGuid, String field, Object value)
          throws IOException, ClientException, JSONException {
    fieldUpdate(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Reads the JSONObject for the given targetGuid. The reader is the guid of
   * the user attempting access. Signs the query using the private key of the
   * user associated with the reader guid (unsigned if reader is null).
   *
   * @param targetGuid
   * @param reader
   * if null guid must be all fields readable for all users
   * @return a JSONObject
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws org.json.JSONException
   */
  public JSONObject read(String targetGuid, GuidEntry reader)
          throws IOException, ClientException, JSONException {
    return new JSONObject(getResponse(reader != null ? CommandType.ReadArray
            : CommandType.ReadArrayUnsigned, reader, GUID,
            targetGuid, FIELD, ENTIRE_RECORD, READER,
            reader != null ? reader.getGuid() : null));
  }

  /**
   * Reads the entire record from the GNS server for the given guid. Signs the
   * query using the private key of the guid.
   *
   * @param guid
   * @return a JSONObject containing the values in the field
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws org.json.JSONException
   */
  public JSONObject read(GuidEntry guid) throws IOException, ClientException, JSONException {
    return read(guid.getGuid(), guid);
  }

  /**
   * Returns true if the field exists in the given targetGuid. Field is a
   * string the naming the field. Field can use dot notation to indicate
   * subfields. The reader is the guid of the user attempting access. This
   * method signs the query using the private key of the user associated with
   * the reader guid (unsigned if reader is null).
   *
   * @param targetGuid
   * @param field
   * @param reader
   * if null the field must be readable for all
   * @return a boolean indicating if the field exists
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public boolean fieldExists(String targetGuid, String field, GuidEntry reader)
          throws IOException, ClientException {
    try {
      if (reader != null) {
        getResponse(CommandType.Read, reader,
                GUID, targetGuid, FIELD, field,
                READER, reader.getGuid());
      } else {
        getResponse(CommandType.ReadUnsigned, reader,
                GUID, targetGuid, FIELD, field,
                READER, null);
      }

      return true;
    } catch (FieldNotFoundException e) {
      return false;
    }
  }

  /**
   * Returns true if the field exists in the given targetGuid. Field is a
   * string the naming the field. Field can use dot notation to indicate
   * subfields. This method signs the query using the private key of the
   * targetGuid.
   *
   * @param targetGuid
   * @param field
   * @return a boolean indicating if the field exists
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public boolean fieldExists(GuidEntry targetGuid, String field)
          throws IOException, ClientException {
    return fieldExists(targetGuid.getGuid(), field, targetGuid);
  }

  /**
   * Reads the value of field for the given targetGuid. Field is a string the
   * naming the field. Field can use dot notation to indicate subfields. The
   * reader is the guid of the user attempting access. This method signs the
   * query using the private key of the user associated with the reader guid
   * (unsigned if reader is null).
   *
   * @param targetGuid
   * @param field
   * @param reader
   * if null the field must be readable for all
   * @return a string containing the values in the field
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public String fieldRead(String targetGuid, String field, GuidEntry reader)
          throws IOException, ClientException {
    if (reader != null) {
      return CommandUtils.specialCaseSingleField(getResponse(CommandType.Read, reader,
              GUID, targetGuid, FIELD, field, READER, reader.getGuid()));
    } else {
      return CommandUtils.specialCaseSingleField(getResponse(CommandType.ReadUnsigned, reader,
              GUID, targetGuid, FIELD, field, READER, null));
    }
  }

  /**
   * Reads the value of field from the targetGuid. Field is a string the
   * naming the field. Field can use dot notation to indicate subfields. This
   * method signs the query using the private key of the targetGuid.
   *
   * @param targetGuid
   * @param field
   * @return field value
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public String fieldRead(GuidEntry targetGuid, String field)
          throws IOException, ClientException {
    return fieldRead(targetGuid.getGuid(), field, targetGuid);
  }

  /**
   * Reads the value of fields for the given targetGuid. Fields is a list of
   * strings the naming the field. Fields can use dot notation to indicate
   * subfields. The reader is the guid of the user attempting access. This
   * method signs the query using the private key of the user associated with
   * the reader guid (unsigned if reader is null).
   *
   * @param targetGuid
   * @param fields
   * @param reader
   * if null the field must be readable for all
   * @return a JSONObject containing the values in the fields
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public String fieldRead(String targetGuid, ArrayList<String> fields,
          GuidEntry reader) throws IOException, ClientException {
    return getResponse(reader != null ? CommandType.ReadMultiField
            : CommandType.ReadMultiFieldUnsigned, reader, GUID, targetGuid,
            FIELDS, fields, READER, reader != null ? reader.getGuid()
                    : null);
  }

  /**
   * Reads the value of fields for the given guid. Fields is a list of strings
   * the naming the field. Fields can use dot notation to indicate subfields.
   * This method signs the query using the private key of the guid.
   *
   * @param targetGuid
   * @param fields
   * @return values of fields
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public String fieldRead(GuidEntry targetGuid, ArrayList<String> fields)
          throws IOException, ClientException {
    return fieldRead(targetGuid.getGuid(), fields, targetGuid);
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
   * @throws ClientException
   */
  public void fieldCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.CreateList, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value.toString(),
            GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Removes a field. Allows a a different guid as the writer.
   *
   * @param targetGuid
   * @param field
   * @param writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws ClientException
   */
  public void fieldRemove(String targetGuid, String field, GuidEntry writer) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, ClientException {
    getResponse(CommandType.RemoveField, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.WRITER, writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldAppendOrCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.AppendOrCreateList, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value.toString(), GNSCommandProtocol.WRITER, writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldReplaceOrCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.ReplaceOrCreateList, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value.toString(), GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Appends a list of values onto a field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppendList(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.AppendListWithDuplication, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value.toString(), GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Replaces all the values of field with the list of values.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldReplaceList(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.ReplaceList, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value.toString(), GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Removes all the values in the list from the field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldClear(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.RemoveList, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value.toString(), GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Removes all values from the field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldClear(String targetGuid, String field, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.Clear, writer, GNSCommandProtocol.GUID, targetGuid, GNSCommandProtocol.FIELD,
            field, GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Reads all the values value for a key from the GNS server for the given
   * guid. The guid of the user attempting access is also needed. Signs the
   * query using the private key of the user associated with the reader guid
   * (unsigned if reader is null).
   *
   * @param guid
   * @param field
   * @param reader if null the field must be readable for all
   * @return a JSONArray containing the values in the field
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws org.json.JSONException
   */
  public JSONArray fieldReadArray(String guid, String field, GuidEntry reader)
          throws IOException, ClientException, JSONException {
    String response;
    if (reader == null) {
      response = getResponse(CommandType.ReadArrayUnsigned, GNSCommandProtocol.GUID,
              guid, GNSCommandProtocol.FIELD, field);
    } else {
      response = getResponse(CommandType.ReadArray, reader, GNSCommandProtocol.GUID,
              guid, GNSCommandProtocol.FIELD, field,
              GNSCommandProtocol.READER, reader.getGuid());
    }
    return commandResponseToJSONArray(field, response);
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
   * @throws ClientException
   */
  public void fieldSetElement(String targetGuid, String field, String newValue, int index, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.Set, writer, GNSCommandProtocol.GUID, targetGuid, GNSCommandProtocol.FIELD,
            field, GNSCommandProtocol.VALUE, newValue, GNSCommandProtocol.N, Integer.toString(index), GNSCommandProtocol.WRITER,
            writer.getGuid());
  }

  /**
   * Sets a field to be null. That is when read field is called a null will be returned.
   *
   * @param targetGuid
   * @param field
   * @param writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws ClientException
   */
  public void fieldSetNull(String targetGuid, String field, GuidEntry writer) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException,
          ClientException {
    getResponse(CommandType.SetFieldNull, writer,
            GNSCommandProtocol.GUID, targetGuid, GNSCommandProtocol.FIELD, field,
            GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /// GROUPS AND ACLS
  /**
   * Return a list of the groups that the guid is a member of. Signs the query
   * using the private key of the user associated with the guid.
   *
   * @param groupGuid the guid of the group to lookup
   * @param reader the guid of the entity doing the lookup
   * @return the list of groups as a JSONArray
   * @throws IOException if a communication error occurs
   * @throws ClientException if a protocol error occurs or the list cannot be
   * parsed
   * @throws InvalidGuidException if the group guid is invalid
   */
  public JSONArray guidGetGroups(String groupGuid, GuidEntry reader) throws IOException, ClientException,
          InvalidGuidException {
    try {
      return new JSONArray(getResponse(CommandType.GetGroups, reader, GNSCommandProtocol.GUID, groupGuid,
              GNSCommandProtocol.READER, reader.getGuid()));
    } catch (JSONException e) {
      throw new ClientException("Invalid member list", e);
    }
  }

  /**
   * Add a guid to a group guid. Any guid can be a group guid. Signs the query
   * using the private key of the user associated with the writer.
   *
   * @param groupGuid guid of the group
   * @param guidToAdd guid to add to the group
   * @param writer the guid doing the add
   * @throws IOException
   * @throws InvalidGuidException if the group guid does not exist
   * @throws ClientException
   */
  public void groupAddGuid(String groupGuid, String guidToAdd, GuidEntry writer) throws IOException,
          InvalidGuidException, ClientException {
    getResponse(CommandType.AddToGroup, writer, GNSCommandProtocol.GUID, groupGuid,
            GNSCommandProtocol.MEMBER, guidToAdd, GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Add multiple members to a group
   *
   * @param groupGuid guid of the group
   * @param members guids of members to add to the group
   * @param writer the guid doing the add
   * @throws IOException
   * @throws InvalidGuidException
   * @throws ClientException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void groupAddGuids(String groupGuid, JSONArray members, GuidEntry writer) throws IOException,
          InvalidGuidException, ClientException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    getResponse(CommandType.AddToGroup, writer,
            GNSCommandProtocol.GUID, groupGuid,
            GNSCommandProtocol.MEMBERS, members.toString(), GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Removes a guid from a group guid. Any guid can be a group guid. Signs the
   * query using the private key of the user associated with the writer.
   *
   * @param guid guid of the group
   * @param guidToRemove guid to remove from the group
   * @param writer the guid of the entity doing the remove
   * @throws IOException
   * @throws InvalidGuidException if the group guid does not exist
   * @throws ClientException
   */
  public void groupRemoveGuid(String guid, String guidToRemove, GuidEntry writer) throws IOException,
          InvalidGuidException, ClientException {
    getResponse(CommandType.RemoveFromGroup, writer, GNSCommandProtocol.GUID, guid,
            GNSCommandProtocol.MEMBER, guidToRemove, GNSCommandProtocol.WRITER, writer.getGuid());

  }

  /**
   * Remove a list of members from a group
   *
   * @param guid guid of the group
   * @param members guids to remove from the group
   * @param writer the guid of the entity doing the remove
   * @throws IOException
   * @throws InvalidGuidException
   * @throws ClientException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void groupRemoveGuids(String guid, JSONArray members, GuidEntry writer) throws IOException,
          InvalidGuidException, ClientException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    getResponse(CommandType.RemoveFromGroup, writer, GNSCommandProtocol.GUID, guid,
            GNSCommandProtocol.MEMBERS, members.toString(), GNSCommandProtocol.WRITER, writer.getGuid());

  }

  /**
   * Return the list of guids that are member of the group. Signs the query
   * using the private key of the user associated with the guid.
   *
   * @param groupGuid the guid of the group to lookup
   * @param reader the guid of the entity doing the lookup
   * @return the list of guids as a JSONArray
   * @throws IOException if a communication error occurs
   * @throws ClientException if a protocol error occurs or the list cannot be
   * parsed
   * @throws InvalidGuidException if the group guid is invalid
   */
  public JSONArray groupGetMembers(String groupGuid, GuidEntry reader) throws IOException, ClientException,
          InvalidGuidException {
    try {
      return new JSONArray(getResponse(CommandType.GetGroupMembers, reader, GNSCommandProtocol.GUID, groupGuid,
              GNSCommandProtocol.READER, reader.getGuid()));
    } catch (JSONException e) {
      throw new ClientException("Invalid member list", e);
    }
  }

  /**
   * Authorize guidToAuthorize to add/remove members from the group groupGuid.
   * If guidToAuthorize is null, everyone is authorized to add/remove members to
   * the group. Note that this method can only be called by the group owner
   * (private key required) Signs the query using the private key of the group
   * owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToAuthorize the guid to authorize to manipulate group membership
   * or null for anyone
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public void groupAddMembershipUpdatePermission(GuidEntry groupGuid, String guidToAuthorize) throws IOException, ClientException {
    aclAdd(AclAccessType.WRITE_WHITELIST, groupGuid, GNSCommandProtocol.GROUP_ACL, guidToAuthorize);
  }

  /**
   * Unauthorize guidToUnauthorize to add/remove members from the group
   * groupGuid. If guidToUnauthorize is null, everyone is forbidden to
   * add/remove members to the group. Note that this method can only be called
   * by the group owner (private key required). Signs the query using the
   * private key of the group owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToUnauthorize the guid to authorize to manipulate group
   * membership or null for anyone
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public void groupRemoveMembershipUpdatePermission(GuidEntry groupGuid, String guidToUnauthorize) throws IOException, ClientException {
    aclRemove(AclAccessType.WRITE_WHITELIST, groupGuid, GNSCommandProtocol.GROUP_ACL, guidToUnauthorize);
  }

  /**
   * Authorize guidToAuthorize to get the membership list from the group
   * groupGuid. If guidToAuthorize is null, everyone is authorized to list
   * members of the group. Note that this method can only be called by the group
   * owner (private key required). Signs the query using the private key of the
   * group owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToAuthorize the guid to authorize to manipulate group membership
   * or null for anyone
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public void groupAddMembershipReadPermission(GuidEntry groupGuid, String guidToAuthorize) throws IOException, ClientException {
    aclAdd(AclAccessType.READ_WHITELIST, groupGuid, GNSCommandProtocol.GROUP_ACL, guidToAuthorize);
  }

  /**
   * Unauthorize guidToUnauthorize to get the membership list from the group
   * groupGuid. If guidToUnauthorize is null, everyone is forbidden from
   * querying the group membership. Note that this method can only be called by
   * the group owner (private key required). Signs the query using the private
   * key of the group owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToUnauthorize the guid to authorize to manipulate group
   * membership or null for anyone
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public void groupRemoveMembershipReadPermission(GuidEntry groupGuid, String guidToUnauthorize) throws IOException, ClientException {
    aclRemove(AclAccessType.READ_WHITELIST, groupGuid, GNSCommandProtocol.GROUP_ACL, guidToUnauthorize);
  }

  /**
   * Adds to an access control list of the given field. The accesser can be a
   * guid of a user or a group guid or null which means anyone can access the
   * field. The field can be also be +ALL+ which means all fields can be read by
   * the reader. Signs the query using the private key of the user associated
   * with the guid.
   *
   * @param accessType a value from GnrsProtocol.AclAccessType
   * @param targetGuid guid of the field to be modified
   * @param field field name
   * @param accesserGuid guid to add to the ACL
   * @throws java.io.IOException
   * @throws ClientException if the query is not accepted by the server.
   */
  public void aclAdd(AclAccessType accessType, GuidEntry targetGuid, String field, String accesserGuid)
          throws IOException, ClientException {
    aclAdd(accessType.name(), targetGuid, field, accesserGuid);
  }

  /**
   * Removes a GUID from an access control list of the given user's field on the
   * GNS server to include the guid specified in the accesser param. The
   * accesser can be a guid of a user or a group guid or null which means anyone
   * can access the field. The field can be also be +ALL+ which means all fields
   * can be read by the reader. Signs the query using the private key of the
   * user associated with the guid.
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accesserGuid
   * @throws java.io.IOException
   * @throws ClientException if the query is not accepted by the server.
   */
  public void aclRemove(AclAccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws IOException, ClientException {
    aclRemove(accessType.name(), guid, field, accesserGuid);
  }

  /**
   * Get an access control list of the given user's field on the GNS server to
   * include the guid specified in the accesser param. The accesser can be a
   * guid of a user or a group guid or null which means anyone can access the
   * field. The field can be also be +ALL+ which means all fields can be read by
   * the reader. Signs the query using the private key of the user associated
   * with the guid.
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accesserGuid
   * @return list of GUIDs for that ACL
   * @throws java.io.IOException
   * @throws ClientException if the query is not accepted by the server.
   */
  public JSONArray aclGet(AclAccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws IOException, ClientException {
    return aclGet(accessType.name(), guid, field, accesserGuid);
  }

  //
  // SELECT
  //
  /**
   * Returns all GUIDs that have a field that contains the given value as a
   * JSONArray containing guids. Also note that the GNS currently does not enforce any
   * ACL checking for this operation - everything is accessible and no
   * signatures are required. This might change.
   *
   * @param field
   * @param value
   * @return a JSONArray containing all the matched records as JSONObjects
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws org.json.JSONException
   */
  public JSONArray select(String field, String value) throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(
            CommandType.Select, GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value));
  }

  /**
   * If field is a GeoSpatial field returns all guids that have fields that are within value
   * which is a bounding box specified as a nested
   * JSONArrays of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   *
   * @param field
   * @param value - [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return a JSONArray containing the guids of all the matched records
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws org.json.JSONException
   */
  public JSONArray selectWithin(String field, JSONArray value)
          throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(
            CommandType.SelectWithin, GNSCommandProtocol.FIELD, field,
            GNSCommandProtocol.WITHIN, value.toString()));
  }

  /**
   * If field is a GeoSpatial field returns all guids that have fields that are near value
   * which is a point specified as a two element
   * JSONArray: [LONG, LAT]. Max Distance is in meters.
   *
   * @param field
   * @param value - [LONG, LAT]
   * @param maxDistance - distance in meters
   * @return a JSONArray containing the guids of all the matched records
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws org.json.JSONException
   */
  public JSONArray selectNear(String field, JSONArray value, Double maxDistance)
          throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(
            CommandType.SelectNear, GNSCommandProtocol.FIELD, field,
            GNSCommandProtocol.NEAR, value.toString(),
            GNSCommandProtocol.MAX_DISTANCE, Double.toString(maxDistance)));
  }

  /**
   * Selects all records that match query.
   *
   * @param query
   * @return a JSONArray containing all the guids
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws org.json.JSONException
   */
  public JSONArray selectQuery(String query) throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(
            CommandType.SelectQuery, GNSCommandProtocol.QUERY, query));
  }

  /**
   * Set up a context aware group guid using a query.
   *
   * @param guid
   * @param query
   * @return a JSONArray containing all the guids
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws org.json.JSONException
   */
  public JSONArray selectSetupGroupQuery(String guid, String query)
          throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(
            CommandType.SelectGroupSetupQuery, GNSCommandProtocol.GUID, guid,
            GNSCommandProtocol.QUERY, query));
  }

  /**
   * Look up the value of a context aware group guid using a query.
   *
   * @param guid
   * @return a JSONArray containing all the guids
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws org.json.JSONException
   */
  public JSONArray selectLookupGroupQuery(String guid) throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(
            CommandType.SelectGroupLookupQuery, GNSCommandProtocol.GUID, guid));
  }

  /**
   * Update the location field for the given GUID
   *
   * @param longitude the GUID longitude
   * @param latitude the GUID latitude
   * @param guid the GUID to update
   * @throws java.io.IOException if a GNS error occurs
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public void setLocation(double longitude, double latitude, GuidEntry guid) throws IOException, ClientException {
    JSONArray array = new JSONArray(Arrays.asList(longitude, latitude));
    fieldReplaceOrCreateList(guid.getGuid(), GNSCommandProtocol.LOCATION_FIELD_NAME, array, guid);
  }

  /**
   * Get the location of the target GUID as a JSONArray: [LONG, LAT]
   *
   * @param readerGuid the GUID issuing the request
   * @param targetGuid the GUID that we want to know the location
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws java.io.IOException if a GNS error occurs
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws org.json.JSONException
   */
  public JSONArray getLocation(GuidEntry readerGuid, String targetGuid)
          throws IOException, ClientException, JSONException {
    return fieldReadArray(targetGuid, GNSCommandProtocol.LOCATION_FIELD_NAME, readerGuid);
  }

  /**
   * Creates an alias entity name for the given guid. The alias can be used just
   * like the original entity name.
   *
   * @param guid
   * @param name - the alias
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public void addAlias(GuidEntry guid, String name) throws IOException, ClientException {
    getResponse(CommandType.AddAlias, guid,
            GNSCommandProtocol.GUID, guid.getGuid(), GNSCommandProtocol.NAME, name);
  }

  /**
   * Removes the alias for the given guid.
   *
   * @param guid
   * @param name - the alias
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public void removeAlias(GuidEntry guid, String name) throws IOException, ClientException {
    getResponse(CommandType.RemoveAlias, guid,
            GNSCommandProtocol.GUID, guid.getGuid(), GNSCommandProtocol.NAME, name);
  }

  /**
   * Retrieve the aliases associated with the given guid.
   *
   * @param guid
   * @return - a JSONArray containing the aliases
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public JSONArray getAliases(GuidEntry guid) throws IOException, ClientException {
    try {
      return new JSONArray(getResponse(CommandType.RetrieveAliases, guid,
              GNSCommandProtocol.GUID, guid.getGuid()));
    } catch (JSONException e) {
      throw new ClientException("Invalid alias list", e);
    }
  }

  // ///////////////////////////////
  // // PRIVATE METHODS BELOW /////
  // /////////////////////////////
  /**
   * Creates a new GUID associated with an account.
   *
   * @param accountGuid
   * @param name
   * @param publicKey
   * @return the guid
   * @throws java.io.IOException
   */
  private String guidCreate(GuidEntry accountGuid, String name, PublicKey publicKey) throws IOException, ClientException {
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    return getResponse(CommandType.AddGuid, accountGuid,
            GNSCommandProtocol.GUID, accountGuid.getGuid(),
            GNSCommandProtocol.NAME, URIEncoderDecoder.quoteIllegal(name, ""),
            GNSCommandProtocol.PUBLIC_KEY, publicKeyString);
  }

  /**
   * Register a new account guid with the corresponding alias and the given
   * public key on the GNS server. Returns a new guid.
   *
   * @param alias the alias to register (usually an email address)
   * @param publicKey the public key associate with the account
   * @return guid the GUID generated by the GNS
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws ClientException
   * @throws InvalidGuidException if the user already exists
   */
  private String accountGuidCreate(String alias, PublicKey publicKey, String password) throws UnsupportedEncodingException, IOException,
          ClientException, InvalidGuidException, NoSuchAlgorithmException {
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    return getResponse(CommandType.RegisterAccount, GNSCommandProtocol.NAME,
            URIEncoderDecoder.quoteIllegal(alias, ""), GNSCommandProtocol.PUBLIC_KEY, publicKeyString,
            GNSCommandProtocol.PASSWORD,
            password != null
                    ? Password.encryptAndEncodePassword(password, alias)
                    : "");
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accesserGuid
   * @throws java.io.IOException
   */
  private void aclAdd(String accessType, GuidEntry guid, String field, String accesserGuid) throws IOException, ClientException {
    getResponse(accesserGuid == null ? CommandType.AclAdd : CommandType.AclAddSelf, guid,
            GNSCommandProtocol.ACL_TYPE, accessType, GNSCommandProtocol.GUID,
            guid.getGuid(), GNSCommandProtocol.FIELD, field, GNSCommandProtocol.ACCESSER, accesserGuid == null
                    ? GNSCommandProtocol.ALL_GUIDS
                    : accesserGuid);
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accesserGuid
   * @throws java.io.IOException
   */
  private void aclRemove(String accessType, GuidEntry guid, String field, String accesserGuid) throws IOException, ClientException {
    getResponse(accesserGuid == null ? CommandType.AclRemove : CommandType.AclRemoveSelf, guid,
            GNSCommandProtocol.ACL_TYPE, accessType,
            GNSCommandProtocol.GUID, guid.getGuid(), GNSCommandProtocol.FIELD, field,
            GNSCommandProtocol.ACCESSER, accesserGuid == null
                    ? GNSCommandProtocol.ALL_GUIDS
                    : accesserGuid);
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accesserGuid
   * @return the acl as a JSON array
   * @throws java.io.IOException
   */
  private JSONArray aclGet(String accessType, GuidEntry guid, String field, String accesserGuid) throws IOException, ClientException {
    try {
      return new JSONArray(getResponse(accesserGuid == null ? CommandType.AclRetrieve : CommandType.AclRetrieveSelf, guid,
              GNSCommandProtocol.ACL_TYPE, accessType,
              GNSCommandProtocol.GUID, guid.getGuid(), GNSCommandProtocol.FIELD, field,
              GNSCommandProtocol.ACCESSER, accesserGuid == null
                      ? GNSCommandProtocol.ALL_GUIDS
                      : accesserGuid));
    } catch (JSONException e) {
      throw new ClientException("Invalid ACL list", e);
    }
  }

  //
  // Extended Methods
  //
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
   * @throws ClientException
   */
  public void fieldCreateSingleElementList(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.Create, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value,
            GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Creates a new one element field in the given guid with single element value being the string.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
   */
  public void fieldCreateSingleElementList(GuidEntry targetGuid, String field, String value) throws IOException,
          ClientException {
    HttpClient.this.fieldCreateSingleElementList(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Appends the single value of the field onto list of values or creates a new field
   * with a single value list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppendOrCreate(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.AppendOrCreateList, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value,
            GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Replaces the values of the field with the single value or creates a new
   * field with a single value list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldReplaceOrCreate(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.ReplaceOrCreateList, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value,
            GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * * Appends a single value onto a field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppend(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.AppendListWithDuplication, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value, GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Appends a list of values onto a field but converts the list to set removing duplicates.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.AppendList, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value.toString(), GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Appends a single value onto a field but converts the list to set removing duplicates.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.Append, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value,
            GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Replaces all the first element of field with the value.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldReplaceFirstElement(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.ReplaceList, writer, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value,
            GNSCommandProtocol.WRITER, writer.getGuid());
  }

  /**
   * Substitutes the value for oldValue in the list of values of a field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param newValue
   * @param oldValue
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldSubstitute(String targetGuid, String field, String newValue,
          String oldValue, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.SubstituteList, writer,
            GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, newValue,
            GNSCommandProtocol.OLD_VALUE, oldValue);
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the list of values of a field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field
   * @param newValue list of new values
   * @param oldValue list of old values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldSubstitute(String targetGuid, String field,
          JSONArray newValue, JSONArray oldValue, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.SubstituteList, writer, GNSCommandProtocol.GUID,
            targetGuid, GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, newValue.toString(),
            GNSCommandProtocol.OLD_VALUE, oldValue.toString());
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
   * @return first value as a string
   * @throws java.io.IOException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public String fieldReadFirstElement(String guid, String field, GuidEntry reader) throws IOException, ClientException {
    if (reader == null) {
      return getResponse(
              CommandType.ReadArrayOneUnsigned,
              GNSCommandProtocol.GUID, guid, GNSCommandProtocol.FIELD, field);
    } else {
      return getResponse(CommandType.ReadArrayOne, reader,
              GNSCommandProtocol.GUID, guid, GNSCommandProtocol.FIELD, field,
              GNSCommandProtocol.READER, reader.getGuid());
    }
  }

  private String getResponse(CommandType commandType, Object... keysAndValues) throws ClientException, IOException {
    return getResponse(commandType, null, keysAndValues);
  }

  private String getResponse(CommandType commandType, GuidEntry querier,
          Object... keysAndValues) throws ClientException, IOException {
    String command;
    if (querier != null) {
      command = createAndSignQuery(commandType, querier, keysAndValues);
    } else {
      command = createQuery(commandType, keysAndValues);
    }
    String response = sendGetCommand(command);
    LOGGER.log(Level.INFO, "getResponse for " + commandType + " : " + response);
    return checkResponse(command, response);
  }

  /**
   *
   * @param command
   * @param response
   * @return the response
   * @throws ClientException
   */
  private String checkResponse(String command, String response) throws ClientException {
    return CommandUtils.checkResponseOldSchool(response);
  }

  /**
   * Creates a http query string from the given action string and a variable
   * number of key and value pairs.
   *
   * @param commandType
   * @param keysAndValues
   * @return the query string
   * @throws IOException
   */
  private String createQuery(CommandType commandType, Object... keysAndValues) throws IOException {
    String key;
    String value;
    StringBuilder result = new StringBuilder(commandType.name() + QUERYPREFIX);

    for (int i = 0; i < keysAndValues.length; i += 2) {
      key = keysAndValues[i].toString();
      value = keysAndValues[i + 1].toString();
      result.append(URIEncoderDecoder.quoteIllegal(key, ""))
              .append(VALSEP).append(URIEncoderDecoder.quoteIllegal(value, ""))
              .append(i + 2 < keysAndValues.length ? KEYSEP : "");
    }
    return result.toString();
  }

  /**
   * Creates a http query string from the given action string and a variable
   * number of key and value pairs with a signature parameter. The signature is
   * generated from the query signed by the given guid.
   *
   * @param guid
   * @param commandType
   * @param keysAndValues
   * @return the query string
   * @throws ClientException
   */
  // This code is similar to what is in CommandUtils.createAndSignCommand
  private String createAndSignQuery(CommandType commandType, GuidEntry guid, Object... keysAndValues)
          throws ClientException {
    String key;
    String value;
    StringBuilder encodedString = new StringBuilder(commandType.name() + QUERYPREFIX);
    //StringBuilder unencodedString = new StringBuilder(commandType.name() + QUERYPREFIX);

    try {
      // map over the leys and values to produce the query
      for (int i = 0; i < keysAndValues.length; i += 2) {
        key = keysAndValues[i].toString();
        value = keysAndValues[i + 1].toString();
        encodedString.append(URIEncoderDecoder.quoteIllegal(key, ""))
                .append(VALSEP).append(URIEncoderDecoder.quoteIllegal(value, ""))
                .append(i + 2 < keysAndValues.length ? KEYSEP : "");
//        unencodedString.append(key)
//                .append(VALSEP)
//                .append(value)
//                .append(i + 2 < keysAndValues.length ? KEYSEP : "");
      }
      
      JSONObject jsonVersionOfCommand = createCommandWithTimestampAndNonce(commandType, keysAndValues);
      String canonicalJSON = CanonicalJSON.getCanonicalForm(jsonVersionOfCommand);

      KeyPair keypair;
      keypair = new KeyPair(guid.getPublicKey(), guid.getPrivateKey());

      PrivateKey privateKey = keypair.getPrivate();
      PublicKey publicKey = keypair.getPublic();
      String signatureString;
      if (!Config.getGlobalBoolean(GNSClientConfig.GNSCC.ENABLE_SECRET_KEY)) {
        signatureString = signDigestOfMessage(privateKey, canonicalJSON);
      } else {
        signatureString = signDigestOfMessage(privateKey, publicKey, canonicalJSON);
      }
      // generate the signature from the unencoded query
      //String signatureString = CommandUtils.signDigestOfMessage(privateKey, unencodedString.toString());
      // return the encoded query with the signature appended
      return encodedString.toString() + KEYSEP + GNSCommandProtocol.SIGNATURE + VALSEP + signatureString 
              //+ KEYSEP + "originalBase64" + VALSEP + Base64.encodeToString(unencodedString.toString().getBytes(), false)
              + KEYSEP + "originalBase64" + VALSEP + Base64.encodeToString(canonicalJSON.getBytes(), false)
              ;
    } catch (JSONException | UnsupportedEncodingException | NoSuchAlgorithmException 
            | InvalidKeyException | SignatureException | IllegalBlockSizeException |
            BadPaddingException | NoSuchPaddingException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  // /////////////////////////////////////////
  // // PLATFORM DEPENDENT METHODS BELOW /////
  // /////////////////////////////////////////
  /**
   * Check that the connectivity with the host:port can be established
   *
   * @throws IOException throws exception if a communication error occurs
   */
  public void checkConnectivity() throws IOException {
    if (IS_ANDROID) {
      String urlString = "http://" + host + ":" + port + "/";
      final AndroidHttpGet httpGet = new AndroidHttpGet();
      httpGet.execute(urlString);
      try {
        Object httpGetResponse = httpGet.get();
        if (httpGetResponse instanceof IOException) {
          throw (IOException) httpGetResponse;
        }
      } catch (InterruptedException | ExecutionException | IOException e) {
        throw new IOException(e);
      }
    } else // Desktop version
    {
      sendGetCommand(null);
    }
  }

  /**
   * Sends a HTTP get with given queryString to the host specified by the
   * {@link host} field.
   *
   * @param queryString
   * @return result of get as a string
   * @throws IOException if an error occurs
   */
  private String sendGetCommand(String queryString) throws IOException {
    if (IS_ANDROID) {
      return androidSendGetCommand(queryString);
    } else {
      return desktopSendGetCommmand(queryString);
    }
  }

  /**
   * Sends a HTTP get with given queryString to the host specified by the
   * {@link host} field.
   *
   * @param queryString
   * @return result of get as a string
   * @throws IOException if an error occurs
   */
  private String desktopSendGetCommmand(String queryString) throws IOException {
    HttpURLConnection connection = null;
    try {

      String urlString = "http://" + host + ":" + port;
      if (queryString != null) {
        urlString += "/GNS/" + queryString;
      }
      GNSClientConfig.getLogger().log(Level.FINE, "Sending: {0}", urlString);
      URL serverURL = new URL(urlString);
      // Set up the initial connection
      connection = (HttpURLConnection) serverURL.openConnection();
      connection.setRequestMethod("GET");
      connection.setDoOutput(true);
      connection.setReadTimeout(readTimeout);

      connection.connect();

      // read the result from the server
      BufferedReader inputStream = new BufferedReader(new InputStreamReader(connection.getInputStream()));

      String response = null;
      int cnt = readRetries;
      do {
        try {
          response = inputStream.readLine(); // we only expect one line to be
          // sent
          break;
        } catch (java.net.SocketTimeoutException e) {
          GNSClientConfig.getLogger().log(Level.INFO,
                  "Get Response timed out. Trying {0} more times. Query is {1}", new Object[]{cnt, queryString});
        }
      } while (cnt-- > 0);
      try {
        // in theory this close should allow the keepalive mechanism to operate
        // correctly
        // http://docs.oracle.com/javase/6/docs/technotes/guides/net/http-keepalive.html
        inputStream.close();
      } catch (IOException e) {
        GNSClientConfig.getLogger().warning("Problem closing the HttpURLConnection's stream.");
      }
      GNSClientConfig.getLogger().log(Level.FINE, "Received: {0}", response);
      if (response != null) {
        return response;
      } else {
        throw new IOException("No response to command: " + queryString);
      }
    } finally {
      // close the connection, set all objects to null
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private String androidSendGetCommand(String queryString) throws IOException {
    String urlString = "http://" + host + ":" + port + "/GNS/" + queryString;
    final AndroidHttpGet httpGet = new AndroidHttpGet();
    httpGet.execute(urlString);
    try {
      Object httpGetResponse = httpGet.get();
      if (httpGetResponse instanceof IOException) {
        throw (IOException) httpGetResponse;
      } else {
        return (String) httpGetResponse;
      }
    } catch (InterruptedException | ExecutionException | IOException e) {
      throw new IOException(e);
    }
  }

  /**
   *
   */
  public void close() {
    // nothing to stop
  }

  private class AndroidHttpGet extends DownloadTask {

    /**
     * Creates a new <code>httpGet</code> object
     */
    public AndroidHttpGet() {
      super();
    }

    // onPostExecute displays the results of the AsyncTask.
    @Override
    protected void onPostExecute(Object result) {
    }

  }

}
