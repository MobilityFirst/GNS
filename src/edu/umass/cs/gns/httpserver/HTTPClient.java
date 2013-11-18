package edu.umass.cs.gns.httpserver;

import edu.umass.cs.gns.client.FieldMetaData.MetaDataTypeName;
import static edu.umass.cs.gns.httpserver.Defs.*;
import edu.umass.cs.gns.aws.EC2Installer;
import edu.umass.cs.gns.util.Base64;
import edu.umass.cs.gns.util.ByteUtils;
import edu.umass.cs.gns.util.Logging;
import edu.umass.cs.gns.util.URIEncoderDecoder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Logger;
import java.util.prefs.Preferences;
import org.json.JSONArray;

/**
 * !!THIS IS NOT MAINTAINED. SEE THE GNSCLient project!!
 * A client for the GNRS HTTP server.
 *
 * A typical use case might look like this:<br>
 * <pre>
 * {@code
 * HTTPClient client = new HTTPClient();
 * client.setHost("127.0.0.1");
 * String westyGuid = lookupUserGuid("westy");
 * if (westyGuid.equals(Protocol.UNKNOWNUSER)) {
 *   westyGuid = registerNewUser("westy");
 * }
 * createField(westyGuid, "location", "work");
 * }
 * </pre>
 *
 * @author westy
 */
public class HTTPClient {

  private final Logger LOGGER = Logger.getLogger(HTTPClient.class.getName());
  private boolean initRun = false;
  private String defaultLogLevel = "INFO";
  /**
   * Controls the throwing of runtime exceptions when a bad response is received. Default is false.
   */
  public boolean throwRuntimeExceptions = false;
  /**
   * The address of the GNRS server we will contact
   */
  private String host = null;
  private int port = 8080;
  /**
   * Save the public/private key using Preferences
   */
  private Preferences userPreferencess;

  public HTTPClient() {
    userPreferencess = Preferences.userRoot().node(HTTPClient.class.getName());
  }

  /**
   * Register a new username on the GNRS server. A guid is returned by the server. Generates a public / private key pair which is
   * saved in preferences and sent to the server with the username.
   *
   * @param username
   * @return guid
   * @throws IOException
   * @throws NoSuchAlgorithmException
   *
   */
  public String registerNewUser(String username) throws IOException, NoSuchAlgorithmException, BadResponseException {

    KeyPair keyPair = KeyPairGenerator.getInstance(Protocol.RASALGORITHM).generateKeyPair();
    saveKeyPairToPreferences(username, keyPair);

    PublicKey publicKey = keyPair.getPublic();
    byte[] publicKeyBytes = publicKey.getEncoded();

    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    //String publicKeyString = MoreUtils.toHex(publicKeyBytes);

    String command = createQuery(Protocol.REGISTERACCOUNT, Protocol.NAME, URIEncoderDecoder.quoteIllegal(username, ""), Protocol.PUBLICKEY, publicKeyString);
    String response = sendGetCommand(command);

    saveKeyPairToPreferences(response, keyPair);

    return checkResponse(response, command);
  }

  public void addAlias(String guid, String name) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.ADDALIAS, Protocol.GUID, guid, Protocol.NAME, name);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  /**
   * Obtains the guid of the username from the GNRS server.
   *
   * @param username
   * @return guid
   * @throws IOException
   *
   */
  public String lookupUserGuid(String username) throws IOException, BadResponseException {
    String command = createQuery(Protocol.LOOKUPGUID, Protocol.NAME, URIEncoderDecoder.quoteIllegal(username, ""));
    String response = sendGetCommand(command);

    return checkResponse(response, command);
  }

  /**
   * Creates a new field with value.
   *
   * @param guid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void createField(String guid, String field, String value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.CREATE, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, value);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  /**
   * Creates a new field with value being the list.
   *
   * @param guid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void createFieldUsingList(String guid, String field, ArrayList value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.CREATELIST, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, new JSONArray(value).toString());
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  public void appendValue(String guid, String field, String value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.APPEND, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, value);
    String response = sendGetCommand(command);
    checkResponse(response, command);
  }

  public void appendValueUsingList(String guid, String field, ArrayList value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.APPENDLIST, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, new JSONArray(value).toString());
    String response = sendGetCommand(command);
    checkResponse(response, command);
  }

  public void appendOrCreate(String guid, String field, String value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.APPENDORCREATE, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, value);
    String response = sendGetCommand(command);
    checkResponse(response, command);
  }

  public void replaceOrCreate(String guid, String field, String value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.REPLACEORCREATE, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, value);
    String response = sendGetCommand(command);
    checkResponse(response, command);
  }

  public void appendOrCreateUsingList(String guid, String field, ArrayList value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.APPENDORCREATELIST, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, new JSONArray(value).toString());
    String response = sendGetCommand(command);
    checkResponse(response, command);
  }

  public void replaceOrCreateUsingList(String guid, String field, ArrayList value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.REPLACEORCREATELIST, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, new JSONArray(value).toString());
    String response = sendGetCommand(command);
    checkResponse(response, command);
  }

  public void appendValueWithDuplication(String guid, String field, String value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.APPENDWITHDUPLICATION, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, value);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  public void appendValuesUsingListWithDuplication(String guid, String field, ArrayList value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.APPENDLISTWITHDUPLICATION, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, new JSONArray(value).toString());
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  /**
   *
   * Replaces all the values of field with the value.
   *
   * @param guid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void replaceValue(String guid, String field, String value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.REPLACE, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, value);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  /**
   * Replaces all the values of field with the list of values.
   *
   * @param guid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void replaceValuesUsingList(String guid, String field, ArrayList value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.REPLACELIST, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, new JSONArray(value).toString());
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  /**
   * Removes the value from the field.
   *
   * @param guid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void removeValue(String guid, String field, String value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.REMOVE, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, value);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  /**
   * Removes all the values in the list from the field.
   *
   * @param guid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void removeValuesUsingList(String guid, String field, ArrayList value) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.REMOVELIST, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.VALUE, new JSONArray(value).toString());
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  /**
   * Removes all the values from a field.
   *
   * @param guid
   * @param field
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void clearField(String guid, String field) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.CLEAR, Protocol.GUID, guid, Protocol.FIELD, field);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  /**
   * Shorthand for readField(guid, field, guid)
   *
   * @param guid
   * @param field
   * @return
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public String readField(String guid, String field) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    return readField(guid, field, guid);
  }

  /**
   * Shorthand for readFieldAsSingleton(guid, field, guid)
   *
   * @param guid
   * @param field
   * @return
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public String readFieldAsSingleton(String guid, String field) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    return readFieldAsSingleton(guid, field, guid);
  }

  /**
   * Reads the value for a key from the GNRS server or the given guid. The guid of the user attempting access is also needed. Signs
   * the query using the private key of the user associated with the guid.
   *
   * @param guid
   * @param field
   * @param reader
   * @return
   * @throws RuntimeException if the query is not accepted by the server.
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public String readField(String guid, String field, String reader) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(reader, Protocol.READ, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.READER, reader);
    //String command = signMessage(reader, Protocol.LOOKUP + QUERYPREFIX + Protocol.GUID + VALSEP + guid + KEYSEP + Protocol.FIELD + VALSEP + URLEncoder.encode(field, "UTF-8") + KEYSEP + Protocol.READER + VALSEP + reader);
    String response = sendGetCommand(command);

    return checkResponse(response, command);
  }

  public String readFieldAsSingleton(String guid, String field, String reader) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(reader, Protocol.READONE, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.READER, reader);
    //String command = signMessage(reader, Protocol.LOOKUP + QUERYPREFIX + Protocol.GUID + VALSEP + guid + KEYSEP + Protocol.FIELD + VALSEP + URLEncoder.encode(field, "UTF-8") + KEYSEP + Protocol.READER + VALSEP + reader);
    String response = sendGetCommand(command);

    return checkResponse(response, command);
  }

  private String checkResponse(String response, String command) {
    if (throwRuntimeExceptions) {
      if (response.startsWith(Protocol.BADRESPONSE)) {
        String results[] = response.split(" ");
        if (results.length == 1) {
          throw (new RuntimeException("Missing bad response indicator: " + response));
        } else if (results.length == 2) {
          throw (new BadResponseException(results[1], command));
        } else {
          throw (new BadResponseException(results[1], results[2], command));
        }
      }
    }
    return response;
  }

  /**
   * Updates the access control list of the given user's field on the GNRS server to include the guid specified in the reader param.
   * The reader can be a guid of a user or a group guid or +ALL+ which means anyone can access the field. The field can be also be
   * +ALL+ which means all fields can be read by the reader. Signs the query using the private key of the user associated with the
   * guid.
   *
   * Query format: aclAdd?guid=<guid>&field=<field>&reader=<allowedreaderguid>&signature=<signature>
   *
   * @param guid
   * @param field
   * @param reader
   * @throws RuntimeException if the query is not accepted by the server.
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   */
  private void addToACL(MetaDataTypeName accessType, String guid, String field, String reader) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    addToACL(accessType.name(), guid, field, reader);
  }

  private void removeFromACL(MetaDataTypeName accessType, String guid, String field, String reader) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    removeFromACL(accessType.name(), guid, field, reader);
  }

  public void addToACL(String accessType, String guid, String field, String reader) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.ACLADD, Protocol.ACLTYPE, accessType, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.ACCESSER, reader);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  public void removeFromACL(String accessType, String guid, String field, String reader) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.ACLREMOVE, Protocol.ACLTYPE, accessType, Protocol.GUID, guid, Protocol.FIELD, field, Protocol.ACCESSER, reader);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  public void getACL(String accessType, String guid, String field, String reader) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.ACLRETRIEVE, Protocol.ACLTYPE, accessType, Protocol.GUID, guid, Protocol.FIELD, field);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  public void addToGroup(String guid, String member, String writer) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.ADDTOGROUP, Protocol.GUID, guid, Protocol.MEMBER, member, Protocol.WRITER, writer);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  public void removeFromGroup(String guid, String member, String writer) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.REMOVEFROMGROUP, Protocol.GUID, guid, Protocol.MEMBER, member, Protocol.WRITER, writer);
    String response = sendGetCommand(command);

    checkResponse(response, command);
  }

  public String getGroupMembers(String guid, String reader) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(guid, Protocol.GETGROUPMEMBERS, Protocol.GUID, guid, Protocol.READER, reader);
    String response = sendGetCommand(command);

    return checkResponse(response, command);
  }

  /**
   * Creates a http query string from the given action string and a variable number of key and value pairs.
   *
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws IOException
   */
  public String createQuery(String action, String... keysAndValues) throws IOException {
    String key;
    String value;
    StringBuilder result = new StringBuilder(action + QUERYPREFIX);

    for (int i = 0; i < keysAndValues.length; i = i + 2) {
      key = keysAndValues[i];
      value = keysAndValues[i + 1];
      result.append(URIEncoderDecoder.quoteIllegal(key, "") + VALSEP + URIEncoderDecoder.quoteIllegal(value, "") + (i + 2 < keysAndValues.length ? KEYSEP : ""));
    }
    return result.toString();
  }

  /**
   * Creates a http query string from the given action string and a variable number of key and value pairs with a signature
   * parameter. The signature is generated from the query signed by the given guid.
   *
   * @param guid
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public String createAndSignQuery(String guid, String action, String... keysAndValues) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String key;
    String value;
    StringBuilder encodedString = new StringBuilder(action + QUERYPREFIX);
    StringBuilder unencodedString = new StringBuilder(action + QUERYPREFIX);

    // map over the leys and values to produce the query
    for (int i = 0; i < keysAndValues.length; i = i + 2) {
      key = keysAndValues[i];
      value = keysAndValues[i + 1];
      encodedString.append(URIEncoderDecoder.quoteIllegal(key, "") + VALSEP + URIEncoderDecoder.quoteIllegal(value, "") + (i + 2 < keysAndValues.length ? KEYSEP : ""));
      unencodedString.append(key + VALSEP + value + (i + 2 < keysAndValues.length ? KEYSEP : ""));
    }
    getLogger().finer("Encoded: " + encodedString.toString());
    getLogger().finer("Unencoded: " + unencodedString.toString());

    // generate the signature from the unencoded query
    String signature = signDigestOfMessage(guid, unencodedString.toString());
    // return the encoded query with the signature appended
    return encodedString.toString() + KEYSEP + Protocol.SIGNATURE + VALSEP + signature;
  }

  /**
   * Signs a digest of a message using private key of the given guid.
   *
   * @param guid
   * @param message
   * @return a signed digest of the message string
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  private String signDigestOfMessage(String guid, String message) throws InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    KeyPair keyPair = getKeyPairFromPreferences(guid);
    PrivateKey privateKey = keyPair.getPrivate();
    Signature instance = Signature.getInstance(Protocol.SIGNATUREALGORITHM);

    instance.initSign(privateKey);
    instance.update(message.getBytes());
    byte[] signature = instance.sign();

    return ByteUtils.toHex(signature);
  }

  /**
   * Saves the public/private key pair to preferences for the given user.
   *
   * @param username
   * @param keyPair
   */
  private void saveKeyPairToPreferences(String username, KeyPair keyPair) {
    String publicString = ByteUtils.toHex(keyPair.getPublic().getEncoded());
    String privateString = ByteUtils.toHex(keyPair.getPrivate().getEncoded());
    getLogger().finer("Save public key: " + publicString);
    getLogger().finer("Save private key: " + privateString);
    userPreferencess.put(username + "-public", publicString);
    userPreferencess.put(username + "-private", privateString);
  }

  /**
   * Retrieves the public/private key pair for the given user.
   *
   * @param username
   * @return the keypair
   */
  public KeyPair getKeyPairFromPreferences(String username) {
    String publicString = userPreferencess.get(username + "-public", "");
    String privateString = userPreferencess.get(username + "-private", "");
    getLogger().finer("Retrieved public key: " + publicString);
    getLogger().finer("Retrieved private key: " + privateString);
    if (!publicString.isEmpty() && !privateString.isEmpty()) {
      try {
        byte[] encodedPublicKey = ByteUtils.hexStringToByteArray(publicString);
        byte[] encodedPrivateKey = ByteUtils.hexStringToByteArray(privateString);

        KeyFactory keyFactory = KeyFactory.getInstance(Protocol.RASALGORITHM);
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
        PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(encodedPrivateKey);
        PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);

        return new KeyPair(publicKey, privateKey);
      } catch (NoSuchAlgorithmException e) {
        getLogger().severe(e.toString());
        return null;
      } catch (InvalidKeySpecException e) {
        getLogger().severe(e.toString());
        return null;
      }

    } else {
      return null;
    }
  }
  
  private static final String RELEASEDRUNSETNAME = "released";

  private String guessHost() {
    if (host != null) {
      return host;
    }
    host = EC2Installer.retrieveHostname(RELEASEDRUNSETNAME, 0);
    if (host != null) {
      return host;
    }
    // give up and assume local
    return "127.0.0.1";
  }

  /**
   * Sends a HTTP get with given queryString to the host specified by the {@link host} field.
   *
   * @param queryString
   * @return result of get as a string
   */
  public String sendGetCommand(String queryString) {
    host = guessHost();
    HttpURLConnection connection = null;
    try {
      String urlString = "http://" + host + ":" + port + "/GNRS/" + queryString;
      getLogger().fine("Sending: " + urlString);
      URL serverURL = new URL(urlString);
      //set up out communications stuff
      connection = null;

      //Set up the initial connection
      connection = (HttpURLConnection) serverURL.openConnection();
      connection.setRequestMethod("GET");
      connection.setDoOutput(true);
      connection.setReadTimeout(0);

      connection.connect();

      //read the result from the server
      BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));

      String response = null;
      int cnt = 3;
      do {
        try {
          response = rd.readLine(); // we only expect one line to be sent
          break;
        } catch (java.net.SocketTimeoutException e) {
          getLogger().info("Get Response timed out. Trying " + cnt + " more times.");
        }
      } while (cnt-- > 0);
      try {
        // in theory this close should allow the keepalive mechanism to operate correctly
        // http://docs.oracle.com/javase/6/docs/technotes/guides/net/http-keepalive.html
        rd.close();
      } catch (IOException e) {
        getLogger().warning("Problem closing the HttpURLConnection's stream.");
      }
      getLogger().fine("Received: " + response);
      if (response != null) {
        return response;
      } else {
        throw (new RuntimeException("No response to command: " + queryString));
      }
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (ProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      //close the connection, set all objects to null
      connection.disconnect();
      connection = null;
    }
    return "";
  }

  /**
   * Return the host we connect to.
   *
   * @return the host we connect to for the GNRS http server
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the host we connect to.
   *
   * @param host which should be something like http://ec2-23-22-88-241.compute-1.amazonaws.com
   */
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Return the port on the host we connect to.
   *
   * @return the port used to connect to the GNRS http server
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the port on the host we connect to.
   *
   * @param port which is usually 8080 (or 80 in the case of a root created server)
   */
  public void setPort(int port) {
    this.port = port;
  }

  private String getDefaultLogLevel() {
    return defaultLogLevel;
  }

  private void setDefaultLogLevel(String defaultLogLevel) {
    this.defaultLogLevel = defaultLogLevel;
  }

  /**
   * Enables the throwing of runtime exceptions when a bad response is received
   */
  public void enableThrowRuntimeExceptions() {
    this.throwRuntimeExceptions = true;
  }

  /**
   * Disables the throwing of runtime exceptions when a bad response is received
   */
  public void disableThrowRuntimeExceptions() {
    this.throwRuntimeExceptions = false;
  }

  private Logger getLogger() {
    if (!initRun) {
      Logging.setupLogger(LOGGER, defaultLogLevel, defaultLogLevel, "log" + "/HTTPClient.xml");
      initRun = true;
    }
    return LOGGER;
  }

  // TEST CODE
  /**
   * Main which runs the runClientTest method.
   *
   */
  public static void main(String argv[]) throws Exception {
    HTTPClient client = new HTTPClient();
    client.setDefaultLogLevel("FINE"); // see more for the command line
    if (argv.length > 0) {
      client.setHost(argv[0]);
    }
    // run this one, which runs all the others
    //client.runClientTest();
    // or one or more of these
    //client.runAliasTest();
    //client.runDBTest();
    //client.runGroupTest();
    //client.setHost("23.21.120.250");

  }

  private void clearDatabase() {
    // using the hostname as the passkey insures that we can't accidentally
    // enable demo mode on an operational server with this code and wipe out the database
    sendGetCommand("demo?passkey=127.0.0.1:8080"); // turn on demo mode
    //sendGetCommand("demo?passkey=23.21.120.250:8080"); // turn on demo mode
    // When running this as a test we need to clear the database otherwise the users will already exist
    sendGetCommand(Protocol.DELETEALLRECORDS); // clear the database
    sendGetCommand("demo?passkey=off"); // turn off demo mode
  }

  /**
   * Send a bunch of test queries to the server
   */
  private void runClientTest() {
    try {

      clearDatabase();

      String unknownUser = lookupUserGuid("unknown");
      getLogger().info("Result of lookup for unknown is : " + unknownUser + " which is " + (isBadResponse(unknownUser, Protocol.BADACCOUNT) ? "correct." : "incorrect."));

      String westyGuid = lookupUserGuid("westy");
      String samGuid = lookupUserGuid("sam");

      if (isBadResponse(westyGuid, Protocol.BADACCOUNT)) {
        westyGuid = registerNewUser("westy");

      }
      if (isBadResponse(samGuid, Protocol.BADACCOUNT)) {
        samGuid = registerNewUser("sam");

      }

      getLogger().info("Result of registerNewUser for westy is: " + westyGuid);
      getLogger().info("Result of registerNewUser for sam is: " + samGuid);

      createField(westyGuid, "location", "work");
      createField(westyGuid, "ssn", "000-00-0000");
      createField(westyGuid, "password", "666flapJack");
      createField(westyGuid, "address", "100 Hinkledinkle Drive");


      // read my own field
      String result = readField(westyGuid, "location", westyGuid);

      getLogger().info("Result of read of westy location by westy is: " + result);

      // read another field
      result = readField(westyGuid, "ssn", westyGuid);
      getLogger().info("Result of read of westy ssn by westy is: " + result);

      // read another field
      result = readField(westyGuid, "address", westyGuid);
      getLogger().info("Result of read of westy address by westy is: " + result);

      addToACL(MetaDataTypeName.READ_WHITELIST, westyGuid, "location", samGuid);



      result = readField(westyGuid, "location", samGuid);
      if (!result.startsWith(Protocol.BADRESPONSE)) {
        getLogger().info("Result of read of westy location by sam is " + result);
      } else {
        getLogger().info("Result of read of westy location by sam was incorrectly rejected: " + result);
      }

      String barneyGuid = lookupUserGuid("barney");

      if (isBadResponse(barneyGuid, Protocol.BADACCOUNT)) {
        barneyGuid = registerNewUser("barney");

      }

      createField(barneyGuid, "cell", "413-555-1234");
      createField(barneyGuid, "address", "100 Main Street");


      // let anybody read barney's cell field
      addToACL(MetaDataTypeName.READ_WHITELIST, barneyGuid, "cell", Protocol.EVERYONE);


      result = readField(barneyGuid, "cell", samGuid);
      if (!result.startsWith(Protocol.BADRESPONSE)) {
        getLogger().info("Result of read of barney's cell by sam is " + result);
      } else {
        getLogger().info("Result of read of barney's cell by sam was incorrectly rejected: " + result);
      }


      result = readField(barneyGuid, "address", samGuid);
      if (!result.startsWith(Protocol.BADRESPONSE)) {
        getLogger().info("Result of read of barney's address by sam is " + result + " which is wrong because it should have been rejected.");
      } else {
        getLogger().info("Result of read of barney's address by sam was correctly rejected: " + result);
      }

      result = readField(barneyGuid, "cell", westyGuid);
      if (!result.startsWith(Protocol.BADRESPONSE)) {
        getLogger().info("Result of read of barney's cell by westy is " + result);
      } else {
        getLogger().info("Result of read of barney's cell by westy was incorrectly rejected: " + result);
      }

      String superuserGuid = lookupUserGuid("superuser");
      if (isBadResponse(superuserGuid, Protocol.BADACCOUNT)) {
        superuserGuid = registerNewUser("superuser");

      }
      getLogger().info("Result of registerNewUser for superuser is: " + superuserGuid);

      // let superuser read any of barney's fields
      addToACL(MetaDataTypeName.READ_WHITELIST, barneyGuid, Protocol.ALLFIELDS, superuserGuid);


      result = readField(barneyGuid, "cell", superuserGuid);
      getLogger().info("Result of read of barney's cell by superuserGuid is: " + result);

      result = readField(barneyGuid, "address", superuserGuid);
      getLogger().info("Result of read of barney's address by superuserGuid is: " + result);

      runGroupTest(false); // run group test but don't clear database first

      // test some of the new DB style commands

      runDBTest(false);
      runAliasTest(false);

    } catch (RuntimeException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (IOException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (SignatureException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    }
  }

  private void runDBTest() {
    runDBTest(true);
  }

  private void runDBTest(boolean clearDatabase) {
    if (clearDatabase) {
      clearDatabase();
    }

    try {
      String result;

      String westyGuid = lookupUserGuid("westy");
      if (isBadResponse(westyGuid, Protocol.BADACCOUNT)) {
        westyGuid = registerNewUser("westy");

      }
      createField(westyGuid, "cats", "whacky");

      appendValuesUsingListWithDuplication(westyGuid, "cats", new ArrayList(Arrays.asList("hooch", "maya", "red", "sox", "toby")));

      result = readField(westyGuid, "cats");

      if (!result.startsWith(Protocol.BADRESPONSE)) {
        getLogger().info("Result of read of westy's cats by westy is " + result);
      } else {
        getLogger().info("Result of read of westy's cats by westy was incorrectly rejected: " + result);
      }

      result = readFieldAsSingleton(westyGuid, "cats");

      if (!result.startsWith(Protocol.BADRESPONSE)) {
        getLogger().info("Result of *SINGLETON* read of westy's cats by westy is " + result);
      } else {
        getLogger().info("Result of read of westy's cats by westy was incorrectly rejected: " + result);
      }

      removeValuesUsingList(westyGuid, "cats", new ArrayList(Arrays.asList("maya", "toby")));

      result = readField(westyGuid, "cats");

      if (!result.startsWith(Protocol.BADRESPONSE)) {
        getLogger().info("Result of read of westy's cats after removing maya and toby by westy is " + result);
      } else {
        getLogger().info("Result of read of westy's cats by westy was incorrectly rejected: " + result);
      }

      // test some error messages
      try {

        enableThrowRuntimeExceptions();

        try {
          createField(westyGuid, "cats", "whacky");
          getLogger().info("This should not have been called, should have got a BadResponseException.");
        } catch (BadResponseException e) {
          getLogger().info("This is correct: " + e.toString());
        } catch (RuntimeException e) {
          getLogger().info("Unexpected Runtime Exception: " + e.toString());
        }


        try {
          appendValueWithDuplication(westyGuid, "dogs", "freddybub");
          getLogger().info("This should not have been called, should have got a BadResponseException.");
        } catch (BadResponseException e) {
          getLogger().info("This is correct: " + e.toString());
        } catch (RuntimeException e) {
          getLogger().info("Unexpected Runtime Exception: " + e.toString());
        }
      } finally {
        disableThrowRuntimeExceptions();
      }

      appendValue(westyGuid, "cats", "fred");
      result = readField(westyGuid, "cats");
      getLogger().info("Result of read of westy's cats after unique append fred is " + result);

      appendValue(westyGuid, "cats", "fred");
      result = readField(westyGuid, "cats");
      getLogger().info("Result of read of westy's cats after second unique append fred is " + result);

      appendOrCreate(westyGuid, "dogs", "bear");
      result = readField(westyGuid, "dogs");
      getLogger().info("Result of read of westy's dogs after append or create is " + result);

      appendOrCreateUsingList(westyGuid, "dogs", new ArrayList(Arrays.asList("wags", "tucker")));
      result = readField(westyGuid, "dogs");
      getLogger().info("Result of read of westy's dogs after a second append or create list is " + result);

      replaceOrCreate(westyGuid, "goats", "sue");
      result = readField(westyGuid, "goats");
      getLogger().info("Result of read of westy's goats after replace or create is " + result);

      replaceOrCreate(westyGuid, "goats", "william");
      result = readField(westyGuid, "goats");
      getLogger().info("Result of read of westy's goats after a second replace or create is " + result);

      replaceOrCreateUsingList(westyGuid, "goats", new ArrayList(Arrays.asList("dink", "tink")));
      result = readField(westyGuid, "goats");
      getLogger().info("Result of read of westy's goats after a third replace or create list is " + result);




    } catch (RuntimeException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (IOException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (SignatureException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    }
  }

  private void runGroupTest() {
    runGroupTest(true);
  }

  private void runGroupTest(boolean clearDatabase) {
    if (clearDatabase) {
      clearDatabase();
    }
    try {
      String result;

      String westyGuid = lookupUserGuid("westy");
      if (isBadResponse(westyGuid, Protocol.BADACCOUNT)) {
        westyGuid = registerNewUser("westy");

      }
      String samGuid = lookupUserGuid("sam");
      if (isBadResponse(samGuid, Protocol.BADACCOUNT)) {
        samGuid = registerNewUser("sam");

      }
      String barneyGuid = lookupUserGuid("barney");
      if (isBadResponse(barneyGuid, Protocol.BADACCOUNT)) {
        barneyGuid = registerNewUser("barney");

      }
      // TEST GROUPS (new version)

      String mygroupGuid = lookupUserGuid("mygroup");
      if (isBadResponse(mygroupGuid, Protocol.BADACCOUNT)) {
        mygroupGuid = registerNewUser("mygroup");
      }

      addToGroup(mygroupGuid, westyGuid, mygroupGuid);
      addToGroup(mygroupGuid, samGuid, mygroupGuid);
      addToGroup(mygroupGuid, barneyGuid, mygroupGuid);

      String groupMembers = getGroupMembers(mygroupGuid, mygroupGuid);

//            String command = createQuery(Protocol.CREATEGROUPV1, Protocol.NAME, "mygroup", Protocol.PUBLICKEY, "dummykey");
//            String response = sendGetCommand(command);
//            
//            command = createQuery(Protocol.LOOKUPGROUPV1, Protocol.NAME, "mygroup");
//            String groupGuid = sendGetCommand(command);
//
//            command = createQuery(Protocol.ADDTOGROUPV1, Protocol.GROUP, groupGuid, Protocol.GUID, westyGuid);
//            response = sendGetCommand(command);
//            
//            command = createQuery(Protocol.ADDTOGROUPV1, Protocol.GROUP, groupGuid, Protocol.GUID, samGuid);
//            response = sendGetCommand(command);
//           
//            command = createQuery(Protocol.ADDTOGROUPV1, Protocol.GROUP, groupGuid, Protocol.GUID, barneyGuid);
//            response = sendGetCommand(command);
//            
//            command = createQuery(Protocol.GETGROUPMEMBERSV1, Protocol.GROUP, groupGuid);
//            String groupMembers = sendGetCommand(command);

      getLogger().info("Group members of myGroup: " + groupMembers);

      String groupAccessUserGuid = lookupUserGuid("groupAccessUser");
      if (isBadResponse(groupAccessUserGuid, Protocol.BADACCOUNT)) {
        groupAccessUserGuid = registerNewUser("groupAccessUser");
      }

      createField(groupAccessUserGuid, "age", "43");
      createField(groupAccessUserGuid, "hometown", "whoville");

      addToACL(MetaDataTypeName.READ_WHITELIST, groupAccessUserGuid, "hometown", mygroupGuid);

      result = readField(groupAccessUserGuid, "age", westyGuid);
      if (!result.startsWith(Protocol.BADRESPONSE)) {
        getLogger().info("Result of read of groupAccessUser's age by westy is " + result + " which is wrong because it should have been rejected.");
      } else {
        getLogger().info("Result of read of groupAccessUser's age by westy was correctly rejected: " + result);
      }

      result = readField(groupAccessUserGuid, "hometown", westyGuid);
      if (!result.startsWith(Protocol.BADRESPONSE)) {
        getLogger().info("Result of read of groupAccessUser's hometown by westy is " + result);
      } else {
        getLogger().info("Result of read of groupAccessUser's hometown by westy was incorrectly rejected: " + result);
      }

      removeFromGroup(mygroupGuid, westyGuid, mygroupGuid);
//            command = createQuery(Protocol.REMOVEFROMGROUPV1, Protocol.GROUP, groupGuid, Protocol.GUID, westyGuid);
//            response = sendGetCommand(command);
//            
//            
//            command = createQuery(Protocol.GETGROUPMEMBERSV1, Protocol.GROUP, groupGuid);
//            groupMembers = sendGetCommand(command);

      groupMembers = getGroupMembers(mygroupGuid, mygroupGuid);

      getLogger().info("Group members of myGroup (minus " + westyGuid + " we hope): " + groupMembers);
    } catch (RuntimeException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (IOException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (SignatureException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    }
  }

  private void runAliasTest() {
    runAliasTest(true);
  }

  private void runAliasTest(boolean clearDatabase) {
    if (clearDatabase) {
      clearDatabase();
    }

    try {

      String westyGuid = lookupUserGuid("westy");
      if (isBadResponse(westyGuid, Protocol.BADACCOUNT)) {
        westyGuid = registerNewUser("westy");
      }

      addAlias(westyGuid, "david@westy.org");



    } catch (RuntimeException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (IOException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    } catch (SignatureException e) {
      getLogger().severe(e.toString());
      e.printStackTrace();
    }
  }

 

  private boolean isBadResponse(String response, String indicator) {
    if (response.startsWith(Protocol.BADRESPONSE)) {
      String results[] = response.split(" ");
      if (results.length < 2) {
        throw (new RuntimeException("Missing bad response indicator: " + response));
      } else {
        return indicator.equals(results[1]);
      }
    } else {
      return false;
    }
  }

  class BadResponseException extends RuntimeException {

    String problemIndicator;
    String additionalInfo;
    String command;

    public BadResponseException(String problemIndicator, String command) {;
      this.problemIndicator = problemIndicator;
    }

    public BadResponseException(String problemIndicator, String additionalInfo, String command) {
      this.problemIndicator = problemIndicator;
      this.additionalInfo = additionalInfo;
    }

    @Override
    public String toString() {
      return "BadResponseException{" + "problemIndicator=" + problemIndicator + ", additionalInfo=" + additionalInfo + ", command=" + command + '}';
    }
  }
  public static String Version = "$Revision$";
}
