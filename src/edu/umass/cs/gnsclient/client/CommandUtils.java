/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnscommon.GNSCommandProtocol;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATURE_ALGORITHM;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.AclException;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidFieldException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGroupException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidUserException;
import edu.umass.cs.gnscommon.exceptions.client.VerificationException;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandType;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.DelayProfiler;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Date;
import java.util.Random;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class CommandUtils {

  private static Signature signatureInstance;
  private static Random random;

  static {
    try {
      signatureInstance = Signature.getInstance(SIGNATURE_ALGORITHM);
      random = new Random();
    } catch (NoSuchAlgorithmException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Unable to initialize for authentication:{0}", e);
    }
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs.
   *
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public static JSONObject createCommand(String action, Object... keysAndValues) throws ClientException {
    long startTime = System.currentTimeMillis();
    try {
      JSONObject result = new JSONObject();
      String key;
      Object value;
      result.put(GNSCommandProtocol.COMMANDNAME, action);
      for (int i = 0; i < keysAndValues.length; i = i + 2) {
        key = (String) keysAndValues[i];
        value = keysAndValues[i + 1];
        result.put(key, value);
      }
      DelayProfiler.updateDelay("createCommand", startTime);
      return result;
    } catch (JSONException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  public static JSONObject createCommand(CommandType commandType, String action,
          Object... keysAndValues)
          throws ClientException {
    try {
      JSONObject result = createCommand(action, keysAndValues);
      result.put(GNSCommandProtocol.COMMAND_INT, commandType.getInt());
      return result;
    } catch (JSONException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs with a signature parameter. The signature is
   * generated from the query signed by the given guid.
   *
   * @param privateKey
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws ClientException
   */
  @Deprecated
  // This is deprecated because the method below will be the one we end up using once the
  // transtion to using enums is finished.
  public static JSONObject createAndSignCommand(PrivateKey privateKey, String action,
          Object... keysAndValues) throws ClientException {
    try {
      JSONObject result = createCommand(action, keysAndValues);
      result.put(GNSCommandProtocol.TIMESTAMP, Format.formatDateISO8601UTC(new Date()));
      result.put(GNSCommandProtocol.SEQUENCE_NUMBER, getRandomRequestId());
      String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
      String signatureString = signDigestOfMessage(privateKey, canonicalJSON);
      result.put(GNSCommandProtocol.SIGNATURE, signatureString);
      return result;
    } catch (ClientException | NoSuchAlgorithmException | InvalidKeyException | SignatureException | JSONException | UnsupportedEncodingException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  /**
   * Creates a command object from the given CommandType and a variable
   * number of key and value pairs with a signature parameter. The signature is
   * generated from the query signed by the given guid.
   *
   * @param commandType
   * @param privateKey
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws ClientException
   */
  // FIXME: Temporarily hacked version for adding new command type integer. Will clean up once
  // transition is done.
  // The action argument will be going away and be ultimately replaced by the
  // enum string.
  public static JSONObject createAndSignCommand(CommandType commandType,
          PrivateKey privateKey, String action, Object... keysAndValues)
          throws ClientException {
    try {
      JSONObject result = createAndSignCommand(privateKey, action, keysAndValues);
      result.put(GNSCommandProtocol.COMMAND_INT, commandType.getInt());
      // Temp hack: need to redo the signature done above
      result.remove(GNSCommandProtocol.SIGNATURE);
      String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
      String signatureString = signDigestOfMessage(privateKey, canonicalJSON);
      result.put(GNSCommandProtocol.SIGNATURE, signatureString);
      return result;
    } catch (JSONException | NoSuchAlgorithmException | InvalidKeyException | SignatureException | UnsupportedEncodingException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  /**
   * Signs a digest of a message using private key of the given guid.
   *
   * @param privateKey
   * @param message
   * @return a signed digest of the message string
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws java.io.UnsupportedEncodingException
   * 
   * arun: This method need to be synchronized, otherwise it will result in corrupted signatures.
   */
  public synchronized static String signDigestOfMessage(PrivateKey privateKey, String message)
          throws NoSuchAlgorithmException, InvalidKeyException,
          SignatureException, UnsupportedEncodingException {
    signatureInstance.initSign(privateKey);
    signatureInstance.update(message.getBytes("UTF-8"));
    byte[] signedString = signatureInstance.sign();
    // FIXME CHANGE THIS TO BASE64 (below) TO SAVE SOME SPACE ONCE THE IOS CLIENT IS UPDATED AS WELL
    String result = ByteUtils.toHex(signedString);
    //String result = Base64.encodeToString(signedString, false);
    return result;
  }

  /**
   * Checks the response from a command request for proper syntax as well as
   * converting error responses into the appropriate thrown GNS exceptions.
   *
   * @param command
   * @param response
   * @return
   * @throws ClientException
   */
  public static String checkResponse(JSONObject command, String response) throws ClientException {
    // System.out.println("response:" + response);
    if (response.startsWith(GNSCommandProtocol.BAD_RESPONSE)) {
      String[] results = response.split(" ");
      // System.out.println("results length:" + results.length);
      if (results.length < 2) {
        throw new ClientException("Invalid bad response indicator: " + response + " Command: " + command.toString());
      } else if (results.length >= 2) {
        // System.out.println("results[0]:" + results[0]);
        // System.out.println("results[1]:" + results[1]);
        String error = results[1];
        // deal with the rest
        StringBuilder parts = new StringBuilder();
        for (int i = 2; i < results.length; i++) {
          parts.append(" ");
          parts.append(results[i]);
        }
        String rest = parts.toString();
        if (error.startsWith(GNSCommandProtocol.BAD_SIGNATURE)) {
          throw new EncryptionException();
        }
        if (error.startsWith(GNSCommandProtocol.BAD_GUID)
                || error.startsWith(GNSCommandProtocol.BAD_ACCESSOR_GUID)
                || error.startsWith(GNSCommandProtocol.DUPLICATE_GUID)
                || error.startsWith(GNSCommandProtocol.BAD_ACCOUNT)) {
          throw new InvalidGuidException(error + rest);
        }
        if (error.startsWith(GNSCommandProtocol.DUPLICATE_FIELD)) {
          throw new InvalidFieldException(error + rest);
        }
        if (error.startsWith(GNSCommandProtocol.BAD_FIELD) || error.startsWith(GNSCommandProtocol.FIELD_NOT_FOUND)) {
          throw new FieldNotFoundException(error + rest);
        }
        if (error.startsWith(GNSCommandProtocol.BAD_USER) || error.startsWith(GNSCommandProtocol.DUPLICATE_USER)) {
          throw new InvalidUserException(error + rest);
        }
        if (error.startsWith(GNSCommandProtocol.BAD_GROUP) || error.startsWith(GNSCommandProtocol.DUPLICATE_GROUP)) {
          throw new InvalidGroupException(error + rest);
        }
        if (error.startsWith(GNSCommandProtocol.ACCESS_DENIED)) {
          throw new AclException(error + rest);
        }
        if (error.startsWith(GNSCommandProtocol.DUPLICATE_NAME)) {
          throw new DuplicateNameException(error + rest);
        }
        if (error.startsWith(GNSCommandProtocol.VERIFICATION_ERROR)) {
          throw new VerificationException(error + rest);
        }
        throw new ClientException("General command failure: " + error + rest);
      }
    }
    if (response.startsWith(GNSCommandProtocol.NULL_RESPONSE)) {
      return null;
    } else if (response.startsWith(GNSCommandProtocol.NO_ACTIVE_REPLICAS)) {
      throw new InvalidGuidException(response);
    } else {
      return response;
    }
  }
  
  public static long getRandomRequestId() {
    return random.nextLong();
  }

}
