/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.GnsACLException;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnscommon.exceptions.client.GnsDuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.GnsFieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidFieldException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidGroupException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidGuidException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidUserException;
import edu.umass.cs.gnscommon.exceptions.client.GnsVerificationException;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.utils.DelayProfiler;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class CommandUtils {

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
   */
  public static String signDigestOfMessage(PrivateKey privateKey, String message) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, UnsupportedEncodingException {
    long startTime = System.currentTimeMillis();
    Signature instance = Signature.getInstance(GnsProtocol.SIGNATURE_ALGORITHM);
    instance.initSign(privateKey);
    instance.update(message.getBytes("UTF-8"));
    byte[] signature = instance.sign();
    String result = ByteUtils.toHex(signature);
    DelayProfiler.updateDelay("signDigestOfMessage", startTime);
    return result;
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
   * @throws GnsClientException
   */
  public static JSONObject createAndSignCommand(PrivateKey privateKey, String action, Object... keysAndValues) throws GnsClientException {
    long startTime = System.currentTimeMillis();
    try {
      JSONObject result = createCommand(action, keysAndValues);
      String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
      //String canonicalJSON = JSONUtils.getCanonicalJSONString(result);
      String signature = signDigestOfMessage(privateKey, canonicalJSON);
      //System.out.println("SIGNING THIS: " + canonicalJSON);
      result.put(GnsProtocol.SIGNATURE, signature);
      DelayProfiler.updateDelay("createAndSignCommand", startTime);
      return result;
    } catch (GnsClientException | NoSuchAlgorithmException | InvalidKeyException | SignatureException | JSONException | UnsupportedEncodingException e) {
      throw new GnsClientException("Error encoding message", e);
    }
  }

  /**
   * Checks the response from a command request for proper syntax as well as
   * converting error responses into the appropriate thrown GNS exceptions.
   *
   * @param command
   * @param response
   * @return
   * @throws GnsClientException
   */
  public static String checkResponse(JSONObject command, String response) throws GnsClientException {
    // System.out.println("response:" + response);
    if (response.startsWith(GnsProtocol.BAD_RESPONSE)) {
      String[] results = response.split(" ");
      // System.out.println("results length:" + results.length);
      if (results.length < 2) {
        throw new GnsClientException("Invalid bad response indicator: " + response + " Command: " + command.toString());
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
        if (error.startsWith(GnsProtocol.BAD_SIGNATURE)) {
          throw new EncryptionException();
        }
        if (error.startsWith(GnsProtocol.BAD_GUID) || error.startsWith(GnsProtocol.BAD_ACCESSOR_GUID) || error.startsWith(GnsProtocol.DUPLICATE_GUID) || error.startsWith(GnsProtocol.BAD_ACCOUNT)) {
          throw new GnsInvalidGuidException(error + rest);
        }
        if (error.startsWith(GnsProtocol.DUPLICATE_FIELD)) {
          throw new GnsInvalidFieldException(error + rest);
        }
        if (error.startsWith(GnsProtocol.BAD_FIELD) || error.startsWith(GnsProtocol.FIELD_NOT_FOUND)) {
          throw new GnsFieldNotFoundException(error + rest);
        }
        if (error.startsWith(GnsProtocol.BAD_USER) || error.startsWith(GnsProtocol.DUPLICATE_USER)) {
          throw new GnsInvalidUserException(error + rest);
        }
        if (error.startsWith(GnsProtocol.BAD_GROUP) || error.startsWith(GnsProtocol.DUPLICATE_GROUP)) {
          throw new GnsInvalidGroupException(error + rest);
        }
        if (error.startsWith(GnsProtocol.ACCESS_DENIED)) {
          throw new GnsACLException(error + rest);
        }
        if (error.startsWith(GnsProtocol.DUPLICATE_NAME)) {
          throw new GnsDuplicateNameException(error + rest);
        }
        if (error.startsWith(GnsProtocol.VERIFICATION_ERROR)) {
          throw new GnsVerificationException(error + rest);
        }
        throw new GnsClientException("General command failure: " + error + rest);
      }
    }
    if (response.startsWith(GnsProtocol.NULL_RESPONSE)) {
      return null;
    } else if (response.startsWith(GnsProtocol.NO_ACTIVE_REPLICAS)) {
      throw new GnsClientException(response);
    } else {
      return response;
    }
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs.
   *
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws edu.umass.cs.gnscommon.exceptions.client.GnsClientException
   */
  public static JSONObject createCommand(String action, Object... keysAndValues) throws GnsClientException {
    long startTime = System.currentTimeMillis();
    try {
      JSONObject result = new JSONObject();
      String key;
      Object value;
      result.put(GnsProtocol.COMMANDNAME, action);
      for (int i = 0; i < keysAndValues.length; i = i + 2) {
        key = (String) keysAndValues[i];
        value = keysAndValues[i + 1];
        result.put(key, value);
      }
      DelayProfiler.updateDelay("createCommand", startTime);
      return result;
    } catch (JSONException e) {
      throw new GnsClientException("Error encoding message", e);
    }
  }
  
}
