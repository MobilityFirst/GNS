/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.GNSResponseCode;
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
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.SessionKeys;
import edu.umass.cs.utils.SessionKeys.SecretKeyCertificate;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Date;
import java.util.Random;
import java.util.logging.Level;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy, arun
 */
public class CommandUtils {

  /* arun: at least as many instances as cores for parallelism. */
  private static Signature[] signatureInstances = new Signature[2 * Runtime.getRuntime().availableProcessors()];
  private static Random random;

  static {
    try {
      for (int i = 0; i < signatureInstances.length; i++) {
        signatureInstances[i] = Signature.getInstance(GNSCommandProtocol.SIGNATURE_ALGORITHM);
      }
      random = new Random();
    } catch (NoSuchAlgorithmException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Unable to initialize for authentication:{0}", e);
    }
  }

  private static int sigIndex = 0;

  private static synchronized Signature getSignatureInstance() {
    return signatureInstances[sigIndex++ % signatureInstances.length];
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs.
   *
   * @param keysAndValues
   * @return the query string
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public static JSONObject createCommand(CommandType commandType, Object... keysAndValues)
          throws ClientException {
    long startTime = System.currentTimeMillis();
    try {
      JSONObject result = new JSONObject();
      String key;
      Object value;
      result.put(GNSCommandProtocol.COMMAND_INT, commandType.getInt());
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

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs with a signature parameter. The signature is
   * generated from the query signed by the given guid.
   *
   * @param commandType
   * @param privateKey
   * @param keysAndValues
   * @return the query string
   * @throws ClientException
   */
  public static JSONObject createAndSignCommand(CommandType commandType, PrivateKey privateKey,
          Object... keysAndValues) throws ClientException {
    try {
      JSONObject result = createCommand(commandType, keysAndValues);
      result.put(GNSCommandProtocol.TIMESTAMP, Format.formatDateISO8601UTC(new Date()));
      result.put(GNSCommandProtocol.SEQUENCE_NUMBER, getRandomRequestId());
      String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
      long t = System.nanoTime();
      String signatureString = signDigestOfMessage(privateKey, canonicalJSON);
      result.put(GNSCommandProtocol.SIGNATURE, signatureString);
      if (edu.umass.cs.utils.Util.oneIn(10)) {
        DelayProfiler.updateDelayNano("signing", t);
      }

      return result;
    } catch (ClientException | NoSuchAlgorithmException | InvalidKeyException | SignatureException | JSONException | UnsupportedEncodingException e) {
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
   * arun: This method need to be synchronized over the signature
   * instance, otherwise it will result in corrupted signatures.
   */
  public static String signDigestOfMessage(PrivateKey privateKey, String message)
          throws NoSuchAlgorithmException, InvalidKeyException,
          SignatureException, UnsupportedEncodingException {
    Signature signatureInstance = getSignatureInstance();
    synchronized (signatureInstance) {
      signatureInstance.initSign(privateKey);
      signatureInstance.update(message.getBytes("UTF-8"));
      byte[] signedString = signatureInstance.sign();
      // FIXME CHANGE THIS TO BASE64 (below) TO SAVE SOME SPACE ONCE THE
      // IOS CLIENT IS UPDATED AS WELL
      String result = ByteUtils.toHex(signedString);
      // String result = Base64.encodeToString(signedString, false);
      return result;
    }
  }
  private static final MessageDigest[] mds = new MessageDigest[Runtime.getRuntime().availableProcessors()];

  static {
    for (int i = 0; i < mds.length; i++) {
      try {
        mds[i] = MessageDigest.getInstance(GNSCommandProtocol.DIGEST_ALGORITHM);
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }
  private static int mdIndex = 0;

  private static MessageDigest getMessageDigestInstance() {
    return mds[mdIndex++ % mds.length];
  }

  private static final Cipher[] ciphers = new Cipher[2 * Runtime.getRuntime().availableProcessors()];

  static {
    for (int i = 0; i < ciphers.length; i++) {
      try {
        ciphers[i] = Cipher.getInstance(GNSCommandProtocol.SECRET_KEY_ALGORITHM);
      } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  private static int cipherIndex = 0;

  private static Cipher getCipherInstance() {
    return ciphers[cipherIndex++ % ciphers.length];
  }

  public static String signDigestOfMessage(PrivateKey privateKey, PublicKey publicKey, String message)
          throws NoSuchAlgorithmException, InvalidKeyException,
          SignatureException, UnsupportedEncodingException,
          IllegalBlockSizeException, BadPaddingException,
          NoSuchPaddingException {
    SecretKey secretKey = SessionKeys.getOrGenerateSecretKey(
            publicKey, privateKey);
    MessageDigest md = getMessageDigestInstance();
    byte[] digest = null;
    byte[] body = message.getBytes(GNSCommandProtocol.CHARSET);
    synchronized (md) {
      digest = md.digest(body);
    }
    assert (digest != null);
    Cipher cipher = getCipherInstance();
    byte[] signature = null;
    synchronized (cipher) {
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      signature = cipher.doFinal(digest);
    }

    SecretKeyCertificate skCert = SessionKeys.getSecretKeyCertificate(publicKey);
    byte[] encodedSKCert = skCert.getEncoded(false);

    /* arun: Combining them like this because the rest of the GNS code seems poorly organized
    	   * to add more signature related fields in a systematic manner.
     */
    byte[] combined = new byte[Short.BYTES + signature.length + Short.BYTES + encodedSKCert.length];
    ByteBuffer.wrap(combined)
            // signature
            .putShort((short) signature.length).put(signature)
            // certificate
            .putShort((short) encodedSKCert.length).put(encodedSKCert);

    return new String(combined, GNSCommandProtocol.CHARSET);
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
    } else if (response.startsWith(
            GNSCommandProtocol.ACTIVE_REPLICA_EXCEPTION.toString()
    //    		GNSResponseCode.ACTIVE_REPLICA_EXCEPTION.toString()
    )) {
      throw new InvalidGuidException(response);
    } else {
      return response;
    }
  }

  // bridging hack 
  public static String checkResponse(JSONObject command, Object response) throws ClientException {
    return response instanceof String ? checkResponse(command, (String) response)
            : checkResponse(command, ((CommandValueReturnPacket) response));
  }

  /**
   * arun: This checkResponse method will replace the old one. There is no
   * reason to not directly use the received CommandValeReturnPacket.
   *
   * @param command
   * @param packet
   * @return Response as a string.
   * @throws ClientException
   */
  public static String checkResponse(JSONObject command,
          CommandValueReturnPacket packet) throws ClientException {
    // FIXME: arun: The line below disables this method
    if (true) {
      return checkResponse(command, packet.getReturnValue());
    }

    GNSResponseCode code = packet.getErrorCode();
    String response = packet.getReturnValue();
    if (!code.isError() && !code.isException()) {
      return (response.startsWith(GNSCommandProtocol.NULL_RESPONSE)) ? null
              : response;
    }
    // else error
    String errorSummary = code + ": " + response + ": "
            + packet.getSummary().toString();
    switch (code) {
      case SIGNATURE_ERROR:
        throw new EncryptionException(errorSummary);

      case BAD_GUID_ERROR:
      case BAD_ACCESSOR_ERROR:
      case BAD_ACCOUNT_ERROR:
        throw new InvalidGuidException(errorSummary);

      case FIELD_NOT_FOUND_ERROR:
        throw new FieldNotFoundException(errorSummary);
      case ACCESS_ERROR:
        throw new AclException(errorSummary);
      case VERIFICATION_ERROR:
        throw new VerificationException(errorSummary);
      case DUPLICATE_ID_EXCEPTION:
        throw new DuplicateNameException(errorSummary);
      case DUPLICATE_FIELD_EXCEPTION:
        throw new InvalidFieldException(errorSummary);

      case ACTIVE_REPLICA_EXCEPTION:
        throw new InvalidGuidException(errorSummary);
      case NONEXISTENT_NAME_EXCEPTION:
        throw new InvalidGuidException(errorSummary);

      /* FIXME: arun: NO_ERROR currently seems to conflate both "all ok"
			 * and "something not ok". This type should not be used.*/
      case NO_ERROR:
        return packet.getReturnValue();

      /**
       * FIXME: arun: All responses containing the BAD_RESPONSE string and NO_ERROR should
       * return this error code instead.
       */
      case BAD_RESPONSE:
        // FIXME: unclear if returning the value is the right action here.
        return packet.getReturnValue();

      case OK:
        return packet.getReturnValue();

      default:
        throw new ClientException(
                "Error received with an unknown response code: "
                + errorSummary);
    }
  }

  public static long getRandomRequestId() {
    return random.nextLong();
  }

}
