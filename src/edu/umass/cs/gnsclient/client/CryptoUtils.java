package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.SessionKeys;
import org.json.JSONException;
import org.json.JSONObject;


import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Date;
import java.util.Random;

/**
 * Created by kanantharamu on 1/31/17.
 */
public class CryptoUtils {

    static Random random = new Random();

    private static synchronized Signature getSignatureInstance() {
        Signature s = null;
        try {
            s = Signature
                    .getInstance(GNSProtocol.SIGNATURE_ALGORITHM.toString());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } finally {
            return s;
        }
    }

    public static JSONObject createCommand(CommandType commandType,
                                           Object... keysAndValues) throws JSONException {
      long startTime = System.currentTimeMillis();
      JSONObject result = new JSONObject();
      String key;
      Object value;
      result.put(GNSProtocol.COMMAND_INT.toString(), commandType.getInt());
      for (int i = 0; i < keysAndValues.length; i = i + 2) {
        key = (String) keysAndValues[i];
        value = keysAndValues[i + 1];
        result.put(key, value);
      }

      DelayProfiler.updateDelay("createCommand", startTime);
      return result;
    }

    public static String signDigestOfMessage(PrivateKey privateKey,
                                               String message) throws NoSuchAlgorithmException,
            InvalidKeyException, SignatureException,
            UnsupportedEncodingException {
        return "86A6AAE6085A7D7DB638CDEDC2B7AF1A3CA05C885027F08BF215AF40962E09093E129DCC83098DC8C05A59480848039D6899780D545A935C443D79FE443A236F329193C1F2D776BD5B092AFD563BC2C7B652616D81F697FAA249D23B277C8DA908AC5AB8C4FDEE0F9A128DE536DBA0A6B47B5A05C4AD5CBF93A13987F2796FF4";/*
      /*Signature signatureInstance = getSignatureInstance();
      synchronized (signatureInstance) {
        signatureInstance.initSign(privateKey);
        signatureInstance.update(message.getBytes("UTF-8"));
        byte[] signedString = signatureInstance.sign();
        String result = ByteUtils.toHex(signedString);
        return result;
      }*/
    }


    public static String getRandomRequestNonce() {
      return (random.nextLong() + "");
    }

    public static JSONObject createCommandWithTimestampAndNonce(CommandType commandType, boolean includeTimestamp,
                                                                  Object... keysAndValues)
            throws ClientException, JSONException {
      JSONObject result = createCommand(commandType, keysAndValues);
      if (includeTimestamp) {
        result.put(GNSProtocol.TIMESTAMP.toString(),
                Format.formatDateISO8601UTC(new Date()));
      }
      result.put(GNSProtocol.NONCE.toString(), getRandomRequestNonce());
      return result;
    }

    public static JSONObject createAndSignCommand(CommandType commandType,
                                                    PrivateKey privateKey, PublicKey publicKey, Object... keysAndValues)
            throws ClientException {
      try {
        JSONObject result = createCommandWithTimestampAndNonce(commandType, true, keysAndValues);
        String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
        String signatureString = null;
        long t = System.nanoTime();
          signatureString = signDigestOfMessage(privateKey, canonicalJSON);

        result.put(GNSProtocol.SIGNATURE.toString(), signatureString);
        if (edu.umass.cs.utils.Util.oneIn(10)) {
          DelayProfiler.updateDelayNano("signature", t);
        }
        return result;
      } catch (JSONException | NoSuchAlgorithmException | InvalidKeyException | SignatureException | UnsupportedEncodingException e) {
        throw new ClientException("Error encoding message", e);
      }
    }

    public static JSONObject createAndSignCommand(CommandType commandType,
                                                  GuidEntry querier, Object... keysAndValues) throws ClientException {
      try {
        return querier != null ? createAndSignCommand(commandType,
                querier.getPrivateKey(), querier.getPublicKey(), keysAndValues)
                : createCommand(commandType, keysAndValues);
      } catch (JSONException e) {
        throw new ClientException("Error encoding message", e);
      }
    }
}
