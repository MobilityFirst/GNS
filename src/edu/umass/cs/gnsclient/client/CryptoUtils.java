package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.SessionKeys;
import org.json.JSONException;
import org.json.JSONObject;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.xml.bind.DatatypeConverter;
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

/**
 * Created by kanantharamu on 1/31/17.
 */
public class CryptoUtils {
    static final MessageDigest[] mds = new MessageDigest[Runtime
            .getRuntime().availableProcessors()];
    static final Cipher[] ciphers = new Cipher[2 * Runtime.getRuntime()
                    .availableProcessors()];
    static Signature[] signatureInstances = new Signature[2 * Runtime
            .getRuntime().availableProcessors()];
    static Random random;
    private static int sigIndex = 0;


    static {
        try {
            for (int i = 0; i < CryptoUtils.signatureInstances.length; i++) {
                CryptoUtils.signatureInstances[i] = Signature
                        .getInstance(GNSProtocol.SIGNATURE_ALGORITHM.toString());
            }
            CryptoUtils.random = new Random();
        } catch (NoSuchAlgorithmException e) {
            GNSConfig.getLogger().log(Level.SEVERE,
                    "Unable to initialize for authentication:{0}", e);
        }
    }
    static {
        for (int i = 0; i < CryptoUtils.mds.length; i++) {
            try {
                CryptoUtils.mds[i] = MessageDigest
                        .getInstance(GNSProtocol.DIGEST_ALGORITHM.toString());
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }


    static {
        for (int i = 0; i < CryptoUtils.ciphers.length; i++) {
            try {
                CryptoUtils.ciphers[i] = Cipher
                        .getInstance(GNSProtocol.SECRET_KEY_ALGORITHM.toString());
            } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }


    private static synchronized Signature getSignatureInstance() {
      return signatureInstances[sigIndex++ % signatureInstances.length];
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
      Signature signatureInstance = getSignatureInstance();
      synchronized (signatureInstance) {
        signatureInstance.initSign(privateKey);
        // iOS client uses UTF-8 - should switch to ISO-8859-1 to be consistent with
        // secret key version
        signatureInstance.update(message.getBytes("UTF-8"));
        byte[] signedString = signatureInstance.sign();
        // We used to encode this as a hex so we could send it with the html without
        // encoding. Not really necessary anymore for the socket based client,
        // but the iOS client does as well so we need to keep it like this.
        // Also note that the secret based method doesn't do this - it just returns a string
        // using the ISO-8859-1 charset.
        String result = DatatypeConverter.printHexBinary(signedString);
        //String result = ByteUtils.toHex(signedString);
        return result;
      }
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
