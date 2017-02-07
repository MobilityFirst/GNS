package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.IOSKeyPairUtils;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.utils.DelayProfiler;
import org.json.JSONException;
import org.json.JSONObject;


import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Date;
import java.util.Random;

/**
 * Created by kanantharamu on 1/31/17.
 */
public class CryptoUtils {

    static Random random = new Random();


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
                                                    String guid, Object... keysAndValues)
            throws ClientException {
      try {
        JSONObject result = createCommandWithTimestampAndNonce(commandType, true, keysAndValues);
        String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
        String signatureString = null;
        long t = System.nanoTime();
          signatureString = IOSKeyPairUtils.signDigestOfMessage(guid, canonicalJSON);

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
        JSONObject r =  querier != null ? createAndSignCommand(commandType,
                querier.getGuid(), keysAndValues)
                : createCommand(commandType, keysAndValues);
        System.out.println("JSONObject: "+r);
        return r;
      } catch (JSONException e) {
        throw new ClientException("Error encoding message", e);
      }
    }
}
