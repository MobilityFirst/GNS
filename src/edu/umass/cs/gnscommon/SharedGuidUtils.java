
package edu.umass.cs.gnscommon;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ShaOneHashFunction;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageExtractor;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.xml.bind.DatatypeConverter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;


public class SharedGuidUtils {


  public static String createGuidStringFromPublicKey(byte[] keyBytes) {
    byte[] publicKeyDigest = ShaOneHashFunction.getInstance().hash(keyBytes);
    return DatatypeConverter.printHexBinary(publicKeyDigest);
    //return ByteUtils.toHex(publicKeyDigest);
  }


  public static String createGuidStringFromBase64PublicKey(String publicKey) throws IllegalArgumentException {
    byte[] publickeyBytes = Base64.decode(publicKey);
    if (publickeyBytes == null) { // bogus public key
      throw new IllegalArgumentException();
    }
    return createGuidStringFromPublicKey(publickeyBytes);
  }


  public static JSONArray convertPublicKeysToGuids(JSONArray publicKeys) throws JSONException {
    JSONArray guids = new JSONArray();
    for (int i = 0; i < publicKeys.length(); i++) {
      // Special case
      try {
        if (publicKeys.getString(i).equals(GNSProtocol.ALL_GUIDS.toString())) {
          guids.put(GNSProtocol.ALL_GUIDS.toString());
        } else {
          guids.put(createGuidStringFromBase64PublicKey(publicKeys.getString(i)));
        }
      } catch (IllegalArgumentException e) {
        // ignore any bogus publicKeys
      }
    }
    return guids;
  }


  public static Set<String> convertPublicKeysToGuids(Set<String> publicKeys) {
    Set<String> guids = new HashSet<>();
    for (String publicKey : publicKeys) {
      try {
        guids.add(createGuidStringFromBase64PublicKey(publicKey));
      } catch (IllegalArgumentException e) {
        // ignore any bogus publicKeys
      }
    }
    return guids;
  }


  public static String findPublicKeyForGuid(String guid, Set<String> publicKeys) {
    if (guid != null) {
      for (String publicKey : publicKeys) {
        try {
          if (guid.equals(createGuidStringFromBase64PublicKey(publicKey))) {
            return publicKey;
          }
        } catch (IllegalArgumentException e) {
          // ignore any bogus publicKeys
        }
      }
    }
    return null;
  }


  public static boolean publicKeyListContainsGuid(String guid, Set<String> publicKeys) {
    return findPublicKeyForGuid(guid, publicKeys) != null;
  }

    public static Request getRequestStatic(byte[] msgBytes, NIOHeader header,
                                           Stringifiable<String> unstringer) throws RequestParseException {
      Request request = null;
      try {
        long t = System.nanoTime();
        if (JSONPacket.couldBeJSON(msgBytes)) {
          JSONObject json = new JSONObject(new String(msgBytes,
                  NIOHeader.CHARSET));
          MessageExtractor.stampAddressIntoJSONObject(header.sndr,
                  header.rcvr, json);
          request = (Request) Packet.createInstance(json, unstringer);
        } else {
          // parse non-JSON byteified form
          return fromBytes(msgBytes);
        }
        if (Util.oneIn(100)) {
          DelayProfiler.updateDelayNano(
                  "getRequest." + request.getRequestType(), t);
        }
      } catch (JSONException | UnsupportedEncodingException e) {
        throw new RequestParseException(e);
      }
      return request;
    }

    private static Request fromBytes(byte[] msgBytes)
            throws RequestParseException {
      switch (Packet.PacketType.getPacketType(ByteBuffer.wrap(msgBytes)
              .getInt())) {
        case COMMAND:
          return new CommandPacket(msgBytes);

        default:
          throw new RequestParseException(new RuntimeException(
                  "Unrecognizable request type"));
      }
    }
}
