package edu.umass.cs.gnsserver.gnsapp;

import com.mongodb.util.JSON;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageExtractor;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/*
 * The reason why this is in a new class instead of GNSApp is to avoid additional dependencies
 * while translating code to Objective C.
 */


public class GNSAppUtil {
    /**
     * This method avoids an unnecessary restringification (as is the case with
     * {@link GNSApp#getRequest(String)} above) by decoding the JSON, stamping it with
     * the sender information, and then creating a packet out of it.
     *
     * @param msgBytes
     * @param header
     * @param unstringer
     * @return Request constructed from msgBytes.
     * @throws RequestParseException
     */
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
          return checkSanity(fromBytes(msgBytes));
        }
        if (Util.oneIn(100)) {
          DelayProfiler.updateDelayNano(
                  "getRequest." + request.getRequestType(), t);
        }
          return checkSanity(request);
      } catch (JSONException | UnsupportedEncodingException|RequestParseException e) {
        throw new RequestParseException(e);
      }

    }


    public static Request checkSanity(Request request) throws RequestParseException {
        try {
            JSONObject requestJSON = new JSONObject(request.toString());
            JSONObject commandQuery = requestJSON.getJSONObject(GNSProtocol.COMMAND_QUERY.toString());
            if (commandQuery.has(GNSProtocol.COMMAND_INT.toString()) &&
                    commandQuery.getInt(GNSProtocol.COMMAND_INT.toString()) == CommandType.ReplaceUserJSON.getInt()) {
                JSONObject userJSON = new JSONObject(commandQuery.getString(GNSProtocol.USER_JSON.toString()));
                if (userJSON.has(GNSProtocol.LOCATION_FIELD_NAME_2D_SPHERE.toString())) {
                    JSONObject geoLocCurrent = userJSON.getJSONObject(GNSProtocol.LOCATION_FIELD_NAME_2D_SPHERE.toString());
                    if (geoLocCurrent.has("coordinates")) {
                        JSONArray coordinates = geoLocCurrent.getJSONArray("coordinates");
                        if(coordinates.get(0).getClass().equals(String.class) || coordinates.get(1).getClass().equals(String.class)) {
                            throw new RequestParseException(new Exception("Numeric value expected for location coordinates, string provided"));
                        }
                    }
                }
            }
        } catch (JSONException e) {
            //Pass
        }

        return request;
    }

    /**
     * This method should invert the implementation of the
     * {@link Byteable#toBytes()} method for GNSApp packets.
     *
     * @param msgBytes
     * @return a request
     * @throws RequestParseException
     */
    private static Request fromBytes(byte[] msgBytes)
            throws RequestParseException {
      switch (Packet.PacketType.getPacketType(ByteBuffer.wrap(msgBytes)
              .getInt())) {
        case COMMAND:
          return new CommandPacket(msgBytes);
        /* Currently only CommandPacket is Byteable, so we shouldn't come
               * here for anything else. */
        default:
          throw new RequestParseException(new RuntimeException(
                  "Unrecognizable request type"));
      }
    }
}
