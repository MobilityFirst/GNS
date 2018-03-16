package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.gigapaxos.interfaces.Request;
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
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/*
 * The reason why this is in a new class instead of GNSApp is to avoid additional dependencies
 * while translating code to Objective C.
 */


/**
 *
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
