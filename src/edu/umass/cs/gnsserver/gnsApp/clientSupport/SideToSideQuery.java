/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientSupport;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.asynch.ClientAsynchBase;
import edu.umass.cs.gnsclient.exceptions.EncryptionException;
import edu.umass.cs.gnsclient.exceptions.GnsACLException;
import edu.umass.cs.gnsclient.exceptions.GnsDuplicateNameException;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnsclient.exceptions.GnsFieldNotFoundException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidFieldException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidGroupException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidGuidException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidUserException;
import edu.umass.cs.gnsclient.exceptions.GnsVerificationException;
import static edu.umass.cs.gnscommon.GnsProtocol.ACCESS_DENIED;
import static edu.umass.cs.gnscommon.GnsProtocol.BAD_ACCESSOR_GUID;
import static edu.umass.cs.gnscommon.GnsProtocol.BAD_ACCOUNT;
import static edu.umass.cs.gnscommon.GnsProtocol.BAD_FIELD;
import static edu.umass.cs.gnscommon.GnsProtocol.BAD_GROUP;
import static edu.umass.cs.gnscommon.GnsProtocol.BAD_GUID;
import static edu.umass.cs.gnscommon.GnsProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GnsProtocol.BAD_SIGNATURE;
import static edu.umass.cs.gnscommon.GnsProtocol.BAD_USER;
import static edu.umass.cs.gnscommon.GnsProtocol.DUPLICATE_FIELD;
import static edu.umass.cs.gnscommon.GnsProtocol.DUPLICATE_GROUP;
import static edu.umass.cs.gnscommon.GnsProtocol.DUPLICATE_GUID;
import static edu.umass.cs.gnscommon.GnsProtocol.DUPLICATE_NAME;
import static edu.umass.cs.gnscommon.GnsProtocol.DUPLICATE_USER;
import static edu.umass.cs.gnscommon.GnsProtocol.FIELD_NOT_FOUND;
import static edu.umass.cs.gnscommon.GnsProtocol.NULL_RESPONSE;
import static edu.umass.cs.gnscommon.GnsProtocol.VERIFICATION_ERROR;
import edu.umass.cs.gnsserver.gnsApp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnsserver.main.GNS;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONException;

/**
 * A synchronized version of ClientAsynchBase for sending requests to other servers.
 *
 * @author westy
 */
public class SideToSideQuery extends ClientAsynchBase {

  private static final ConcurrentMap<Integer, CommandValueReturnPacket> resultMap
          = new ConcurrentHashMap<>(10, 0.75f, 3);
  private static long readTimeout = 10000;
  private static final Object monitor = new Object();
  private boolean debuggingEnabled = true;
  /**
   * A callback that notifys any waits and records the response
   */
  private static RequestCallback callback = (Request response) -> {
    CommandValueReturnPacket packet = (CommandValueReturnPacket) response;
    resultMap.put(packet.getClientRequestId(), packet);
    synchronized (monitor) {
      monitor.notifyAll();
    }
  };

  public SideToSideQuery() throws IOException {
  }

  public String fieldRead(String guid, String field) throws IOException, JSONException, GnsException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    int requestId = (int) fieldRead(guid, field, callback);
    CommandValueReturnPacket packet = waitForResponse(requestId);
    if (packet == null) {
      throw new GnsException("Packet not found in table " + requestId);
    } else {
      String returnValue = packet.getReturnValue();
      if (debuggingEnabled) {
        GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field read of " + packet.getServiceName() 
                + " got from " + packet.getResponder() + " this: " + returnValue);
      }
      return checkResponse(returnValue);
    }
  }

  public String fieldReadArray(String guid, String field) throws IOException, JSONException, GnsException {
    // FIXME: NEED TO FIX COMMANDPACKET AND FRIENDS TO USE LONG
    int requestId = (int) fieldReadArray(guid, field, callback);
    CommandValueReturnPacket packet = waitForResponse(requestId);
    if (packet == null) {
      throw new GnsException("Packet not found in table " + requestId);
    } else {
      String returnValue = packet.getReturnValue();
      if (debuggingEnabled) {
        GNS.getLogger().info("HHHHHHHHHHHHHHHHHHHHHHHHH Field read array of " + packet.getServiceName() 
                + " got from " + packet.getResponder() + " this: " + returnValue);
      }
      return checkResponse(returnValue);
    }
  }

  private static CommandValueReturnPacket waitForResponse(int id) throws GnsException {
    try {
      synchronized (monitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (!resultMap.containsKey(id)
                && (readTimeout == 0 || System.currentTimeMillis() - monitorStartTime < readTimeout)) {
          monitor.wait(readTimeout);
        }
        if (readTimeout != 0 && System.currentTimeMillis() - monitorStartTime >= readTimeout) {
          throw new GnsException("Timeout waiting for response packet for " + id);
        }
      }
    } catch (InterruptedException x) {
      throw new GnsException("Wait for return packet was interrupted " + x);
    }
    return resultMap.remove(id);
  }

  /**
   * Checks the response from a command request for proper syntax as well as
   * converting error responses into the appropriate thrown GNS exceptions.
   *
   * @param command
   * @param response
   * @return the result of the command
   * @throws GnsException
   */
  //FIXME: With somae changes we could probably some existing version of this in the client.
  private String checkResponse(String response) throws GnsException {
    // System.out.println("response:" + response);
    if (response.startsWith(BAD_RESPONSE)) {
      String results[] = response.split(" ");
      // System.out.println("results length:" + results.length);
      if (results.length < 2) {
        throw new GnsException("Invalid bad response indicator: " + response);
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

        if (error.startsWith(BAD_SIGNATURE)) {
          throw new EncryptionException();
        }
        if (error.startsWith(BAD_GUID) || error.startsWith(BAD_ACCESSOR_GUID)
                || error.startsWith(DUPLICATE_GUID) || error.startsWith(BAD_ACCOUNT)) {
          throw new GnsInvalidGuidException(error + rest);
        }
        if (error.startsWith(DUPLICATE_FIELD)) {
          throw new GnsInvalidFieldException(error + rest);
        }
        if (error.startsWith(BAD_FIELD) || error.startsWith(FIELD_NOT_FOUND)) {
          throw new GnsFieldNotFoundException(error + rest);
        }
        if (error.startsWith(BAD_USER) || error.startsWith(DUPLICATE_USER)) {
          throw new GnsInvalidUserException(error + rest);
        }
        if (error.startsWith(BAD_GROUP) || error.startsWith(DUPLICATE_GROUP)) {
          throw new GnsInvalidGroupException(error + rest);
        }

        if (error.startsWith(ACCESS_DENIED)) {
          throw new GnsACLException(error + rest);
        }

        if (error.startsWith(DUPLICATE_NAME)) {
          throw new GnsDuplicateNameException(error + rest);
        }

        if (error.startsWith(VERIFICATION_ERROR)) {
          throw new GnsVerificationException(error + rest);
        }
        throw new GnsException("General command failure: " + error + rest);
      }
    }
    if (response.startsWith(NULL_RESPONSE)) {
      return null;
    } else {
      return response;
    }
  }

}
