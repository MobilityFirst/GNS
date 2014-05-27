/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import static edu.umass.cs.gns.clientsupport.Defs.BADRESPONSE;
import static edu.umass.cs.gns.clientsupport.Defs.JSONPARSEERROR;
import static edu.umass.cs.gns.clientsupport.Defs.OPERATIONNOTSUPPORTED;
import static edu.umass.cs.gns.clientsupport.Defs.QUERYPROCESSINGERROR;
import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.httpserver.Defs.QUERYPREFIX;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.CommandPacket;
import edu.umass.cs.gns.nsdesign.packet.CommandValueReturnPacket;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

/**
 * Handles sending and receiving of commands.
 *
 * @author westy
 */
public class CommandRequest {

  // handles command processing
  private static final CommandModule commandModule = new CommandModule();

  public static void handlePacketCommandRequest(JSONObject incomingJSON, ClientRequestHandlerInterface handler)
          throws JSONException, UnknownHostException {

    CommandPacket packet = new CommandPacket(incomingJSON);
    JSONObject jsonFormattedCommand = packet.getCommand();
    GnsCommand command = commandModule.lookupCommand(jsonFormattedCommand);
    String returnValue = executeCommand(command, jsonFormattedCommand);
    CommandValueReturnPacket returnPacket = new CommandValueReturnPacket(packet.getRequestId(), returnValue);
    handler.sendToAddress(returnPacket.toJSONObject(), packet.getSenderAddress(), GNS.CLIENTPORT);
  }

  public static String executeCommand(GnsCommand command, JSONObject json) {
    try {
      if (command != null) {
        //GNS.getLogger().info("Executing command: " + command.toString());
        GNS.getLogger().info("Executing command: " + command.toString() + " with " + json);
        return command.execute(json);
      } else {
        return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " - Don't understand " + json.toString();
      }
    } catch (JSONException e) {
      e.printStackTrace();
      return BADRESPONSE + " " + JSONPARSEERROR + " " + e;
    } catch (NoSuchAlgorithmException e) {
      return BADRESPONSE + " " + QUERYPROCESSINGERROR + " " + e;
    } catch (InvalidKeySpecException e) {
      return BADRESPONSE + " " + QUERYPROCESSINGERROR + " " + e;
    } catch (SignatureException e) {
      return BADRESPONSE + " " + QUERYPROCESSINGERROR + " " + e;
    } catch (InvalidKeyException e) {
      return BADRESPONSE + " " + QUERYPROCESSINGERROR + " " + e;
    }
  }

}
