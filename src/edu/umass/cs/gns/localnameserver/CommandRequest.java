/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.clientsupport.Defs.*;
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

  public static void handlePacketCommandRequest(JSONObject incomingJSON, final ClientRequestHandlerInterface handler)
          throws JSONException, UnknownHostException {
    GNS.getLogger().info("######## COMMAND PACKET RECEIVED: " + incomingJSON);
    final CommandPacket packet = new CommandPacket(incomingJSON);
    final JSONObject jsonFormattedCommand = packet.getCommand();
    addMessageWithoutSignatureToCommand(jsonFormattedCommand);
    final GnsCommand command = commandModule.lookupCommand(jsonFormattedCommand);
    // This makes it work better which is weird because I thought we were in a separate worker thread from 
    // the NIO message handling thread
    (new Thread() {
      public void run() {
        try {
          String returnValue = executeCommand(command, jsonFormattedCommand);
          CommandValueReturnPacket returnPacket = new CommandValueReturnPacket(packet.getRequestId(), returnValue);
          GNS.getLogger().info("######## SENDING VALUE BACK TO " + packet.getSenderAddress() + "/" + GNS.CLIENTPORT + ": " + returnPacket.toString());
          handler.sendToAddress(returnPacket.toJSONObject(), packet.getSenderAddress(), GNS.CLIENTPORT);
        } catch (JSONException e) {
          GNS.getLogger().severe("Problem  executing command: " + e);
          e.printStackTrace();
        }
      }
    }).start();
  }

  // this little dance is because we need to remove the signature to get the message that was signed
  // alternatively we could have the client do it but that just means a longer message
  // OR we could put the signature outside the command in the packet, but some packets don't need a signature
  private static void addMessageWithoutSignatureToCommand(JSONObject command) throws JSONException {
    if (command.has(SIGNATURE)) {
      String signature = command.getString(SIGNATURE);
      command.remove(SIGNATURE);
      String commandSansSignature = JSONUtils.getCanonicalJSONString(command);
      GNS.getLogger().info("######## WITHOUT SIGNATURE: " + commandSansSignature);
      command.put(SIGNATURE, signature);
      command.put(SIGNATUREFULLMESSAGE, commandSansSignature);
    }
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
