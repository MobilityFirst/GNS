/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */

package edu.umass.cs.gns.nsdesign.commands;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.nsdesign.packet.LNSToNSCommandPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import java.net.InetSocketAddress;

/**
 *
 * @author westy
 */
@Deprecated
public class CommandProcessor {
  
  private static final NSCommandModule commandModule = new NSCommandModule();
  
  public static void processCommandPacket(LNSToNSCommandPacket packet, GnsReconfigurableInterface gnsReconfigurable) throws IOException, JSONException{
    String returnValue = processCommand(packet.getCommand(), gnsReconfigurable, packet.getLnsAddress());
    packet.setReturnValue(returnValue);
     GNS.getLogger().info("NS" + gnsReconfigurable.getNodeID() + " sending back to LNS " + packet.getLnsAddress()
             + " command result: " + returnValue );
    gnsReconfigurable.getNioServer().sendToAddress(packet.getLnsAddress(), packet.toJSONObject());
  }
  
  public static String processCommand(JSONObject json, GnsReconfigurableInterface gnsReconfigurable, InetSocketAddress lnsAddress) {
    // not sure if this is the best way to do this
    commandModule.setHost(gnsReconfigurable.getGNSNodeConfig().getNodeAddress(gnsReconfigurable.getNodeID()).getHostName());
    // Now we execute the command
    NSCommand command = commandModule.lookupCommand(json);
    try {
      if (command != null) {
        GNS.getLogger().info("NS" + gnsReconfigurable.getNodeID() + " executing command: " + command.toString());
        return command.execute(json, gnsReconfigurable, lnsAddress);
      } else {
        return BADRESPONSE + " " + OPERATIONNOTSUPPORTED;
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
    } catch (FailedDBOperationException e) {
      return BADRESPONSE + " " + QUERYPROCESSINGERROR + " " + e;
    }
  }
  
}
