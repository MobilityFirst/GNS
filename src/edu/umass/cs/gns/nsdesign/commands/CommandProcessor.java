/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */

package edu.umass.cs.gns.nsdesign.commands;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplica;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class CommandProcessor {
  
  private static final NSCommandModule commandModule = new NSCommandModule();
  
  public static String processCommand(JSONObject json, GnsReconfigurable gnsReconfigurable) {
    // not sure if this is the best way to do this
    commandModule.setHost(gnsReconfigurable.getGNSNodeConfig().getNodeAddress(gnsReconfigurable.getNodeID()).getHostName());
    // Now we execute the command
    NSCommand command = commandModule.lookupCommand(json);
    try {
      if (command != null) {
        GNS.getLogger().info("NS" + gnsReconfigurable.getNodeID() + " executing command: " + command.toString());
        return command.execute(json, gnsReconfigurable);
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
    }
  }
  
}
