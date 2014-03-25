package edu.umass.cs.gns.clientsupport;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.httpserver.Defs.*;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.Util;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.Map.Entry;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * THIS CLASS IS A STUB THAT IS GOING AWAY SOON.
 * 
 * It has been superceded by the new command module.
 * Parts will be retained.
 * Stay tuned for more changes.
 * 
 * Implements the GNS server protocol for the HTTP Server.  
 *
 * @author westy
 */
public class Protocol {

  //new command processing
  CommandModule commandModule = new CommandModule();

  /**
   * Top level routine to process queries for the http service *
   */
  public String processQuery(String host, String action, String queryString) {
    String fullString = action + QUERYPREFIX + queryString; // for signature check
    Map<String, String> queryMap = Util.parseURIQueryString(queryString);
    // THIS DOESN'T ALLOW EMPTY STRINGS FOR ARGUMENTES TO FIELD VALUES
//    // make sure none of the arguments are empty
//    if (queryMap.values().contains("")) {
//      // find they key that has an empty value
//      String result = "";
//      for (Entry<String, String> entry : queryMap.entrySet()) {
//        if ("".equals(entry.getValue())) {
//          result = " " + entry.getKey();
//        }
//      }
//      return BADRESPONSE + " " + "Empty argument" + result;
//    }

    //new command processing
    queryMap.put(COMMANDNAME, action);
    if (queryMap.keySet().contains(SIGNATURE)) {
      String signature = queryMap.get(SIGNATURE);
      String message = AccessSupport.removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature);
      queryMap.put(SIGNATUREFULLMESSAGE, message);
    }
    commandModule.setHost(host); // not terribly happy with this
    JSONObject json = new JSONObject(queryMap);
    GnsCommand command = commandModule.lookupCommand(json);
    try {
      if (command != null) {
        GNS.getLogger().info("Executing command: " + command.toString());
        //GNS.getLogger().info("Executing command: " + command.toString() + " with " + json);
        return command.execute(json);
      } else {
        return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " - Don't understand " + action + QUERYPREFIX + queryString;
      }
    } catch (JSONException e) {
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
  public static String Version = "$Revision$";
}
