/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnsclient.console.GnsUtils;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.io.IOException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Command to update the entire records using JSON in the GNS
 *
 * @author Westy
 * @version 1.0
 */
public class Update extends ConsoleCommand {

  /**
   * Creates a new <code>Update</code> object
   *
   * @param module
   */
  public Update(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Update the value of the target GUID using the JSON String (using the credential of the current GUID/alias)";
  }

  @Override
  public String getCommandName() {
    return "update";
  }

  @Override
  public String getCommandParameters() {
    return "[target_guid_or_alias] jsonString";
  }

  /**
   * Override execute to check for a selected gui
   *
   * @throws java.lang.Exception
   */
  @Override
  public void execute(String commandText) throws Exception {
    if (!module.isCurrentGuidSetAndVerified()) {
      return;
    }
    super.execute(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    UniversalTcpClient gnsClient = module.getGnsClient();
    try {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      String guid;
      if (st.countTokens() == 1) {
        guid = module.getCurrentGuid().getGuid();
      } else if (st.countTokens() == 2) {
        guid = st.nextToken();
        if (!GnsUtils.isValidGuidString(guid)) {
          // We probably have an alias, lookup the GUID
          guid = gnsClient.lookupGuid(guid);
        }
      } else {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String value = st.nextToken();
      JSONObject json = new JSONObject(value);
      

      gnsClient.update(guid, json, module.getCurrentGuid());
      console.printString("GUID " + guid + " has been updated using '" + json.toString());
      console.printNewline();
    } catch (IOException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    } catch (GnsException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    } catch (JSONException e) {
      console.printString("Unable to parse JSON string:" + e + "\n");
    }
  }
}
