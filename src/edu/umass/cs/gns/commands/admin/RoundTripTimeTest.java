/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.clientprotocol.PerformanceTests;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class RoundTripTimeTest extends GnsCommand {

  public RoundTripTimeTest(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{N};
  }

  @Override
  public String getCommandName() {
    return RTTTEST;
  }

  @Override
  public String execute(JSONObject json) throws JSONException {
    String sizeString = json.getString(N);
    int size = Integer.parseInt(sizeString);
    return PerformanceTests.runRttPerformanceTest(size);
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
