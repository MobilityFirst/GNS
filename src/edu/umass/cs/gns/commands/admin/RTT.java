/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.PerformanceTests;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class RTT extends GnsCommand {

  public RTT(CommandModule module) {
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
    return "Runs the round trip test.";
  }
}
