/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.clientsupport.CommandResponse;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.PerformanceTests;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
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
    return new String[]{N, GUIDCNT};
  }

  @Override
  public String getCommandName() {
    return RTTTEST;
  }

  @Override
  public CommandResponse execute(JSONObject json) throws JSONException {
    if (module.isAdminMode()) {
      String sizeString = json.getString(N);
      int size = Integer.parseInt(sizeString);
      String guidCntString = json.getString(GUIDCNT);
      int guidCnt = Integer.parseInt(guidCntString);
      return new CommandResponse(PerformanceTests.runRttPerformanceTest(size, guidCnt, true));
    } else {
      return new CommandResponse(BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName());
    }

  }

  @Override
  public String getCommandDescription() {
    return "Runs the round trip test.";
  }
}
