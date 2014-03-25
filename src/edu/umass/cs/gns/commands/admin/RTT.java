/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.PerformanceTests;
import edu.umass.cs.gns.commands.data.CommandModule;
import edu.umass.cs.gns.commands.data.GnsCommand;
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
  public String execute(JSONObject json) throws JSONException {
    if (module.isAdminMode()) {
      String sizeString = json.getString(N);
      int size = Integer.parseInt(sizeString);
      String guidCntString = json.getString(GUIDCNT);
      int guidCnt = Integer.parseInt(guidCntString);
      return PerformanceTests.runRttPerformanceTest(size, guidCnt);
    } else {
      return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName();
    }

  }

  @Override
  public String getCommandDescription() {
    return "Runs the round trip test.";
  }
}
