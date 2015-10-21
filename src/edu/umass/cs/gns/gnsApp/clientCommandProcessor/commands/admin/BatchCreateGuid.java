/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.admin;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.BatchTests;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class BatchCreateGuid extends GnsCommand {

  /**
   *
   * @param module
   */
  public BatchCreateGuid(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, PUBLIC_KEY, GUIDCNT};
  }

  @Override
  public String getCommandName() {
    return BATCH_TEST;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    if (module.isAdminMode()) {
      String accountName = json.optString(NAME, BatchTests.DEFAULT_ACCOUNTNAME);
      String publicKey = json.optString(PUBLIC_KEY, BatchTests.DEFAULT_PUBLICKEY);
      String guidCntString = json.getString(GUIDCNT);
      int guidCnt = Integer.parseInt(guidCntString);
      BatchTests.runBatchTest(accountName, publicKey, guidCnt, handler);
      return new CommandResponse<>(OKRESPONSE);
    } else {
      return new CommandResponse<>(BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName());
    }

  }

  @Override
  public String getCommandDescription() {
    return "Creates N guids using batch create for the supplied account. The public key is used"
            + "if we need to create the account.";
  }
}
