/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;

/**
 *
 * @author westy
 */
public class ReplaceUserJSON extends AbstractUpdate {

  /**
   *
   * @param module
   */
  public ReplaceUserJSON(CommandModule module) {
    super(module);
  }

  /**
   * Return the update operation.
   * 
   * @return an {@link UpdateOperation}
   */
  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.USER_JSON_REPLACE;
  }

  @Override
  public String getCommandName() {
    return REPLACEUSERJSON;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, USERJSON, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Replaces existing fields in JSON record with the given JSONObject's fields. "
            + "Doesn't touch top-level fields that aren't in the given JSONObject.";
  }
}
