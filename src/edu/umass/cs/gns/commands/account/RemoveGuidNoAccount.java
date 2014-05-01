/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.account;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;

/**
 *
 * @author westy
 */
public class RemoveGuidNoAccount extends RemoveGuid {

  public RemoveGuidNoAccount(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return REMOVEGUID;
  }

  @Override
  public String getCommandDescription() {
    return "Removes the GUID. "
            + "Must be signed by the guid. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";

  }
}
