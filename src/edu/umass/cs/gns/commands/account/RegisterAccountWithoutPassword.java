/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.account;

import edu.umass.cs.gns.commands.CommandModule;
import static edu.umass.cs.gns.clientprotocol.Defs.*;

/**
 *
 * @author westy
 */
public class RegisterAccountWithoutPassword extends RegisterAccount {

  public RegisterAccountWithoutPassword(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, GUID, PUBLICKEY};
  }

  @Override
  public String getCommandName() {
    return REGISTERACCOUNT;
  }


  @Override
  public String getCommandDescription() {
    return "Creates a GUID associated with the the human readable name "
            + "(a human readable name) and the supplied publickey. Returns a guid.";

  }
}
