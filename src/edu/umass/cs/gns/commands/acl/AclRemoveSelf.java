/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.acl;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;

/**
 *
 * @author westy
 */
public class AclRemoveSelf extends AclRemove {

  public AclRemoveSelf(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, ACCESSER, ACLTYPE, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ACLREMOVE;
  }

  @Override
  public String getCommandDescription() {
    return " Updates the access control list of the given GUID's field to remove the accesser guid."
            + "See below for description of ACL type and signature.";

  }
}
