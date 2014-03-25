/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.acl;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.data.CommandModule;

/**
 *
 * @author westy
 */
public class AclRetrieveSelf extends AclRetrieve {

  public AclRetrieveSelf(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, ACLTYPE, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ACLRETRIEVE;
  }

  @Override
  public String getCommandDescription() {
    return "Returns the access control list for a guids's field. See below for description of ACL type and signature.";

  }
}
