/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands;

import edu.umass.cs.gns.commands.*;

/**
 *
 * @author westy
 */
@Deprecated
public class NSCommandDefs {

  private static String[] commands = new String[]{
    "edu.umass.cs.gns.nsdesign.commands.account.AddAlias",
    "edu.umass.cs.gns.nsdesign.commands.account.AddGuid",
    "edu.umass.cs.gns.nsdesign.commands.account.LookupAccountRecord",
    "edu.umass.cs.gns.nsdesign.commands.account.LookupGuid",
    "edu.umass.cs.gns.nsdesign.commands.account.LookupPrimaryGuid",
    "edu.umass.cs.gns.nsdesign.commands.account.LookupGuidRecord",
    "edu.umass.cs.gns.nsdesign.commands.account.RegisterAccount",
    "edu.umass.cs.gns.nsdesign.commands.account.RegisterAccountWithoutGuid",
    "edu.umass.cs.gns.nsdesign.commands.account.RegisterAccountWithoutGuidOrPassword",
    "edu.umass.cs.gns.nsdesign.commands.account.RegisterAccountWithoutPassword",
    "edu.umass.cs.gns.nsdesign.commands.account.RemoveAccount",
    "edu.umass.cs.gns.nsdesign.commands.account.RemoveAlias",
    "edu.umass.cs.gns.nsdesign.commands.account.RemoveGuid",
    "edu.umass.cs.gns.nsdesign.commands.account.RemoveGuidNoAccount",
    "edu.umass.cs.gns.nsdesign.commands.account.RetrieveAliases",
    "edu.umass.cs.gns.nsdesign.commands.account.SetPassword",
    "edu.umass.cs.gns.nsdesign.commands.account.VerifyAccount",
  };

  public static String[] getCommandDefs() {
    return commands;
  }
}
