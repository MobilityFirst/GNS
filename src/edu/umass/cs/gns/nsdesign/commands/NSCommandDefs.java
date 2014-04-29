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
public class NSCommandDefs {

  private static String[] commands = new String[]{
    "edu.umass.cs.gns.nsdesign.commands.AddAlias",
    "edu.umass.cs.gns.nsdesign.commands.AddGuid",
    "edu.umass.cs.gns.nsdesign.commands.LookupAccountRecord",
    "edu.umass.cs.gns.nsdesign.commands.LookupGuid",
    "edu.umass.cs.gns.nsdesign.commands.LookupPrimaryGuid",
    "edu.umass.cs.gns.nsdesign.commands.LookupGuidRecord",
    "edu.umass.cs.gns.nsdesign.commands.RegisterAccount",
    "edu.umass.cs.gns.nsdesign.commands.RegisterAccountWithoutGuid",
    "edu.umass.cs.gns.nsdesign.commands.RegisterAccountWithoutGuidOrPassword",
    "edu.umass.cs.gns.nsdesign.commands.RegisterAccountWithoutPassword",
    "edu.umass.cs.gns.nsdesign.commands.RemoveAccount",
    "edu.umass.cs.gns.nsdesign.commands.RemoveAlias",
    "edu.umass.cs.gns.nsdesign.commands.RemoveGuid",
    "edu.umass.cs.gns.nsdesign.commands.RemoveGuidNoAccount",
    "edu.umass.cs.gns.nsdesign.commands.RetrieveAliases",
    "edu.umass.cs.gns.nsdesign.commands.SetPassword",
    "edu.umass.cs.gns.nsdesign.commands.VerifyAccount",
  };

  public static String[] getCommandDefs() {
    return commands;
  }
}
