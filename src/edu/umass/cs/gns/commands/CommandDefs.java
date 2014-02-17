/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands;

/**
 *
 * @author westy
 */
public class CommandDefs {

  private static String[] commands = new String[]{
    "edu.umass.cs.gns.commands.AddAlias",
    "edu.umass.cs.gns.commands.AddGuid",
    "edu.umass.cs.gns.commands.Append",
    "edu.umass.cs.gns.commands.AppendList",
    "edu.umass.cs.gns.commands.AppendListSelf",
    "edu.umass.cs.gns.commands.AppendListUnsigned",
    "edu.umass.cs.gns.commands.AppendListWithDuplication",
    "edu.umass.cs.gns.commands.AppendListWithDuplicationSelf",
    "edu.umass.cs.gns.commands.AppendListWithDuplicationUnsigned",
    "edu.umass.cs.gns.commands.AppendOrCreate",
    "edu.umass.cs.gns.commands.AppendOrCreateList",
    "edu.umass.cs.gns.commands.AppendOrCreateListSelf",
    "edu.umass.cs.gns.commands.AppendOrCreateListUnsigned",
    "edu.umass.cs.gns.commands.AppendSelf",
    "edu.umass.cs.gns.commands.AppendUnsigned",
    "edu.umass.cs.gns.commands.AppendWithDuplication",
    "edu.umass.cs.gns.commands.AppendWithDuplicationSelf",
    "edu.umass.cs.gns.commands.AppendWithDuplicationUnsigned",
    "edu.umass.cs.gns.commands.Clear",
    "edu.umass.cs.gns.commands.ClearSelf",
    "edu.umass.cs.gns.commands.ClearUnsigned",
    "edu.umass.cs.gns.commands.Create",
    "edu.umass.cs.gns.commands.CreateEmpty",
    "edu.umass.cs.gns.commands.CreateEmptySelf",
    "edu.umass.cs.gns.commands.CreateList",
    "edu.umass.cs.gns.commands.CreateListSelf",
    "edu.umass.cs.gns.commands.CreateSelf",
    "edu.umass.cs.gns.commands.LookupAccountRecord",
    "edu.umass.cs.gns.commands.LookupGuid",
    "edu.umass.cs.gns.commands.LookupGuidRecord",
    "edu.umass.cs.gns.commands.Read",
    "edu.umass.cs.gns.commands.ReadOne",
    "edu.umass.cs.gns.commands.ReadOneSelf",
    "edu.umass.cs.gns.commands.ReadOneUnsigned",
    "edu.umass.cs.gns.commands.ReadSelf",
    "edu.umass.cs.gns.commands.ReadUnsigned",
    "edu.umass.cs.gns.commands.RegisterAccount",
    "edu.umass.cs.gns.commands.RegisterAccountWithoutGuid",
    "edu.umass.cs.gns.commands.RegisterAccountWithoutGuidOrPassword",
    "edu.umass.cs.gns.commands.RegisterAccountWithoutPassword",
    "edu.umass.cs.gns.commands.Remove",
    "edu.umass.cs.gns.commands.RemoveAccount",
    "edu.umass.cs.gns.commands.RemoveAlias",
    "edu.umass.cs.gns.commands.RemoveField",
    "edu.umass.cs.gns.commands.RemoveFieldSelf",
    "edu.umass.cs.gns.commands.RemoveFieldUnsigned",
    "edu.umass.cs.gns.commands.RemoveGuid",
    "edu.umass.cs.gns.commands.RemoveList",
    "edu.umass.cs.gns.commands.RemoveListSelf",
    "edu.umass.cs.gns.commands.RemoveListUnsigned",
    "edu.umass.cs.gns.commands.RemoveSelf",
    "edu.umass.cs.gns.commands.RemoveUnsigned",
    "edu.umass.cs.gns.commands.Replace",
    "edu.umass.cs.gns.commands.ReplaceList",
    "edu.umass.cs.gns.commands.ReplaceListSelf",
    "edu.umass.cs.gns.commands.ReplaceListUnsigned",
    "edu.umass.cs.gns.commands.ReplaceOrCreate",
    "edu.umass.cs.gns.commands.ReplaceOrCreateList",
    "edu.umass.cs.gns.commands.ReplaceOrCreateListSelf",
    "edu.umass.cs.gns.commands.ReplaceOrCreateListUnsigned",
    "edu.umass.cs.gns.commands.ReplaceOrCreateSelf",
    "edu.umass.cs.gns.commands.ReplaceOrCreateUnsigned",
    "edu.umass.cs.gns.commands.ReplaceSelf",
    "edu.umass.cs.gns.commands.ReplaceUnsigned",
    "edu.umass.cs.gns.commands.RetrieveAliases",
    "edu.umass.cs.gns.commands.Select",
    "edu.umass.cs.gns.commands.SelectNear",
    "edu.umass.cs.gns.commands.SelectQuery",
    "edu.umass.cs.gns.commands.SelectWithin",
    "edu.umass.cs.gns.commands.SetPassword",
    "edu.umass.cs.gns.commands.Substitute",
    "edu.umass.cs.gns.commands.SubstituteList",
    "edu.umass.cs.gns.commands.SubstituteListSelf",
    "edu.umass.cs.gns.commands.SubstituteListUnsigned",
    "edu.umass.cs.gns.commands.SubstituteSelf",
    "edu.umass.cs.gns.commands.SubstituteUnsigned",
    "edu.umass.cs.gns.commands.VerifyAccount"
  
  
  };

  public static String[] getCommandDefs() {
    return commands;
  }
}
