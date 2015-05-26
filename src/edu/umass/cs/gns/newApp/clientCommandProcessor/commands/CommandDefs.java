/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commands;

/**
 *
 * @author westy
 */
public class CommandDefs {
  
  // If true this will enable a new feature that handles certain command
  // requests at the name server. It is not finished and maybe never will be
  // now. Don't set this to true without talking to Westy first!!
  public static boolean handleAcccountCommandsAtNameServer = false;

  private static String[] commands = new String[]{
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.Append",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendList",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendListSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendListUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendListWithDuplication",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendListWithDuplicationSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendListWithDuplicationUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendOrCreate",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendOrCreateList",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendOrCreateListSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendOrCreateListUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendWithDuplication",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendWithDuplicationSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.AppendWithDuplicationUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.Clear",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ClearSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ClearUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.Create",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.CreateEmpty",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.CreateEmptySelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.CreateList",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.CreateListSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.CreateSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.Help",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.Read",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReadMultiField",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReadArray",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReadArrayOne",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReadArrayOneSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReadArrayOneUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReadArraySelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReadArrayUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.Remove",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.RemoveField",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.RemoveFieldSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.RemoveFieldUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.RemoveList",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.RemoveListSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.RemoveListUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.RemoveSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.RemoveUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.Replace",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceList",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceListSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceListUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceOrCreate",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceOrCreateList",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceOrCreateListSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceOrCreateListUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceOrCreateSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceOrCreateUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.ReplaceUserJSON",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.Select",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SelectGroupLookupQuery",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SelectGroupSetupQuery",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SelectGroupSetupQueryWithGuid",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SelectGroupSetupQueryWithGuidAndInterval",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SelectGroupSetupQueryWithInterval",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SelectNear",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SelectWithin",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SelectQuery",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.Substitute",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SubstituteList",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SubstituteListSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SubstituteListUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SubstituteSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SubstituteUnsigned",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.Set",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SetSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SetFieldNull",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data.SetFieldNullSelf",
    // Account
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.AddAlias",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.AddGuid",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.LookupAccountRecord",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.LookupGuid",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.LookupPrimaryGuid",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.LookupGuidRecord",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.RegisterAccount",
    //"edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.RegisterAccountWithoutGuid",
    //"edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.RegisterAccountWithoutGuidOrPassword",
    //"edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.RegisterAccountWithoutPassword",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.RemoveAccount",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.RemoveAlias",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.RemoveGuid",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.RemoveGuidNoAccount",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.RetrieveAliases",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.SetPassword",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account.VerifyAccount",
    // ACL
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.acl.AclAdd",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.acl.AclAddSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.acl.AclRemove",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.acl.AclRemoveSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.acl.AclRetrieve",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.acl.AclRetrieveSelf",
    // Group
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.AddMembersToGroup",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.AddMembersToGroupSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.AddToGroup",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.AddToGroupSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.GetGroupMembers",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.GetGroupMembersSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.GetGroups",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.GetGroupsSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.RemoveFromGroup",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.RemoveFromGroupSelf",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.RemoveMembersFromGroup",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.RemoveMembersFromGroupSelf",
//    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.GrantMembership",
//    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.GrantMemberships",
//    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.RequestJoinGroup",
//    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.RequestLeaveGroup",
//    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.RetrieveGroupJoinRequests",
//    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.RetrieveGroupLeaveRequests",
//    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.RevokeMembership",
//    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.group.RevokeMemberships",
    //Admin
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.Admin",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.GetParameter",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.SetParameter",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.ListParameters",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.DeleteAllRecords",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.ResetDatabase",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.ClearCache",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.DumpCache",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.ChangeLogLevel",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.AddTag",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.RemoveTag",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.ClearTagged",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.GetTagged",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.Dump",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.PingTable",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.PingValue",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.RTT",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.RTTQuick",
    "edu.umass.cs.gns.newApp.clientCommandProcessor.commands.admin.ConnectionCheck"
          
  };

  public static String[] getCommandDefs() {
    return commands;
  }
}
