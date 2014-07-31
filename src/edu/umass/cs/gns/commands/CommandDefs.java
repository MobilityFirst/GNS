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
  
  // If true this will enable a new feature that handles certain command
  // requests at the name server. It is not finished and maybe never will be
  // now. Don't set this to true without talking to Westy first!!
  public static boolean handleAcccountCommandsAtNameServer = false;

  private static String[] commands = new String[]{
    "edu.umass.cs.gns.commands.data.Append",
    "edu.umass.cs.gns.commands.data.AppendList",
    "edu.umass.cs.gns.commands.data.AppendListSelf",
    "edu.umass.cs.gns.commands.data.AppendListUnsigned",
    "edu.umass.cs.gns.commands.data.AppendListWithDuplication",
    "edu.umass.cs.gns.commands.data.AppendListWithDuplicationSelf",
    "edu.umass.cs.gns.commands.data.AppendListWithDuplicationUnsigned",
    "edu.umass.cs.gns.commands.data.AppendOrCreate",
    "edu.umass.cs.gns.commands.data.AppendOrCreateList",
    "edu.umass.cs.gns.commands.data.AppendOrCreateListSelf",
    "edu.umass.cs.gns.commands.data.AppendOrCreateListUnsigned",
    "edu.umass.cs.gns.commands.data.AppendSelf",
    "edu.umass.cs.gns.commands.data.AppendUnsigned",
    "edu.umass.cs.gns.commands.data.AppendWithDuplication",
    "edu.umass.cs.gns.commands.data.AppendWithDuplicationSelf",
    "edu.umass.cs.gns.commands.data.AppendWithDuplicationUnsigned",
    "edu.umass.cs.gns.commands.data.Clear",
    "edu.umass.cs.gns.commands.data.ClearSelf",
    "edu.umass.cs.gns.commands.data.ClearUnsigned",
    "edu.umass.cs.gns.commands.data.Create",
    "edu.umass.cs.gns.commands.data.CreateEmpty",
    "edu.umass.cs.gns.commands.data.CreateEmptySelf",
    "edu.umass.cs.gns.commands.data.CreateList",
    "edu.umass.cs.gns.commands.data.CreateListSelf",
    "edu.umass.cs.gns.commands.data.CreateSelf",
    "edu.umass.cs.gns.commands.data.Help",
    "edu.umass.cs.gns.commands.data.NewRead",
    "edu.umass.cs.gns.commands.data.ReadArray",
    "edu.umass.cs.gns.commands.data.ReadArrayOne",
    "edu.umass.cs.gns.commands.data.ReadArrayOneSelf",
    "edu.umass.cs.gns.commands.data.ReadArrayOneUnsigned",
    "edu.umass.cs.gns.commands.data.ReadArraySelf",
    "edu.umass.cs.gns.commands.data.ReadArrayUnsigned",
    "edu.umass.cs.gns.commands.data.Remove",
    "edu.umass.cs.gns.commands.data.RemoveField",
    "edu.umass.cs.gns.commands.data.RemoveFieldSelf",
    "edu.umass.cs.gns.commands.data.RemoveFieldUnsigned",
    "edu.umass.cs.gns.commands.data.RemoveList",
    "edu.umass.cs.gns.commands.data.RemoveListSelf",
    "edu.umass.cs.gns.commands.data.RemoveListUnsigned",
    "edu.umass.cs.gns.commands.data.RemoveSelf",
    "edu.umass.cs.gns.commands.data.RemoveUnsigned",
    "edu.umass.cs.gns.commands.data.Replace",
    "edu.umass.cs.gns.commands.data.ReplaceList",
    "edu.umass.cs.gns.commands.data.ReplaceListSelf",
    "edu.umass.cs.gns.commands.data.ReplaceListUnsigned",
    "edu.umass.cs.gns.commands.data.ReplaceOrCreate",
    "edu.umass.cs.gns.commands.data.ReplaceOrCreateList",
    "edu.umass.cs.gns.commands.data.ReplaceOrCreateListSelf",
    "edu.umass.cs.gns.commands.data.ReplaceOrCreateListUnsigned",
    "edu.umass.cs.gns.commands.data.ReplaceOrCreateSelf",
    "edu.umass.cs.gns.commands.data.ReplaceOrCreateUnsigned",
    "edu.umass.cs.gns.commands.data.ReplaceSelf",
    "edu.umass.cs.gns.commands.data.ReplaceUnsigned",
    "edu.umass.cs.gns.commands.data.ReplaceUserJSON",
    "edu.umass.cs.gns.commands.data.Select",
    "edu.umass.cs.gns.commands.data.SelectNear",
    "edu.umass.cs.gns.commands.data.SelectWithin",
    "edu.umass.cs.gns.commands.data.SelectQuery",
    "edu.umass.cs.gns.commands.data.SelectGroupLookupQuery",
    "edu.umass.cs.gns.commands.data.SelectGroupSetupQuery",
    "edu.umass.cs.gns.commands.data.Substitute",
    "edu.umass.cs.gns.commands.data.SubstituteList",
    "edu.umass.cs.gns.commands.data.SubstituteListSelf",
    "edu.umass.cs.gns.commands.data.SubstituteListUnsigned",
    "edu.umass.cs.gns.commands.data.SubstituteSelf",
    "edu.umass.cs.gns.commands.data.SubstituteUnsigned",
    "edu.umass.cs.gns.commands.data.Set",
    "edu.umass.cs.gns.commands.data.SetSelf",
    "edu.umass.cs.gns.commands.data.SetFieldNull",
    "edu.umass.cs.gns.commands.data.SetFieldNullSelf",
    // Account
    "edu.umass.cs.gns.commands.account.AddAlias",
    "edu.umass.cs.gns.commands.account.AddGuid",
    "edu.umass.cs.gns.commands.account.LookupAccountRecord",
    "edu.umass.cs.gns.commands.account.LookupGuid",
    "edu.umass.cs.gns.commands.account.LookupPrimaryGuid",
    "edu.umass.cs.gns.commands.account.LookupGuidRecord",
    "edu.umass.cs.gns.commands.account.RegisterAccount",
    "edu.umass.cs.gns.commands.account.RegisterAccountWithoutGuid",
    "edu.umass.cs.gns.commands.account.RegisterAccountWithoutGuidOrPassword",
    "edu.umass.cs.gns.commands.account.RegisterAccountWithoutPassword",
    "edu.umass.cs.gns.commands.account.RemoveAccount",
    "edu.umass.cs.gns.commands.account.RemoveAlias",
    "edu.umass.cs.gns.commands.account.RemoveGuid",
    "edu.umass.cs.gns.commands.account.RemoveGuidNoAccount",
    "edu.umass.cs.gns.commands.account.RetrieveAliases",
    "edu.umass.cs.gns.commands.account.SetPassword",
    "edu.umass.cs.gns.commands.account.VerifyAccount",
    // ACL
    "edu.umass.cs.gns.commands.acl.AclAdd",
    "edu.umass.cs.gns.commands.acl.AclAddSelf",
    "edu.umass.cs.gns.commands.acl.AclRemove",
    "edu.umass.cs.gns.commands.acl.AclRemoveSelf",
    "edu.umass.cs.gns.commands.acl.AclRetrieve",
    "edu.umass.cs.gns.commands.acl.AclRetrieveSelf",
    // Group
    "edu.umass.cs.gns.commands.group.AddMembersToGroup",
    "edu.umass.cs.gns.commands.group.AddMembersToGroupSelf",
    "edu.umass.cs.gns.commands.group.AddToGroup",
    "edu.umass.cs.gns.commands.group.AddToGroupSelf",
    "edu.umass.cs.gns.commands.group.GetGroupMembers",
    "edu.umass.cs.gns.commands.group.GetGroupMembersSelf",
    "edu.umass.cs.gns.commands.group.GetGroups",
    "edu.umass.cs.gns.commands.group.GetGroupsSelf",
    "edu.umass.cs.gns.commands.group.RemoveFromGroup",
    "edu.umass.cs.gns.commands.group.RemoveFromGroupSelf",
    "edu.umass.cs.gns.commands.group.RemoveMembersFromGroup",
    "edu.umass.cs.gns.commands.group.RemoveMembersFromGroupSelf",
//    "edu.umass.cs.gns.commands.group.GrantMembership",
//    "edu.umass.cs.gns.commands.group.GrantMemberships",
//    "edu.umass.cs.gns.commands.group.RequestJoinGroup",
//    "edu.umass.cs.gns.commands.group.RequestLeaveGroup",
//    "edu.umass.cs.gns.commands.group.RetrieveGroupJoinRequests",
//    "edu.umass.cs.gns.commands.group.RetrieveGroupLeaveRequests",
//    "edu.umass.cs.gns.commands.group.RevokeMembership",
//    "edu.umass.cs.gns.commands.group.RevokeMemberships",
    //Admin
    "edu.umass.cs.gns.commands.admin.Admin",
    "edu.umass.cs.gns.commands.admin.GetParameter",
    "edu.umass.cs.gns.commands.admin.SetParameter",
    "edu.umass.cs.gns.commands.admin.ListParameters",
    "edu.umass.cs.gns.commands.admin.DeleteAllRecords",
    "edu.umass.cs.gns.commands.admin.ResetDatabase",
    "edu.umass.cs.gns.commands.admin.ClearCache",
    "edu.umass.cs.gns.commands.admin.DumpCache",
    "edu.umass.cs.gns.commands.admin.ChangeLogLevel",
    "edu.umass.cs.gns.commands.admin.AddTag",
    "edu.umass.cs.gns.commands.admin.RemoveTag",
    "edu.umass.cs.gns.commands.admin.ClearTagged",
    "edu.umass.cs.gns.commands.admin.GetTagged",
    "edu.umass.cs.gns.commands.admin.Dump",
    "edu.umass.cs.gns.commands.admin.PingTable",
    "edu.umass.cs.gns.commands.admin.PingValue",
    "edu.umass.cs.gns.commands.admin.RTT",
    "edu.umass.cs.gns.commands.admin.RTTQuick"
  };

  public static String[] getCommandDefs() {
    return commands;
  }
}
