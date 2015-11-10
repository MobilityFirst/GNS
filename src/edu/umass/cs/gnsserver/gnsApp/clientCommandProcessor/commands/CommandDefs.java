/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands;

/**
 * All the command classes supported by the GNS server are listed here.
 *
 * @author westy
 */
public class CommandDefs {

  private static String[] commands = new String[]{
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.Append",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendList",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendListSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendListUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendListWithDuplication",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendListWithDuplicationSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendListWithDuplicationUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendOrCreate",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendOrCreateList",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendOrCreateListSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendOrCreateListUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendWithDuplication",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendWithDuplicationSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.AppendWithDuplicationUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.Clear",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ClearSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ClearUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.Create",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.CreateEmpty",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.CreateEmptySelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.CreateList",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.CreateListSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.CreateSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.Help",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.Read",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReadMultiField",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReadArray",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReadArrayOne",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReadArrayOneSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReadArrayOneUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReadArraySelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReadArrayUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.Remove",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.RemoveField",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.RemoveFieldSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.RemoveFieldUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.RemoveList",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.RemoveListSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.RemoveListUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.RemoveSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.RemoveUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.Replace",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceList",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceListSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceListUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceOrCreate",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceOrCreateList",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceOrCreateListSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceOrCreateListUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceOrCreateSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceOrCreateUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.ReplaceUserJSON",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.Select",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SelectGroupLookupQuery",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SelectGroupSetupQuery",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SelectGroupSetupQueryWithGuid",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SelectGroupSetupQueryWithGuidAndInterval",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SelectGroupSetupQueryWithInterval",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SelectNear",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SelectWithin",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SelectQuery",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.Substitute",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SubstituteList",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SubstituteListSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SubstituteListUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SubstituteSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SubstituteUnsigned",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.Set",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SetSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SetFieldNull",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data.SetFieldNullSelf",
    // Account
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.AddAlias",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.AddGuid",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.AddMultipleGuids",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.LookupAccountRecord",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.LookupRandomGuids", //for testing 
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.LookupGuid",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.LookupPrimaryGuid",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.LookupGuidRecord",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.RegisterAccount",
    //"edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.RegisterAccountWithoutGuid",
    //"edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.RegisterAccountWithoutGuidOrPassword",
    //"edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.RegisterAccountWithoutPassword",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.RemoveAccount",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.RemoveAlias",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.RemoveGuid",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.RemoveGuidNoAccount",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.RetrieveAliases",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.SetPassword",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account.VerifyAccount",
    // ACL
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.acl.AclAdd",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.acl.AclAddSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.acl.AclRemove",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.acl.AclRemoveSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.acl.AclRetrieve",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.acl.AclRetrieveSelf",
    // Group
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.AddMembersToGroup",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.AddMembersToGroupSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.AddToGroup",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.AddToGroupSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.GetGroupMembers",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.GetGroupMembersSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.GetGroups",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.GetGroupsSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.RemoveFromGroup",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.RemoveFromGroupSelf",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.RemoveMembersFromGroup",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.RemoveMembersFromGroupSelf",
    //    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.GrantMembership",
    //    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.GrantMemberships",
    //    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.RequestJoinGroup",
    //    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.RequestLeaveGroup",
    //    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.RetrieveGroupJoinRequests",
    //    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.RetrieveGroupLeaveRequests",
    //    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.RevokeMembership",
    //    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.group.RevokeMemberships",
    //Admin
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.Admin",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.BatchCreateGuidSimple",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.BatchCreateGuidName",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.BatchCreateGuid",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.GetParameter",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.SetParameter",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.ListParameters",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.DeleteAllRecords",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.ResetDatabase",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.ClearCache",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.DumpCache",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.ChangeLogLevel",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.AddTag",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.RemoveTag",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.ClearTagged",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.GetTagged",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.Dump",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.PingTable",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.PingValue",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.RTT",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.RTTQuick",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin.ConnectionCheck",
    // Active code
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.activecode.Set",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.activecode.Clear",
    "edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.activecode.Get"

  };

  /**
   * Return all the command definitions.
   * 
   * @return an array of strings
   */
  public static String[] getCommandDefs() {
    return commands;
  }
}
