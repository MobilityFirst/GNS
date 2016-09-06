/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy */

package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands;

/**
 * All the command classes supported by the GNS server are listed here.
 *
 * @author arun, westy
 */
@Deprecated
public class CommandDefs {

  private static final Class<?>[] COMMANDS = new Class<?>[]{
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Append.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendList.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.AppendListSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplication.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.AppendListWithDuplicationSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplicationUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreate.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateList.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.AppendOrCreateListSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateListUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.AppendSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplication.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.AppendWithDuplicationSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplicationUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Clear.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.ClearSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ClearUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Create.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateEmpty.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.CreateEmptySelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateList.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.CreateListSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.CreateSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Help.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Read.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.ReadSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadMultiField.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArray.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOne.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.ReadArrayOneSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOneUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.ReadArraySelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Remove.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveField.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.RemoveFieldSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveFieldUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveList.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.RemoveListSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveListUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.RemoveSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Replace.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceList.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.ReplaceListSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceListUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreate.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateList.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.ReplaceOrCreateListSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateListUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.ReplaceOrCreateSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.ReplaceSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUserJSON.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUserJSONUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateIndex.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Substitute.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteList.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.SubstituteListSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteListUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.SubstituteSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Set.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.SetSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SetFieldNull.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.deprecated.SetFieldNullSelf.class,
    // Select
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.Select.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupLookupQuery.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQuery.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuid.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuidAndInterval.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithInterval.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectNear.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectWithin.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectQuery.class,
    // Account
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddAlias.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddGuid.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuids.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuidsFast.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuidsFastRandom.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupAccountRecord.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupRandomGuids.class, // for testing
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupGuid.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupPrimaryGuid.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupGuidRecord.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RegisterAccount.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RegisterAccountUnsigned.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAccount.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAlias.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveGuid.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveGuidNoAccount.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RetrieveAliases.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.SetPassword.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.VerifyAccount.class,
    // ACL
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclAdd.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclAddSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRemove.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRemoveSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRetrieve.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRetrieveSelf.class,
    // Group
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddMembersToGroup.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddMembersToGroupSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddToGroup.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddToGroupSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupMembers.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupMembersSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroups.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupsSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveFromGroup.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveFromGroupSelf.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveMembersFromGroup.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveMembersFromGroupSelf.class,
    // edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GrantMembership.class,
    // edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GrantMemberships.class,
    // edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RequestJoinGroup.class,
    // edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RequestLeaveGroup.class,
    // edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RetrieveGroupJoinRequests.class,
    // edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RetrieveGroupLeaveRequests.class,
    // edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RevokeMembership.class,
    // edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RevokeMemberships.class,
    // Admin
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Admin.class,
    // edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.BatchCreateGuid.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.GetParameter.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.SetParameter.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ListParameters.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.DeleteAllRecords.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ResetDatabase.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ClearCache.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.DumpCache.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ChangeLogLevel.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.AddTag.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.RemoveTag.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ClearTagged.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.GetTagged.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Dump.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ConnectionCheck.class,
    // Active code
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.SetCode.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.ClearCode.class,
    edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.GetCode.class

  };

  /**
   * Return all the command definitions.
   *
   * @return an array of strings
   */
  public static Class<?>[] getCommandDefs() {
    return COMMANDS;
  }
}
