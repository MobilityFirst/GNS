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
public class CommandDefs {

	/* FIXME: arun: Don't use strings, use class.getCanonicalName. It's an
	 * unnecessary pain to refactor otherwise. */
	private static String[] commands = new String[] {
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Append.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendList.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplication.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplicationSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplicationUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreate.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateList.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateListSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateListUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplication.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplicationSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplicationUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Clear.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ClearSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ClearUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Create.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateEmpty.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateEmptySelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateList.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateListSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Help.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Read.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadMultiField.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArray.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOne.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOneSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOneUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArraySelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Remove.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveField.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveFieldSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveFieldUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveList.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveListSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveListUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Replace.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceList.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceListSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceListUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreate.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateList.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateListSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateListUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUserJSON.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUserJSONUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateIndex.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Substitute.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteList.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteListSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteListUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Set.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SetSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SetFieldNull.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SetFieldNullSelf.class
					.getCanonicalName(),
			// Select
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.Select.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupLookupQuery.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQuery.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuid.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuidAndInterval.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithInterval.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectNear.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectWithin.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectQuery.class
					.getCanonicalName(),
			// Account
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddAlias.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddGuid.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuids.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuidsFast.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuidsFastRandom.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupAccountRecord.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupRandomGuids.class
					.getCanonicalName(), // for testing
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupGuid.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupPrimaryGuid.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupGuidRecord.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RegisterAccount.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RegisterAccountUnsigned.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAccount.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAlias.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveGuid.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveGuidNoAccount.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RetrieveAliases.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.SetPassword.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.VerifyAccount.class
					.getCanonicalName(),
			// ACL
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclAdd.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclAddSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRemove.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRemoveSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRetrieve.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRetrieveSelf.class
					.getCanonicalName(),
			// Group
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddMembersToGroup.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddMembersToGroupSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddToGroup.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddToGroupSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupMembers.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupMembersSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroups.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupsSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveFromGroup.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveFromGroupSelf.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveMembersFromGroup.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveMembersFromGroupSelf.class
					.getCanonicalName(),
			// edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GrantMembership.class.getCanonicalName(),
			// edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GrantMemberships.class.getCanonicalName(),
			// edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RequestJoinGroup.class.getCanonicalName(),
			// edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RequestLeaveGroup.class.getCanonicalName(),
			// edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RetrieveGroupJoinRequests.class.getCanonicalName(),
			// edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RetrieveGroupLeaveRequests.class.getCanonicalName(),
			// edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RevokeMembership.class.getCanonicalName(),
			// edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RevokeMemberships.class.getCanonicalName(),
			// Admin
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Admin.class
					.getCanonicalName(),
			// edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.BatchCreateGuid.class.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.GetParameter.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.SetParameter.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ListParameters.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.DeleteAllRecords.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ResetDatabase.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ClearCache.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.DumpCache.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ChangeLogLevel.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.AddTag.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.RemoveTag.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ClearTagged.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.GetTagged.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Dump.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.PingTable.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.PingValue.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ConnectionCheck.class
					.getCanonicalName(),
			// Active code
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.Set.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.Clear.class
					.getCanonicalName(),
			edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.Get.class
					.getCanonicalName()

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
