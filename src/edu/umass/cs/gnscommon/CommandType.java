/* Copyright (1c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (1the "License"); you may not
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
package edu.umass.cs.gnscommon;

import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

/**
 * All the commands supported by the GNS server are listed here.
 *
 * Each one of these has a corresponding method in an array defined in
 * edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandDefs
 */
public enum CommandType {
//
  // Data Commands
  //
  Append(110, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Append.class,
          GNSCommand.ResultType.NULL, true, false),
  AppendList(111, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendList.class,
          GNSCommand.ResultType.NULL, true, false),
  //  AppendListSelf(112, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  AppendListUnsigned(113, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  AppendListWithDuplication(114, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplication.class,
          GNSCommand.ResultType.NULL, true, false),
  //  AppendListWithDuplicationSelf(115, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplicationSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  AppendListWithDuplicationUnsigned(116, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplicationUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  AppendOrCreate(120, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreate.class,
          GNSCommand.ResultType.NULL, true, false),
  AppendOrCreateList(121, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateList.class,
          GNSCommand.ResultType.NULL, true, false),
  //  AppendOrCreateListSelf(122, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateListSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  AppendOrCreateListUnsigned(123, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateListUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //  AppendOrCreateSelf(124, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  AppendOrCreateUnsigned(125, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  //  AppendSelf(130, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  AppendUnsigned(131, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  AppendWithDuplication(132, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplication.class,
          GNSCommand.ResultType.NULL, true, false),
  //  AppendWithDuplicationSelf(133, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplicationSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  AppendWithDuplicationUnsigned(134, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplicationUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  Clear(140, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Clear.class,
          GNSCommand.ResultType.NULL, true, false),
  //  ClearSelf(141, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ClearSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  ClearUnsigned(142, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ClearUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  Create(150, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Create.class,
          GNSCommand.ResultType.NULL, true, false),
  CreateEmpty(151, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateEmpty.class,
          GNSCommand.ResultType.NULL, true, false),
  //  CreateEmptySelf(152, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateEmptySelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  CreateList(153, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateList.class,
          GNSCommand.ResultType.NULL, true, false),
  //  CreateListSelf(154, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateListSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  //  CreateSelf(155, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  //
  Read(160, Type.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Read.class,
          GNSCommand.ResultType.MAP, true, false),
  //  ReadSelf(161, Type.READ,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadSelf.class,
  //          GNSCommand.ResultType.MAP, true, false),
  ReadUnsigned(162, Type.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadUnsigned.class,
          GNSCommand.ResultType.MAP, true, false),
  ReadMultiField(163, Type.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadMultiField.class,
          GNSCommand.ResultType.MAP, true, false),
  ReadMultiFieldUnsigned(164, Type.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadMultiFieldUnsigned.class,
          GNSCommand.ResultType.MAP, true, false),
  //
  ReadArray(170, Type.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArray.class,
          GNSCommand.ResultType.LIST, true, false),
  ReadArrayOne(171, Type.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOne.class,
          GNSCommand.ResultType.STRING, true, false),
  //  ReadArrayOneSelf(172, Type.READ,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOneSelf.class,
  //          GNSCommand.ResultType.STRING, true, false),
  ReadArrayOneUnsigned(173, Type.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOneUnsigned.class,
          GNSCommand.ResultType.STRING, true, false),
  //  ReadArraySelf(174, Type.READ,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArraySelf.class,
  //          GNSCommand.ResultType.LIST, true, false),
  ReadArrayUnsigned(175, Type.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayUnsigned.class,
          GNSCommand.ResultType.LIST, true, false),
  //
  Remove(180, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Remove.class,
          GNSCommand.ResultType.NULL, true, false),
  RemoveList(181, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveList.class,
          GNSCommand.ResultType.NULL, true, false),
  //  RemoveListSelf(182, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveListSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  RemoveListUnsigned(183, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveListUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //  RemoveSelf(184, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  RemoveUnsigned(185, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  Replace(190, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Replace.class,
          GNSCommand.ResultType.NULL, true, false),
  ReplaceList(191, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceList.class,
          GNSCommand.ResultType.NULL, true, false),
  //  ReplaceListSelf(192, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceListSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  ReplaceListUnsigned(193, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceListUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  ReplaceOrCreate(210, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreate.class,
          GNSCommand.ResultType.NULL, true, false),
  ReplaceOrCreateList(211, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateList.class,
          GNSCommand.ResultType.NULL, true, false),
  //  ReplaceOrCreateListSelf(212, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateListSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  ReplaceOrCreateListUnsigned(213, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateListUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //  ReplaceOrCreateSelf(214, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  ReplaceOrCreateUnsigned(215, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //  ReplaceSelf(216, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  ReplaceUnsigned(217, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  ReplaceUserJSON(220, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUserJSON.class,
          GNSCommand.ResultType.NULL, true, false),
  ReplaceUserJSONUnsigned(221, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUserJSONUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  CreateIndex(230, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateIndex.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  Substitute(231, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Substitute.class,
          GNSCommand.ResultType.NULL, true, false),
  SubstituteList(232, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteList.class,
          GNSCommand.ResultType.NULL, true, false),
  //  SubstituteListSelf(233, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteListSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  SubstituteListUnsigned(234, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteListUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //  SubstituteSelf(235, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  SubstituteUnsigned(236, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  RemoveField(240, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveField.class,
          GNSCommand.ResultType.NULL, true, false),
  //  RemoveFieldSelf(241, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveFieldSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  RemoveFieldUnsigned(242, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveFieldUnsigned.class,
          GNSCommand.ResultType.NULL, true, false),
  //
  Set(250, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Set.class,
          GNSCommand.ResultType.NULL, true, false),
  //  SetSelf(251, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SetSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  SetFieldNull(252, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SetFieldNull.class,
          GNSCommand.ResultType.NULL, true, false),
  //  SetFieldNullSelf(253, Type.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SetFieldNullSelf.class,
  //          GNSCommand.ResultType.NULL, true, false),
  //
  // Select Commands - not sure about the coordination of any of the select commands
  //
  Select(310, Type.SELECT,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.Select.class,
          GNSCommand.ResultType.LIST, false, false),
  SelectGroupLookupQuery(311, Type.SELECT,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupLookupQuery.class,
          GNSCommand.ResultType.LIST, false, false),
  SelectGroupSetupQuery(312, Type.SELECT,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQuery.class,
          GNSCommand.ResultType.LIST, false, false),
  SelectGroupSetupQueryWithGuid(313, Type.SELECT,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuid.class,
          GNSCommand.ResultType.LIST, false, false),
  SelectGroupSetupQueryWithGuidAndInterval(314, Type.SELECT,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuidAndInterval.class,
          GNSCommand.ResultType.LIST, false, false),
  SelectGroupSetupQueryWithInterval(315, Type.SELECT,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithInterval.class,
          GNSCommand.ResultType.LIST, false, false),
  //
  SelectNear(320, Type.SELECT,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectNear.class,
          GNSCommand.ResultType.LIST, false, false),
  SelectWithin(321, Type.SELECT,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectWithin.class,
          GNSCommand.ResultType.LIST, false, false),
  SelectQuery(322, Type.SELECT,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectQuery.class,
          GNSCommand.ResultType.LIST, false, false),
  //
  // Account Commands - also not sure about coordination for these
  //
  AddAlias(410, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddAlias.class,
          GNSCommand.ResultType.NULL, false, false),
  AddGuid(411, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddGuid.class,
          GNSCommand.ResultType.NULL, false, false),
  AddMultipleGuids(412, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuids.class,
          GNSCommand.ResultType.NULL, false, false),
  AddMultipleGuidsFast(413, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuidsFast.class,
          GNSCommand.ResultType.NULL, false, false),
  AddMultipleGuidsFastRandom(414, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuidsFastRandom.class,
          GNSCommand.ResultType.NULL, false, false),
  // These should all be coordinatable.
  //FIXME:This command was failing tests when set to Type.SYSTEM_LOOKUP
  LookupAccountRecord(420, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupAccountRecord.class,
          GNSCommand.ResultType.MAP, true, false),
  LookupRandomGuids(421, Type.SYSTEM_LOOKUP,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupRandomGuids.class,
          GNSCommand.ResultType.LIST, true, false), // for testing
  //FIXME:This command was failing tests when set to Type.SYSTEM_LOOKUP
  LookupGuid(422, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupGuid.class,
          GNSCommand.ResultType.STRING, true, false),
  LookupPrimaryGuid(423, Type.SYSTEM_LOOKUP,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupPrimaryGuid.class,
          GNSCommand.ResultType.STRING, true, false),
  LookupGuidRecord(424, Type.SYSTEM_LOOKUP,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupGuidRecord.class,
          GNSCommand.ResultType.MAP, true, false),
  //
  RegisterAccount(430, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RegisterAccount.class,
          GNSCommand.ResultType.NULL, false, false),
  RegisterAccountUnsigned(432, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RegisterAccountUnsigned.class,
          GNSCommand.ResultType.NULL, false, false),
  //
  RemoveAccount(440, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAccount.class,
          GNSCommand.ResultType.NULL, false, false),
  RemoveAlias(441, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAlias.class,
          GNSCommand.ResultType.NULL, false, false),
  RemoveGuid(442, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveGuid.class,
          GNSCommand.ResultType.NULL, false, false),
  RemoveGuidNoAccount(443, Type.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveGuidNoAccount.class,
          GNSCommand.ResultType.NULL, false, false),
  RetrieveAliases(444, Type.SYSTEM_LOOKUP,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RetrieveAliases.class,
          GNSCommand.ResultType.LIST, true, false),
  //
  SetPassword(450, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.SetPassword.class,
          GNSCommand.ResultType.NULL, true, false),
  VerifyAccount(451, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.VerifyAccount.class,
          GNSCommand.ResultType.STRING, true, false),
  //
  ResetKey(460, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.ResetKey.class,
          GNSCommand.ResultType.NULL, true, false),
  // ACL - these should all be safe to coordinate
  // AclAdd (and friends) does a potentially remote lookup of the guid that we are granting access
  // then does a local update of the guid which will be accessed, updating the appropriate ACL list.
  AclAdd(510, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclAdd.class,
          GNSCommand.ResultType.NULL, true, false),
  AclAddSelf(511, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclAddSelf.class,
          GNSCommand.ResultType.NULL, true, false),
  AclRemove(512, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRemove.class,
          GNSCommand.ResultType.NULL, true, false),
  AclRemoveSelf(513, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRemoveSelf.class,
          GNSCommand.ResultType.NULL, true, false),
  AclRetrieve(514, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRetrieve.class,
          GNSCommand.ResultType.LIST, true, false),
  AclRetrieveSelf(515, Type.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRetrieveSelf.class,
          GNSCommand.ResultType.LIST, true, false),
  // Given the remote query calls all the setters below make
  // it currently appears that they cannot be coordinated.
  // Group
  // Fixme: None of these are currently doing authentication.
  AddMembersToGroup(610, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddMembersToGroup.class,
          GNSCommand.ResultType.NULL, false, false),
  AddMembersToGroupSelf(611, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddMembersToGroupSelf.class,
          GNSCommand.ResultType.NULL, false, false),
  AddToGroup(612, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddToGroup.class,
          GNSCommand.ResultType.NULL, false, false),
  AddToGroupSelf(613, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddToGroupSelf.class,
          GNSCommand.ResultType.NULL, false, false),
  //
  GetGroupMembers(614, Type.SYSTEM_LOOKUP,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupMembers.class,
          GNSCommand.ResultType.LIST, true, false),
  GetGroupMembersSelf(615, Type.SYSTEM_LOOKUP,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupMembersSelf.class,
          GNSCommand.ResultType.LIST, true, false),
  GetGroups(616, Type.SYSTEM_LOOKUP,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroups.class,
          GNSCommand.ResultType.LIST, true, false),
  GetGroupsSelf(617, Type.SYSTEM_LOOKUP,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupsSelf.class,
          GNSCommand.ResultType.LIST, true, false),
  //
  RemoveFromGroup(620, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveFromGroup.class,
          GNSCommand.ResultType.NULL, false, false),
  RemoveFromGroupSelf(621, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveFromGroupSelf.class,
          GNSCommand.ResultType.NULL, false, false),
  RemoveMembersFromGroup(622, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveMembersFromGroup.class,
          GNSCommand.ResultType.NULL, false, false),
  RemoveMembersFromGroupSelf(623, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveMembersFromGroupSelf.class,
          GNSCommand.ResultType.NULL, false, false),
  // Admin
  Help(710, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Help.class,
          GNSCommand.ResultType.STRING, true, false),
  HelpTcp(711, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.HelpTcp.class,
          GNSCommand.ResultType.STRING, true, false),
  HelpTcpWiki(712, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.HelpTcpWiki.class,
          GNSCommand.ResultType.STRING, true, false),
  Admin(715, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Admin.class,
          GNSCommand.ResultType.NULL, true, true),
  Dump(716, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Dump.class,
          GNSCommand.ResultType.STRING, true, true),
  //
  GetParameter(720, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.GetParameter.class,
          GNSCommand.ResultType.STRING, true, true),
  SetParameter(721, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.SetParameter.class,
          GNSCommand.ResultType.NULL, true, true),
  ListParameters(722, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ListParameters.class,
          GNSCommand.ResultType.STRING, true, true),
  DeleteAllRecords(723, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.DeleteAllRecords.class,
          GNSCommand.ResultType.NULL, true, true),
  ResetDatabase(724, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ResetDatabase.class,
          GNSCommand.ResultType.NULL, true, true),
  ClearCache(725, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ClearCache.class,
          GNSCommand.ResultType.NULL, true, true),
  DumpCache(726, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.DumpCache.class,
          GNSCommand.ResultType.STRING, true, false),
  //
  ChangeLogLevel(730, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ChangeLogLevel.class,
          GNSCommand.ResultType.NULL, true, true),
  @Deprecated
  AddTag(731, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.AddTag.class,
          GNSCommand.ResultType.NULL, true, false),
  @Deprecated
  RemoveTag(732, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.RemoveTag.class,
          GNSCommand.ResultType.NULL, true, false),
  @Deprecated
  ClearTagged(733, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ClearTagged.class,
          GNSCommand.ResultType.NULL, true, false),
  @Deprecated
  GetTagged(734, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.GetTagged.class,
          GNSCommand.ResultType.STRING, true, false),
  ConnectionCheck(737, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ConnectionCheck.class,
          GNSCommand.ResultType.STRING, true, false),
  // Active code
  SetCode(810, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.SetCode.class,
          GNSCommand.ResultType.NULL, true, false),
  ClearCode(811, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.ClearCode.class,
          GNSCommand.ResultType.NULL, true, false),
  GetCode(812, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.GetCode.class,
          GNSCommand.ResultType.STRING, true, false),
  // Catch all for parsing errors
  Unknown(999, Type.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Unknown.class,
          GNSCommand.ResultType.NULL, true, true);

  private final int number;
  private final Type category;
  private final Class<?> commandClass;
  private final GNSCommand.ResultType returnType;
  /* arun: Adding more fields below for documentation and invariant assertion
	 * purposes. */
  /**
   * True means that this command can be safely coordinated by RemoteQuery at
   * servers. True must mean that it is never the case that execute(name1) for
   * any canBeSafelyCoordinated command never remote-query-invokes this
   * command also for name1.
   */
  private boolean canBeSafelyCoordinated;

  /**
   * Commands that are not intended to be run by rogue clients.
   */
  private boolean notForRogueClients;

  /**
   * Other commands that may be remote-query-invoked by this command.
   * Non-final only because we can not name enums before they are defined.
   */
  private CommandType[] invokedCommands;

  private void setChain(CommandType... invokedCommands) {
    if (this.invokedCommands == null) {
      this.invokedCommands = invokedCommands;
    } else {
      throw new RuntimeException("Can set command chain exactly once");
    }
  }

  /**
   * The chain of any command must contain a list
   * of all commands that the execution of that command MAY invoke. For
   * example, if AddGuid may (not necessarily every time) invoke LookupGuid as
   * a step, LookupGuid belongs to AddGuid's chain. The list interpretation is
   * somewhat loose as the call chain is really a DAG, but it is good to
   * flatten the DAG in breadth-first order, e.g., if A->B and B->C and B->D
   * and D->E, where "->" means "invokes", the chain of A is {B,C,D,E}. It is
   * okay to stop recursively unraveling a chain, e.g., stop at A->B, if what
   * follows is identical to B's chain.
   *
   * Hopefully there are no cycles in these chains. Assume standard execution
   * for all commands, i.e., with no active code enabled, while listing chains.
   */
  static {
    Read.setChain(ReadUnsigned);
    //ReadSelf.setChain(ReadUnsigned);
    ReadUnsigned.setChain();
    ReadMultiField.setChain(ReadUnsigned);
    ReadMultiFieldUnsigned.setChain(ReadUnsigned);
    ReadArray.setChain(ReadUnsigned);
    ReadArrayOne.setChain(ReadUnsigned);
    //ReadArrayOneSelf.setChain(ReadUnsigned);
    ReadArrayOneUnsigned.setChain();
    //ReadArraySelf.setChain(ReadUnsigned);
    ReadArrayUnsigned.setChain();
    // Every command that is a subclass of AbstractUpdate could potentially call ReadUnsigned 
    // because of the group guid check in NSAuthentication.signatureAndACLCheck.
    // Add to that all the other commands that call NSAuthentication.signatureAndACLCheck
    // which is most of them.
    Append.setChain(ReadUnsigned);
    AppendList.setChain(ReadUnsigned);
    //AppendListSelf.setChain(ReadUnsigned);
    AppendListUnsigned.setChain(ReadUnsigned);
    AppendListWithDuplication.setChain(ReadUnsigned);
    //AppendListWithDuplicationSelf.setChain(ReadUnsigned);
    AppendListWithDuplicationUnsigned.setChain(ReadUnsigned);
    AppendOrCreate.setChain(ReadUnsigned);
    AppendOrCreateList.setChain(ReadUnsigned);
    //AppendOrCreateListSelf.setChain(ReadUnsigned);
    AppendOrCreateListUnsigned.setChain(ReadUnsigned);
    //AppendOrCreateSelf.setChain(ReadUnsigned);
    AppendOrCreateUnsigned.setChain(ReadUnsigned);
    //AppendSelf.setChain(ReadUnsigned);
    AppendUnsigned.setChain(ReadUnsigned);
    AppendWithDuplication.setChain(ReadUnsigned);
    //AppendWithDuplicationSelf.setChain(ReadUnsigned);
    AppendWithDuplicationUnsigned.setChain(ReadUnsigned);
    Clear.setChain(ReadUnsigned);
    //ClearSelf.setChain(ReadUnsigned);
    ClearUnsigned.setChain(ReadUnsigned);
    Create.setChain(ReadUnsigned);
    CreateEmpty.setChain(ReadUnsigned);
    //CreateEmptySelf.setChain(ReadUnsigned);
    CreateList.setChain(ReadUnsigned);
    //CreateListSelf.setChain(ReadUnsigned);
    //CreateSelf.setChain(ReadUnsigned);
    Remove.setChain(ReadUnsigned);
    RemoveList.setChain(ReadUnsigned);
    //RemoveListSelf.setChain(ReadUnsigned);
    RemoveListUnsigned.setChain(ReadUnsigned);
    //RemoveSelf.setChain(ReadUnsigned);
    RemoveUnsigned.setChain(ReadUnsigned);
    Replace.setChain(ReadUnsigned);
    ReplaceList.setChain(ReadUnsigned);
    //ReplaceListSelf.setChain(ReadUnsigned);
    ReplaceListUnsigned.setChain(ReadUnsigned);
    ReplaceOrCreate.setChain(ReadUnsigned);
    ReplaceOrCreateList.setChain(ReadUnsigned);
    //ReplaceOrCreateListSelf.setChain(ReadUnsigned);
    ReplaceOrCreateListUnsigned.setChain(ReadUnsigned);
    //ReplaceOrCreateSelf.setChain(ReadUnsigned);
    ReplaceOrCreateUnsigned.setChain(ReadUnsigned);
    //ReplaceSelf.setChain(ReadUnsigned);
    ReplaceUnsigned.setChain(ReadUnsigned);
    ReplaceUserJSON.setChain(ReadUnsigned);
    ReplaceUserJSONUnsigned.setChain(ReadUnsigned);
    CreateIndex.setChain(ReadUnsigned);
    Substitute.setChain(ReadUnsigned);
    SubstituteList.setChain(ReadUnsigned);
    //SubstituteListSelf.setChain(ReadUnsigned);
    SubstituteListUnsigned.setChain(ReadUnsigned);
    //SubstituteSelf.setChain(ReadUnsigned);
    SubstituteUnsigned.setChain(ReadUnsigned);
    RemoveField.setChain(ReadUnsigned);
    //RemoveFieldSelf.setChain(ReadUnsigned);
    RemoveFieldUnsigned.setChain(ReadUnsigned);
    Set.setChain(ReadUnsigned);
    //SetSelf.setChain(ReadUnsigned);
    SetFieldNull.setChain(ReadUnsigned);
    //SetFieldNullSelf.setChain(ReadUnsigned);
    //
    Select.setChain();
    SelectGroupLookupQuery.setChain();
    SelectGroupSetupQueryWithGuid.setChain();
    SelectGroupSetupQueryWithGuidAndInterval.setChain();
    SelectGroupSetupQueryWithInterval.setChain();
    SelectNear.setChain();
    SelectWithin.setChain();
    SelectQuery.setChain();
    //
    AddGuid.setChain(LookupGuid, ReplaceUserJSONUnsigned, ReadUnsigned); // what else?
    RemoveGuid.setChain(ReadUnsigned);
    RemoveAccount.setChain(ReadUnsigned);
    SelectGroupSetupQuery.setChain(ReadUnsigned);
    VerifyAccount.setChain(ReplaceUserJSONUnsigned);

    AddAlias.setChain(ReadUnsigned, ReplaceUserJSONUnsigned);
    AddMultipleGuids.setChain(ReadUnsigned);
    AddMultipleGuidsFast.setChain(ReadUnsigned);
    AddMultipleGuidsFastRandom.setChain(ReadUnsigned);
    // Fixme: Some inconsistencies in the way these account commands are implmented
    // insofar as whether they go remote or not.
    LookupAccountRecord.setChain();
    LookupRandomGuids.setChain();
    LookupGuid.setChain();
    LookupPrimaryGuid.setChain(ReadUnsigned);
    LookupGuidRecord.setChain();
    RegisterAccount.setChain(ReadUnsigned);
    RegisterAccountUnsigned.setChain(ReadUnsigned);
    RemoveAlias.setChain(ReadUnsigned, ReplaceUserJSONUnsigned);
    RemoveGuidNoAccount.setChain(ReadUnsigned, ReplaceUserJSONUnsigned);
    RetrieveAliases.setChain(ReadUnsigned);
    SetPassword.setChain(ReadUnsigned, ReplaceUserJSONUnsigned);
    ResetKey.setChain(ReadUnsigned);
    //
    AclAdd.setChain(ReadUnsigned);
    AclAddSelf.setChain(ReadUnsigned);
    AclRemoveSelf.setChain(ReadUnsigned);
    AclRetrieveSelf.setChain(ReadUnsigned);
    AclRetrieve.setChain(ReadUnsigned);
    AclRemove.setChain(ReadUnsigned);
    //
    AddMembersToGroup.setChain(AppendListUnsigned);
    AddMembersToGroupSelf.setChain(AppendListUnsigned);
    AddToGroup.setChain(AppendListUnsigned);
    AddToGroupSelf.setChain(ReadUnsigned);
    GetGroupMembers.setChain(ReadUnsigned);
    GetGroupMembersSelf.setChain(ReadUnsigned);
    GetGroupsSelf.setChain(ReadUnsigned);
    GetGroups.setChain(ReadUnsigned);
    RemoveFromGroup.setChain(RemoveUnsigned);
    RemoveMembersFromGroup.setChain(RemoveUnsigned);
    RemoveFromGroupSelf.setChain(ReadUnsigned);
    RemoveMembersFromGroupSelf.setChain(ReadUnsigned);
    //
    SetCode.setChain(RemoveUnsigned);
    ClearCode.setChain(RemoveUnsigned);
    GetCode.setChain(RemoveUnsigned);
    // admin
    Help.setChain();
    HelpTcp.setChain();
    HelpTcpWiki.setChain();
    Admin.setChain();
    Dump.setChain();
    GetParameter.setChain();
    SetParameter.setChain();
    ListParameters.setChain();
    DeleteAllRecords.setChain();
    ResetDatabase.setChain();
    ClearCache.setChain();
    DumpCache.setChain();
    ChangeLogLevel.setChain();
    AddTag.setChain();
    RemoveTag.setChain();
    ClearTagged.setChain();
    GetTagged.setChain();
    ConnectionCheck.setChain();
    Unknown.setChain();

  }

  public enum Type {
    READ, UPDATE, CREATE_DELETE, SELECT, OTHER, SYSTEM_LOOKUP
  }

  private CommandType(int number, Type category, Class<?> commandClass,
          GNSCommand.ResultType returnType, boolean canBeSafelyCoordinated,
          boolean notForRogueClients) {
    this.number = number;
    this.category = category;
    this.commandClass = commandClass;
    this.returnType = returnType;

    this.canBeSafelyCoordinated = canBeSafelyCoordinated;

    // presumably every command is currently available to thugs
    this.notForRogueClients = notForRogueClients;

  }

  // In general, isCoordinated is not equivalent to isUpdate()
  private boolean isCoordinated() {
    return this.isUpdate();
  }

  public int getInt() {
    return number;
  }

  // add isCoordinated
  // add strictly local flag or remote flag
  // what are the set of commands that will be invoked by this command
  // AKA multi-transactional commands
  public boolean isRead() {
    return category.equals(Type.READ);
  }

  public boolean isUpdate() {
    return category.equals(Type.UPDATE);
  }

  public boolean isCreateDelete() {
    return category.equals(Type.CREATE_DELETE);
  }

  public boolean isSelect() {
    return category.equals(Type.SELECT);
  }
  
  public boolean isSystemLookup() {
	    return category.equals(Type.SYSTEM_LOOKUP);
	  }

  private static final Map<Integer, CommandType> map = new HashMap<Integer, CommandType>();

  static {
    for (CommandType type : CommandType.values()) {
      if (!type.getCommandClass().getSimpleName().equals(type.name())) {
        GNSConfig.getLogger().log(Level.WARNING,
                "Enum name should be the same as implmenting class: {0} vs. {1}",
                new Object[]{type.getCommandClass().getSimpleName(), type.name()});
      }
      if (map.containsKey(type.getInt())) {
        GNSConfig.getLogger().warning(
                "**** Duplicate number for command type " + type + ": "
                + type.getInt());
      }
      map.put(type.getInt(), type);
    }
  }

  public static CommandType getCommandType(int number) {
    return map.get(number);
  }

  public Class<?> getCommandClass() {
    return commandClass;
  }

  public static Class<?>[] getCommandClassesArray() {
    return (Class<?>[]) Stream.of(values()).map(CommandType::getCommandClass).toArray();
  }

  public static List<Class<?>> getCommandClasses() {
    return Stream.of(values()).map(CommandType::getCommandClass).collect(Collectors.toList());
  }

  public GNSCommand.ResultType getResultType() {
    return this.returnType;
  }

  private static String insertUnderScoresBeforeCapitals(String str) {
    StringBuilder result = new StringBuilder();
    // SKIP THE FIRST CAPITAL
    result.append(str.charAt(0));
    // START AT ONE SO WE SKIP THE FIRST CAPITAL
    for (int i = 1; i < str.length(); i++) {
      if (Character.isUpperCase(str.charAt(i))) {
        result.append("_");
      }
      result.append(str.charAt(i));
    }
    return result.toString();
  }

  private static String generateSwiftConstants() {
    StringBuilder result = new StringBuilder();
    for (CommandType commandType : CommandType.values()) {
      result.append("    public static let ");
      result.append(insertUnderScoresBeforeCapitals(
              commandType.toString()).toUpperCase());
      result.append("\t\t\t\t = ");
      result.append("\"");
      result.append(commandType.toString());
      result.append("\"\n");
    }
    return result.toString();
  }

  private static String generateEmptySetChains() {
    StringBuilder result = new StringBuilder();
    for (CommandType commandType : CommandType.values()) {
      if (commandType.invokedCommands == null) {
        result.append("    ");
        result.append(commandType.name());
        result.append(".setChain(ReadUnsigned);");
        result.append("\n");
      }
    }
    return result.toString();
  }

  private static void enforceChecks() {
    HashSet<CommandType> curLevel, nextLevel = new HashSet<CommandType>(), cumulative = new HashSet<CommandType>();

    for (CommandType ctype : CommandType.values()) {
      curLevel = new HashSet<CommandType>(Arrays.asList(ctype));
      nextLevel.clear();
      cumulative.clear();

      while (!curLevel.isEmpty()) {
        nextLevel.clear();
        for (CommandType curLevelType : curLevel) {
          nextLevel.addAll(new HashSet<CommandType>(Arrays
                  .asList(curLevelType.invokedCommands)));
        }
        curLevel = nextLevel;
        cumulative.addAll(nextLevel);
        if (curLevel.size() > 256) {
          Util.suicide("Likely cycle in chain for command "
                  + ctype);
        }

        GNSConfig.getLogger().log(Level.FINE,
                "{0} expanding next level {1}",
                new Object[]{ctype, nextLevel});
      }
      GNSConfig.getLogger().log(Level.INFO,
              "Cumulative chain for {0} = {1}",
              new Object[]{ctype, cumulative});

      for (CommandType downstream : cumulative) {
        if ((ctype.isUpdate() && downstream.isCoordinated())) {
          throw new RuntimeException("Coordinated " + ctype
                  + " is invoking another coordinated command "
                  + downstream);
        }
      }

    }
  }

  public static class CommandTypeTest extends DefaultTest {

    @Test
    public void enforceChecks() {
      CommandType.enforceChecks();
    }
  }

  public static void main(String args[]) {
    CommandType.enforceChecks();
    //System.out.println(generateEmptySetChains());
    System.out.println(generateSwiftConstants());
  }
}
