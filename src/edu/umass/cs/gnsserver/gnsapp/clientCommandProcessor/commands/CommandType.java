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

package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands;

import edu.umass.cs.gnsserver.main.GNSConfig;
import java.util.HashMap;
import java.util.Map;

/**
 * All the commands supported by the GNS server are listed here.
 *
 */
public enum CommandType {
  Append(110),
  AppendList(111),
  AppendListSelf(112),
  AppendListUnsigned(113),
  AppendListWithDuplication(114),
  AppendListWithDuplicationSelf(115),
  AppendListWithDuplicationUnsigned(116),
  //
  AppendOrCreate(120),
  AppendOrCreateList(121),
  AppendOrCreateListSelf(122),
  AppendOrCreateListUnsigned(123),
  AppendOrCreateSelf(124),
  AppendOrCreateUnsigned(125),
  //
  AppendSelf(130),
  AppendUnsigned(131),
  AppendWithDuplication(132),
  AppendWithDuplicationSelf(133),
  AppendWithDuplicationUnsigned(134),
  //
  Clear(140),
  ClearSelf(141),
  ClearUnsigned(142),
  //
  Create(150),
  CreateEmpty(151),
  CreateEmptySelf(152),
  CreateList(153),
  CreateListSelf(154),
  CreateSelf(155),
  //
  Read(160),
  ReadSelf(161),
  ReadUnsigned(162),
  ReadMultiField(163),
  ReadMultiFieldUnsigned(164),
  //
  ReadArray(170),
  ReadArrayOne(171),
  ReadArrayOneSelf(172),
  ReadArrayOneUnsigned(173),
  ReadArraySelf(174),
  ReadArrayUnsigned(175),
  //
  Remove(180),
  RemoveField(181),
  RemoveFieldSelf(182),
  RemoveFieldUnsigned(183),
  RemoveList(184),
  RemoveListSelf(185),
  RemoveListUnsigned(186),
  RemoveSelf(187),
  RemoveUnsigned(188),
  //
  Replace(190),
  ReplaceList(191),
  ReplaceListSelf(192),
  ReplaceListUnsigned(193),
  //
  ReplaceOrCreate(210),
  ReplaceOrCreateList(211),
  ReplaceOrCreateListSelf(212),
  ReplaceOrCreateListUnsigned(213),
  ReplaceOrCreateSelf(214),
  ReplaceOrCreateUnsigned(215),
  ReplaceSelf(216),
  ReplaceUnsigned(217),
  //
  ReplaceUserJSON(220),
  ReplaceUserJSONUnsigned(221),
  //
  CreateIndex(230),
  Substitute(231),
  SubstituteList(232),
  SubstituteListSelf(233),
  SubstituteListUnsigned(234),
  SubstituteSelf(235),
  SubstituteUnsigned(236),
  //
  Set(240),
  SetSelf(241),
  SetFieldNull(242),
  SetFieldNullSelf(243),
  // Select
  Select(310),
  SelectGroupLookupQuery(311),
  SelectGroupSetupQuery(312),
  SelectGroupSetupQueryWithGuid(313),
  SelectGroupSetupQueryWithGuidAndInterval(314),
  SelectGroupSetupQueryWithInterval(315),
  //
  SelectNear(320),
  SelectWithin(321),
  SelectQuery(322),
  // Account
  AddAlias(410),
  AddGuid(411),
  AddMultipleGuids(412),
  AddMultipleGuidsFast(413),
  AddMultipleGuidsFastRandom(414),
  //
  LookupAccountRecord(420),
  LookupRandomGuids(421), // for testing
  LookupGuid(422),
  LookupPrimaryGuid(423),
  LookupGuidRecord(424),
  //
  RegisterAccount(430),
  RegisterAccountSansPassword(431),
  RegisterAccountUnsigned(432),
  //
  RemoveAccount(440),
  RemoveAlias(441),
  RemoveGuid(442),
  RemoveGuidNoAccount(443),
  RetrieveAliases(444),
  //
  SetPassword(450),
  VerifyAccount(451),
  //
  ResetKey(460),
  // ACL
  AclAdd(510),
  AclAddSelf(511),
  AclRemove(512),
  AclRemoveSelf(513),
  AclRetrieve(514),
  AclRetrieveSelf(515),
  // Group
  AddMembersToGroup(610),
  AddMembersToGroupSelf(611),
  AddToGroup(612),
  AddToGroupSelf(613),
  GetGroupMembers(614),
  GetGroupMembersSelf(615),
  GetGroups(616),
  GetGroupsSelf(617),
  //
  RemoveFromGroup(620),
  RemoveFromGroupSelf(621),
  RemoveMembersFromGroup(622),
  RemoveMembersFromGroupSelf(623),
  // Admin
  Help(710),
  HelpTcp(711),
  HelpTcpWiki(712),
  Admin(715),
  Dump(716),
  //
  GetParameter(720),
  SetParameter(721),
  ListParameters(722),
  DeleteAllRecords(723),
  ResetDatabase(724),
  ClearCache(725),
  DumpCache(726),
  //
  ChangeLogLevel(730),
  AddTag(731),
  RemoveTag(732),
  ClearTagged(733),
  GetTagged(734),
  PingTable(735),
  PingValue(736),
  ConnectionCheck(737),
  // Active code
  SetActiveCode(810),
  ClearActiveCode(811),
  GetActiveCode(812);

  private int number;

  private CommandType(int number) {
    this.number = number;
  }

  public int getInt() {
    return number;
  }

  private static final Map<Integer, CommandType> map = new HashMap<Integer, CommandType>();

  static {
    for (CommandType type : CommandType.values()) {
      if (map.containsKey(type.getInt())) {
        GNSConfig.getLogger().warning("**** Duplicate number for command type " + type + ": " + type.getInt());
      }
      map.put(type.getInt(), type);
    }
  }

  public static CommandType getCommandType(int number) {
    return map.get(number);
  }

}
