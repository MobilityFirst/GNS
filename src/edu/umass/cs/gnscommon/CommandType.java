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

import edu.umass.cs.gnsserver.main.GNSConfig;
import java.util.HashMap;
import java.util.Map;

/**
 * All the commands supported by the GNS server are listed here.
 *
 * Each one of these has a corresponding method in an array defined in
 * edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandDefs
 */
// We could probably despense with the CommandDefs array (see above) and just put the classes in the enum
// once we upgrade older clients to not use the old command strings.
public enum CommandType {
  Append(110, Coordination.UPDATE),
  AppendList(111, Coordination.UPDATE),
  AppendListSelf(112, Coordination.UPDATE),
  AppendListUnsigned(113, Coordination.UPDATE),
  AppendListWithDuplication(114, Coordination.UPDATE),
  AppendListWithDuplicationSelf(115, Coordination.UPDATE),
  AppendListWithDuplicationUnsigned(116, Coordination.UPDATE),
  //
  AppendOrCreate(120, Coordination.UPDATE),
  AppendOrCreateList(121, Coordination.UPDATE),
  AppendOrCreateListSelf(122, Coordination.UPDATE),
  AppendOrCreateListUnsigned(123, Coordination.UPDATE),
  AppendOrCreateSelf(124, Coordination.UPDATE),
  AppendOrCreateUnsigned(125, Coordination.UPDATE),
  //
  AppendSelf(130, Coordination.UPDATE),
  AppendUnsigned(131, Coordination.UPDATE),
  AppendWithDuplication(132, Coordination.UPDATE),
  AppendWithDuplicationSelf(133, Coordination.UPDATE),
  AppendWithDuplicationUnsigned(134, Coordination.UPDATE),
  //
  Clear(140, Coordination.UPDATE),
  ClearSelf(141, Coordination.UPDATE),
  ClearUnsigned(142, Coordination.UPDATE),
  //
  Create(150, Coordination.UPDATE),
  CreateEmpty(151, Coordination.UPDATE),
  CreateEmptySelf(152, Coordination.UPDATE),
  CreateList(153, Coordination.UPDATE),
  CreateListSelf(154, Coordination.UPDATE),
  CreateSelf(155, Coordination.UPDATE),
  //
  Read(160, Coordination.READ),
  ReadSelf(161, Coordination.READ),
  ReadUnsigned(162, Coordination.READ),
  ReadMultiField(163, Coordination.READ),
  ReadMultiFieldUnsigned(164, Coordination.READ),
  //
  ReadArray(170, Coordination.READ),
  ReadArrayOne(171, Coordination.READ),
  ReadArrayOneSelf(172, Coordination.READ),
  ReadArrayOneUnsigned(173, Coordination.READ),
  ReadArraySelf(174, Coordination.READ),
  ReadArrayUnsigned(175, Coordination.READ),
  //
  Remove(180, Coordination.UPDATE),
  RemoveList(181, Coordination.UPDATE),
  RemoveListSelf(182, Coordination.UPDATE),
  RemoveListUnsigned(183, Coordination.UPDATE),
  RemoveSelf(184, Coordination.UPDATE),
  RemoveUnsigned(185, Coordination.UPDATE),
  //
  Replace(190, Coordination.UPDATE),
  ReplaceList(191, Coordination.UPDATE),
  ReplaceListSelf(192, Coordination.UPDATE),
  ReplaceListUnsigned(193, Coordination.UPDATE),
  //
  ReplaceOrCreate(210, Coordination.UPDATE),
  ReplaceOrCreateList(211, Coordination.UPDATE),
  ReplaceOrCreateListSelf(212, Coordination.UPDATE),
  ReplaceOrCreateListUnsigned(213, Coordination.UPDATE),
  ReplaceOrCreateSelf(214, Coordination.UPDATE),
  ReplaceOrCreateUnsigned(215, Coordination.UPDATE),
  ReplaceSelf(216, Coordination.UPDATE),
  ReplaceUnsigned(217, Coordination.UPDATE),
  //
  ReplaceUserJSON(220, Coordination.UPDATE),
  ReplaceUserJSONUnsigned(221, Coordination.UPDATE),
  //
  CreateIndex(230, Coordination.OTHER),
  //
  Substitute(231, Coordination.UPDATE),
  SubstituteList(232, Coordination.UPDATE),
  SubstituteListSelf(233, Coordination.UPDATE),
  SubstituteListUnsigned(234, Coordination.UPDATE),
  SubstituteSelf(235, Coordination.UPDATE),
  SubstituteUnsigned(236, Coordination.UPDATE),
  //
  RemoveField(240, Coordination.UPDATE),
  RemoveFieldSelf(241, Coordination.UPDATE),
  RemoveFieldUnsigned(242, Coordination.UPDATE),
  //
  Set(250, Coordination.UPDATE),
  SetSelf(251, Coordination.UPDATE),
  SetFieldNull(252, Coordination.UPDATE),
  SetFieldNullSelf(253, Coordination.UPDATE),
  // Select
  Select(310, Coordination.SELECT),
  SelectGroupLookupQuery(311, Coordination.SELECT),
  SelectGroupSetupQuery(312, Coordination.SELECT),
  SelectGroupSetupQueryWithGuid(313, Coordination.SELECT),
  SelectGroupSetupQueryWithGuidAndInterval(314, Coordination.SELECT),
  SelectGroupSetupQueryWithInterval(315, Coordination.SELECT),
  //
  SelectNear(320, Coordination.SELECT),
  SelectWithin(321, Coordination.SELECT),
  SelectQuery(322, Coordination.SELECT),
  // Account
  AddAlias(410, Coordination.CREATE_DELETE),
  AddGuid(411, Coordination.CREATE_DELETE),
  AddMultipleGuids(412, Coordination.CREATE_DELETE),
  AddMultipleGuidsFast(413, Coordination.CREATE_DELETE),
  AddMultipleGuidsFastRandom(414, Coordination.CREATE_DELETE),
  //
  LookupAccountRecord(420, Coordination.OTHER),
  LookupRandomGuids(421, Coordination.OTHER), // for testing
  LookupGuid(422, Coordination.OTHER),
  LookupPrimaryGuid(423, Coordination.OTHER),
  LookupGuidRecord(424, Coordination.OTHER),
  //
  RegisterAccount(430, Coordination.CREATE_DELETE),
  RegisterAccountSansPassword(431, Coordination.CREATE_DELETE),
  RegisterAccountUnsigned(432, Coordination.CREATE_DELETE),
  //
  RemoveAccount(440, Coordination.CREATE_DELETE),
  RemoveAlias(441, Coordination.CREATE_DELETE),
  RemoveGuid(442, Coordination.CREATE_DELETE),
  RemoveGuidNoAccount(443, Coordination.CREATE_DELETE),
  RetrieveAliases(444, Coordination.OTHER),
  //
  SetPassword(450, Coordination.UPDATE),
  VerifyAccount(451, Coordination.OTHER),
  //
  ResetKey(460, Coordination.UPDATE),
  // ACL
  AclAdd(510, Coordination.UPDATE),
  AclAddSelf(511, Coordination.UPDATE),
  AclRemove(512, Coordination.UPDATE),
  AclRemoveSelf(513, Coordination.UPDATE),
  AclRetrieve(514, Coordination.UPDATE),
  AclRetrieveSelf(515, Coordination.UPDATE),
  // Group
  AddMembersToGroup(610, Coordination.OTHER),
  AddMembersToGroupSelf(611, Coordination.OTHER),
  AddToGroup(612, Coordination.OTHER),
  AddToGroupSelf(613, Coordination.OTHER),
  GetGroupMembers(614, Coordination.OTHER),
  GetGroupMembersSelf(615, Coordination.OTHER),
  GetGroups(616, Coordination.OTHER),
  GetGroupsSelf(617, Coordination.OTHER),
  //
  RemoveFromGroup(620, Coordination.OTHER),
  RemoveFromGroupSelf(621, Coordination.OTHER),
  RemoveMembersFromGroup(622, Coordination.OTHER),
  RemoveMembersFromGroupSelf(623, Coordination.OTHER),
  // Admin
  Help(710, Coordination.OTHER),
  HelpTcp(711, Coordination.OTHER),
  HelpTcpWiki(712, Coordination.OTHER),
  Admin(715, Coordination.OTHER),
  Dump(716, Coordination.OTHER),
  //
  GetParameter(720, Coordination.OTHER),
  SetParameter(721, Coordination.OTHER),
  ListParameters(722, Coordination.OTHER),
  DeleteAllRecords(723, Coordination.OTHER),
  ResetDatabase(724, Coordination.OTHER),
  ClearCache(725, Coordination.OTHER),
  DumpCache(726, Coordination.OTHER),
  //
  ChangeLogLevel(730, Coordination.OTHER),
  AddTag(731, Coordination.OTHER),
  RemoveTag(732, Coordination.OTHER),
  ClearTagged(733, Coordination.OTHER),
  GetTagged(734, Coordination.OTHER),
  PingTable(735, Coordination.OTHER),
  PingValue(736, Coordination.OTHER),
  ConnectionCheck(737, Coordination.OTHER),
  // Active code
  SetActiveCode(810, Coordination.OTHER),
  ClearActiveCode(811, Coordination.OTHER),
  GetActiveCode(812, Coordination.OTHER);

  private int number;
  private Coordination coordination;
  private final String alias; // must also be unique
  
  public enum Coordination {
    READ, UPDATE, CREATE_DELETE, SELECT, OTHER
  }

  private CommandType(int number, Coordination coordination) {
    this(number, coordination, number + "" // default alias is just number
    );
  }

  private CommandType(int number, Coordination readUpdateCreateDelete, String alias) {
    this.number = number;
    this.coordination = readUpdateCreateDelete;
    this.alias = alias;
  }

  public int getInt() {
    return number;
  }
  
  public boolean isRead() {
    return coordination.equals(Coordination.READ);
  }
  
  public boolean isUpdate() {
    return coordination.equals(Coordination.UPDATE);
  }
  
  public boolean isCreateDelete() {
    return coordination.equals(Coordination.CREATE_DELETE);
  }
  
  public boolean isSelect() {
    return coordination.equals(Coordination.SELECT);
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
