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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * All the commands supported by the GNS server are listed here.
 *
 * Each one of these has a corresponding method in an array defined in
 * edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandDefs
 */
// We could probably dispense with the CommandDefs array (see above) and just
// put the classes in the enum
// once we upgrade older clients to not use the old command strings.
public enum CommandType {
	Append(110, Type.UPDATE, GNSCommand.ResultType.NULL, true, false),

	AppendList(111, Type.UPDATE),

	AppendListSelf(112, Type.UPDATE),

	AppendListUnsigned(113, Type.UPDATE),

	AppendListWithDuplication(114, Type.UPDATE),

	AppendListWithDuplicationSelf(115, Type.UPDATE),

	AppendListWithDuplicationUnsigned(116, Type.UPDATE),

	//
	AppendOrCreate(120, Type.UPDATE),

	AppendOrCreateList(121, Type.UPDATE),

	AppendOrCreateListSelf(122, Type.UPDATE),

	AppendOrCreateListUnsigned(123, Type.UPDATE),

	AppendOrCreateSelf(124, Type.UPDATE),

	AppendOrCreateUnsigned(125, Type.UPDATE),

	//
	AppendSelf(130, Type.UPDATE),

	AppendUnsigned(131, Type.UPDATE),

	AppendWithDuplication(132, Type.UPDATE),

	AppendWithDuplicationSelf(133, Type.UPDATE),

	AppendWithDuplicationUnsigned(134, Type.UPDATE),

	//

	Clear(140, Type.UPDATE),

	ClearSelf(141, Type.UPDATE),

	ClearUnsigned(142, Type.UPDATE),

	//
	Create(150, Type.UPDATE),

	CreateEmpty(151, Type.UPDATE),

	CreateEmptySelf(152, Type.UPDATE),

	CreateList(153, Type.UPDATE),

	CreateListSelf(154, Type.UPDATE),

	CreateSelf(155, Type.UPDATE),

	//
	Read(160, Type.READ, GNSCommand.ResultType.MAP, true, false),

	ReadSelf(161, Type.READ, GNSCommand.ResultType.MAP),

	ReadUnsigned(162, Type.READ, GNSCommand.ResultType.MAP),

	ReadMultiField(163, Type.READ, GNSCommand.ResultType.MAP),

	ReadMultiFieldUnsigned(164, Type.READ, GNSCommand.ResultType.MAP),

	//
	ReadArray(170, Type.READ, GNSCommand.ResultType.MAP),

	ReadArrayOne(171, Type.READ),

	ReadArrayOneSelf(172, Type.READ),

	ReadArrayOneUnsigned(173, Type.READ),

	ReadArraySelf(174, Type.READ),

	ReadArrayUnsigned(175, Type.READ),

	//
	Remove(180, Type.UPDATE),

	RemoveList(181, Type.UPDATE),

	RemoveListSelf(182, Type.UPDATE),

	RemoveListUnsigned(183, Type.UPDATE),

	RemoveSelf(184, Type.UPDATE),

	RemoveUnsigned(185, Type.UPDATE),

	//
	Replace(190, Type.UPDATE),

	ReplaceList(191, Type.UPDATE),

	ReplaceListSelf(192, Type.UPDATE),

	ReplaceListUnsigned(193, Type.UPDATE),

	//
	ReplaceOrCreate(210, Type.UPDATE),

	ReplaceOrCreateList(211, Type.UPDATE),

	ReplaceOrCreateListSelf(212, Type.UPDATE),

	ReplaceOrCreateListUnsigned(213, Type.UPDATE),

	ReplaceOrCreateSelf(214, Type.UPDATE),

	ReplaceOrCreateUnsigned(215, Type.UPDATE),

	ReplaceSelf(216, Type.UPDATE),

	ReplaceUnsigned(217, Type.UPDATE),

	//
	ReplaceUserJSON(220, Type.UPDATE),

	ReplaceUserJSONUnsigned(221, Type.UPDATE),

	//
	CreateIndex(230, Type.OTHER),

	//
	Substitute(231, Type.UPDATE),

	SubstituteList(232, Type.UPDATE),

	SubstituteListSelf(233, Type.UPDATE),

	SubstituteListUnsigned(234, Type.UPDATE),

	SubstituteSelf(235, Type.UPDATE),

	SubstituteUnsigned(236, Type.UPDATE),

	//
	RemoveField(240, Type.UPDATE),

	RemoveFieldSelf(241, Type.UPDATE),

	RemoveFieldUnsigned(242, Type.UPDATE),

	//
	Set(250, Type.UPDATE),

	SetSelf(251, Type.UPDATE),

	SetFieldNull(252, Type.UPDATE),

	SetFieldNullSelf(253, Type.UPDATE),

	// Select
	Select(310, Type.SELECT),

	SelectGroupLookupQuery(311, Type.SELECT),

	SelectGroupSetupQuery(312, Type.SELECT),

	SelectGroupSetupQueryWithGuid(313, Type.SELECT),

	SelectGroupSetupQueryWithGuidAndInterval(314, Type.SELECT),

	SelectGroupSetupQueryWithInterval(315, Type.SELECT),

	//
	SelectNear(320, Type.SELECT, GNSCommand.ResultType.LIST),

	SelectWithin(321, Type.SELECT, GNSCommand.ResultType.LIST),

	SelectQuery(322, Type.SELECT),

	// Account
	AddAlias(410, Type.CREATE_DELETE),

	AddGuid(411, Type.CREATE_DELETE),

	AddMultipleGuids(412, Type.CREATE_DELETE),

	AddMultipleGuidsFast(413, Type.CREATE_DELETE),

	AddMultipleGuidsFastRandom(414, Type.CREATE_DELETE),

	//
	LookupAccountRecord(420, Type.OTHER),

	LookupRandomGuids(421, Type.OTHER), // for testing

	LookupGuid(422, Type.OTHER),

	LookupPrimaryGuid(423, Type.OTHER),

	LookupGuidRecord(424, Type.OTHER),

	//
	RegisterAccount(430, Type.CREATE_DELETE),

	RegisterAccountSansPassword(431, Type.CREATE_DELETE),

	RegisterAccountUnsigned(432, Type.CREATE_DELETE),

	//
	RemoveAccount(440, Type.CREATE_DELETE),

	RemoveAlias(441, Type.CREATE_DELETE),

	RemoveGuid(442, Type.CREATE_DELETE),

	RemoveGuidNoAccount(443, Type.CREATE_DELETE),

	RetrieveAliases(444, Type.OTHER, GNSCommand.ResultType.LIST),

	//
	SetPassword(450, Type.UPDATE),

	VerifyAccount(451, Type.OTHER),

	//
	ResetKey(460, Type.UPDATE),

	// ACL
	AclAdd(510, Type.UPDATE),

	AclAddSelf(511, Type.UPDATE),

	AclRemove(512, Type.UPDATE),

	AclRemoveSelf(513, Type.UPDATE),

	AclRetrieve(514, Type.UPDATE),

	AclRetrieveSelf(515, Type.UPDATE),

	// Group
	AddMembersToGroup(610, Type.OTHER),

	AddMembersToGroupSelf(611, Type.OTHER),

	AddToGroup(612, Type.OTHER),

	AddToGroupSelf(613, Type.OTHER),

	GetGroupMembers(614, Type.OTHER, GNSCommand.ResultType.LIST),

	GetGroupMembersSelf(615, Type.OTHER),

	GetGroups(616, Type.OTHER, GNSCommand.ResultType.LIST),

	GetGroupsSelf(617, Type.OTHER),

	//
	RemoveFromGroup(620, Type.OTHER),

	RemoveFromGroupSelf(621, Type.OTHER),

	RemoveMembersFromGroup(622, Type.OTHER),

	RemoveMembersFromGroupSelf(623, Type.OTHER),

	// Admin
	Help(710, Type.OTHER),

	HelpTcp(711, Type.OTHER),

	HelpTcpWiki(712, Type.OTHER),

	Admin(715, Type.OTHER),

	Dump(716, Type.OTHER),

	//
	GetParameter(720, Type.OTHER),

	SetParameter(721, Type.OTHER),

	ListParameters(722, Type.OTHER),

	DeleteAllRecords(723, Type.OTHER),

	ResetDatabase(724, Type.OTHER),

	ClearCache(725, Type.OTHER),

	DumpCache(726, Type.OTHER),

	//
	ChangeLogLevel(730, Type.OTHER),

	AddTag(731, Type.OTHER),

	RemoveTag(732, Type.OTHER),

	ClearTagged(733, Type.OTHER),

	GetTagged(734, Type.OTHER),

	ConnectionCheck(737, Type.OTHER),

	// Active code
	SetActiveCode(810, Type.OTHER),

	ClearActiveCode(811, Type.OTHER),

	GetActiveCode(812, Type.OTHER),

	// Catch all for parsing errors
	Unknown(999, Type.OTHER);

	private final int number;
	private final Type category;
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
	private CommandType[] commandTypes;

	private void setChain(CommandType... commandTypes) {
		if (this.commandTypes == null)
			this.commandTypes = commandTypes;
		else
			throw new RuntimeException("Can set command chain exactly once");
	}

	// Westy: please fill these in
	static {
		Read.setChain(ReadUnsigned);
		AddGuid.setChain(LookupGuid, ReplaceUserJSONUnsigned); // what else?
	}

	public enum Type {
		READ, UPDATE, CREATE_DELETE, SELECT, OTHER
	}

	private CommandType(int number, Type coordination) {
		this(number, coordination, GNSCommand.ResultType.STRING);
	}

	private CommandType(int number, Type category,
			GNSCommand.ResultType returnType) {
		this(number, category, GNSCommand.ResultType.STRING,
				category == Type.READ || category == Type.UPDATE, false);

	}

	private CommandType(int number, Type readUpdateCreateDelete,
			GNSCommand.ResultType returnType, boolean canBeSafelyCoordinated,
			boolean notForRogueClients) {
		this.number = number;
		this.category = readUpdateCreateDelete;
		this.returnType = returnType;

		this.canBeSafelyCoordinated = canBeSafelyCoordinated;

		// presumably every command is currently available to thugs
		this.notForRogueClients = notForRogueClients;

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

	private static final Map<Integer, CommandType> map = new HashMap<Integer, CommandType>();

	static {
		for (CommandType type : CommandType.values()) {
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

	public static void main(String args[]) {
		System.out.println(generateSwiftConstants());

	}
}
