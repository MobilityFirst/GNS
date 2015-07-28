/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxosutil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 *         A file for just static SQL constants.
 */
public class SQL {
	/**
	 * The user name and password here are used by both gigapaxos and
	 * reconfiguration. Embedded derby can take any user name and password, so
	 * the values here should be for mysql or other DBs supported here.
	 */
	private static final String USER = "user";
	private static final String PASSWORD = "password";

	/**
	 *
	 */
	public static enum SQLType {
		/**
		 * 
		 */
		EMBEDDED_DERBY,
		/**
		 * 
		 */
		MYSQL
	};

	/**
	 * 
	 */
	public static final List<String> DUPLICATE_KEY = new ArrayList<String>(
			Arrays.asList("23505"));
	/**
	 * 
	 */
	public static final List<String> DUPLICATE_TABLE = new ArrayList<String>(
			Arrays.asList("X0Y32", "42S01", "42000"));
	/**
	 * 
	 */
	public static final List<String> NONEXISTENT_TABLE = new ArrayList<String>(
			Arrays.asList("42Y07", "42Y55", "42X05", "42S02"));

	/**
	 * @param size
	 * @param type
	 * @return The string to use in create table statements.
	 */
	public static String getClobString(int size, SQLType type) {
		switch (type) {
		case EMBEDDED_DERBY:
			return " clob(" + size + ")";
		case MYSQL:
			if (size < 65536)
				return " TEXT ";
			else if (size < (int) Math.pow(2, 24))
				return " MEDIUMTEXT ";
			else if (size < (int) Math.pow(2, 32))
				return " LONGTEXT ";
		}
		return "";
	}

	/**
	 * @param type
	 * @return Varchar size limit.
	 */
	public static int getVarcharSize(SQLType type) {
		switch (type) {
		case EMBEDDED_DERBY:
			return 32672;
		case MYSQL:
			// 65535 in 5.1 onwards
			return 21845;
		}
		Util.suicide("SQL type not recognized");
		return -1;
	}

	/**
	 * @param type
	 * @return Driver string.
	 */
	public static String getDriver(SQLType type) {
		switch (type) {
		case EMBEDDED_DERBY:
			return "org.apache.derby.jdbc.EmbeddedDriver";
		case MYSQL:
			return "com.mysql.jdbc.Driver";
		}
		Util.suicide("SQL type not recognized");
		return null;
	}

	/**
	 * @param type
	 * @return Protocol or URL string.
	 */
	public static String getProtocolOrURL(SQLType type) {
		switch (type) {
		case EMBEDDED_DERBY:
			return "jdbc:derby:";
		case MYSQL:
			return "jdbc:mysql://date.cs.umass.edu/";
		}
		Util.suicide("SQL type not recognized");
		return null;
	}

	/**
	 * @return Username.
	 */
	public static String getUser() {
		return USER;
	}

	/**
	 * @return Password.
	 */
	public static String getPassword() {
		return PASSWORD;
	}

}