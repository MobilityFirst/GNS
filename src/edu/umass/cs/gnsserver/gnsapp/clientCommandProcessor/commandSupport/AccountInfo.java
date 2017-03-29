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
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Stores the Human Readable Name (HRN), GUID for an account plus
 * other stuff we need to keep like a password, aliases and additional guids.
 *
 * This class handles the conversion to and from JSON objects.
 *
 * The account can have multiple aliases which are extra HRNs. The account can
 * also have additional associated GUIDs. For certain things we also keep an encrypted
 * password.
 *
 * @author westy, arun
 */
public class AccountInfo {

  private final String name;
  private final String guid;
  // This is reserved for future use.
  private final String type;
  private final Set<String> aliases;
  private final Set<String> guids;
  private final Date created;
  private Date updated;
  /**
   * An encrypted and base64 encoded password
   */
  private String password;
  /**
   * Indicates if the account has been verified.
   */
  private boolean verified;
  /**
   * Used during the verification process.
   */
  private String verificationCode;
  /**
   * The time the verification code was created.
   */
  private Date codeTime;

  /**
   * Stores the guid and associated human readable name and
   * other information for an Account guid.
   *
   * This class handles the conversion to and from JSON objects as well as
   * conversion to the format which can be store in the database.
   *
   * @param name
   * @param password
   * @param guid
   */
  public AccountInfo(String name, String guid, String password) {
    this.name = name;
    this.guid = guid;
    this.type = "DEFAULT"; // huh? :-)
    this.aliases = new HashSet<>();
    this.guids = new HashSet<>();
    this.created = new Date();
    this.updated = new Date();
    this.password = password;
    // FIXME: TEST HACK
    //this.verified = true;
    this.verified = false;
    this.verificationCode = null;
    this.codeTime = null;
  }

  /**
   * Returns the primary name.
   *
   * @return a string
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the guid.
   *
   * @return a string
   */
  public String getGuid() {
    return guid;
  }

  /**
   * Returns the password.
   *
   * @return a string
   */
  public String getPassword() {
    return password;
  }

  /**
   * Sets the password.
   *
   * @param password
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Gets the type. Currently unused.
   *
   * @return a string
   */
  public String getType() {
    return type;
  }

  /**
   * Returns the aliases.
   *
   * @return a list of strings
   */
  public List<String> getAliases() {
    return new ArrayList<>(aliases);
  }

  /**
   * Returns true if the alias is present.
   *
   * @param alias
   * @return true or false
   */
  public boolean containsAlias(String alias) {
    return aliases.contains(alias);
  }

  /**
   * Adds the alias to the list of aliases
   *
   * @param alias
   */
  public void addAlias(String alias) {
    aliases.add(alias);
  }

  /**
   * Removes the alias from the list of aliases.
   *
   * @param alias
   * @return true if the alias was found
   */
  public boolean removeAlias(String alias) {
    return aliases.remove(alias);
  }

  /**
   * Returns the guids associated with this account.
   *
   * @return a list of strings
   */
  public List<String> getGuids() {
    return new ArrayList<>(guids);
  }

  /**
   * Adds a guid to this account.
   *
   * @param guid
   * @return {@code this}
   */
  public AccountInfo addGuid(String guid) {
    guids.add(guid);
    return this;
  }

  /**
   * Removes a guid from this account.
   *
   * @param guid
   * @return true if the guid was found
   */
  public boolean removeGuid(String guid) {
    return guids.remove(guid);
  }

  /**
   * Returns the creation date of the guid.
   *
   * @return the date
   */
  public Date getCreated() {
    return created;
  }

  /**
   * Returns the update date of the guid.
   *
   * @return the date
   */
  public Date getUpdated() {
    return updated;
  }

  /**
   * Updates the update date of the guid.
   *
   * @return {@code this}
   *
   */
  public AccountInfo noteUpdate() {
    this.updated = new Date();
    return this;
  }

  /**
   * Sets the verification flag.
   *
   * @param verified
   */
  public void setVerified(boolean verified) {
    this.verified = verified;
  }

  /**
   * Returns true is this account has been verified.
   *
   * @return true is this account has been verified
   */
  public boolean isVerified() {
    return verified;
  }

  /**
   * Sets the verification code for this account.
   *
   * @param verificationCode
   */
  public void setVerificationCode(String verificationCode) {
    this.verificationCode = verificationCode;
    this.codeTime = new Date();
  }

  /**
   * Returns the verification code for this account.
   *
   * @return a string
   */
  public String getVerificationCode() {
    return verificationCode;
  }

  /**
   * Returns the time the verification code was created. Can be null.
   *
   * @return the time the verification code was created
   */
  public Date getCodeTime() {
    return codeTime;
  }

  // JSON Conversion
  // Todo: this should be using string in GNSCommandProtocol
  private static final String USERNAME = "username";
  private static final String GUID = "guid";
  private static final String TYPE = "type";
  private static final String ALIASES = "aliases";
  private static final String GUIDS = "guids";
  private static final String CREATED = "created";
  private static final String UPDATED = "updated";
  private static final String PASSWORD = "password";
  private static final String VERIFIED = "verified";
  private static final String CODE = "code";
  private static final String CODE_TIME = "codeTime";

  /**
   * Creates an AccountInfo instance from a JSONObject.
   *
   * @param json
   * @throws JSONException
   * @throws ParseException
   */
  public AccountInfo(JSONObject json) throws JSONException, ParseException {
    this.name = json.getString(USERNAME);
    this.guid = json.getString(GUID);
    this.type = json.getString(TYPE);
    this.aliases = JSONUtils.JSONArrayToHashSet(json.getJSONArray(ALIASES));
    this.guids = JSONUtils.JSONArrayToHashSet(json.getJSONArray(GUIDS));
    this.created = Format.parseDateUTC(json.getString(CREATED));
    this.updated = Format.parseDateUTC(json.getString(UPDATED));
    this.password = json.optString(PASSWORD, null);
    this.verified = json.getBoolean(VERIFIED);
    this.verificationCode = json.optString(CODE, null);
    this.codeTime = json.has(CODE_TIME) ? Format.parseDateUTC(json
            .getString(CODE_TIME)) : null;
  }

  /**
   * Converts this instance into a JSONObject.
   *
   * @return a JSONObject
   * @throws JSONException
   */
  public JSONObject toJSONObject() throws JSONException {
    return toJSONObject(false);
  }

  // If the number of subguids exceeds this we don't return them
  // to the client.
  private static final int TOO_MANY_GUIDS = 50000;

  /**
   * Converts this instance into a JSONObject. If forClient is true, we don't
   * include some information like the verification code and the actual
   * guids if there are too many.
   *
   * @param forClient
   * @return the JSON Object
   * @throws JSONException
   */
  public JSONObject toJSONObject(boolean forClient) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(USERNAME, name);
    json.put(GUID, guid);
    json.put(TYPE, type);
    json.put(ALIASES, new JSONArray(aliases));
    if (forClient && guids.size() > TOO_MANY_GUIDS) {
      json.put("guidCnt", guids.size());
    } else {
      json.put("guidCnt", guids.size());
      json.put(GUIDS, new JSONArray(guids));
    }
    json.put(CREATED, Format.formatDateUTC(created));
    json.put(UPDATED, Format.formatDateUTC(updated));
    if (password != null) {
      json.put(PASSWORD, password);
    }
    json.put(VERIFIED, verified);
    if (!forClient) {
      if (verificationCode != null) {
        json.put(CODE, verificationCode);
      }
      if (codeTime != null) {
        json.put(CODE_TIME, Format.formatDateUTC(codeTime));
      }
    }
    return json;
  }

  /**
   * Returns a informational string version of the instance.
   *
   * @return a string
   */
  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      return "AccountInfo{" + guid + "}";
    }
  }
}
