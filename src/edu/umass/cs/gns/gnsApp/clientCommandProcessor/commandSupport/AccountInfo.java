/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ResultValueString;
import edu.umass.cs.gns.util.Format;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Stores the Human Readable Name (HRN), GUID for an account plus
 * other stuff we need to keep like a password, aliases and additional guids.
 * 
 * This class handles the conversion to and from JSON objects as well as 
 * conversion to the format which can be store in the database.
 * 
 * The account can have multiple aliases which are extra HRNs. The account can
 * also have additional associated GUIDs. For certain things we also keep an encrypted
 * password.
 *
 * @author westy
 */
public class AccountInfo {

  private String primaryName;
  private String primaryGuid;
  // This is reserved for future use.
  private String type;
  private Set<String> aliases;
  private Set<String> guids;
  private Date created;
  private Date updated;
  /**
   * An encrypted password
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
   * Stores the guid and associated human readable name and
   * other information for an Account guid.
   *
   * This class handles the conversion to and from JSON objects as well as 
   * conversion to the format which can be store in the database.
   *
   * @author westy
   * @param userName
   * @param password
   * @param primaryGUID
   */
  public AccountInfo(String userName, String primaryGUID, String password) {
    this.primaryName = userName;
    this.primaryGuid = primaryGUID;
    this.type = "DEFAULT"; // huh? :-)
    this.aliases = new HashSet<String>();
    this.guids = new HashSet<String>();
    this.created = new Date();
    this.updated = new Date();
    this.password = password;
    this.verified = false;
    this.verificationCode = null;
  }

  /**
   * Returns the primary name.
   * 
   * @return
   */
  public String getPrimaryName() {
    return primaryName;
  }

  /**
   * Returns the guid.
   * 
   * @return
   */
  public String getPrimaryGuid() {
    return primaryGuid;
  }

  /**
   * Returns the password.
   *
   * @return
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
   * @return
   */
  public String getType() {
    return type;
  }

  /**
   * Returns the aliases.
   *
   * @return
   */
  public ArrayList<String> getAliases() {
    return new ArrayList<String>(aliases);
  }

  /**
   * Returns true if the alias is present.
   *
   * @param alias
   * @return
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
   * @return
   */
  public boolean removeAlias(String alias) {
    return aliases.remove(alias);
  }

  /**
   * Returns the guids associated with this account.
   *
   * @return
   */
  public ArrayList<String> getGuids() {
    return new ArrayList<String>(guids);
  }

  /**
   * Adds a guid to this account.
   *
   * @param guid
   */
  public void addGuid(String guid) {
    guids.add(guid);
  }

  /**
   * Removes a guid from this account.
   *
   * @param guid
   * @return
   */
  public boolean removeGuid(String guid) {
    return guids.remove(guid);
  }

  /**
   * Returns the creation date of the guid.
   *
   * @return
   */
  public Date getCreated() {
    return created;
  }

  /**
   * Returns the update date of the guid.
   *
   * @return
   */
  public Date getUpdated() {
    return updated;
  }

  /**
   * Updates the update date of the guid.
   *
   */
  public void noteUpdate() {
    this.updated = new Date();
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
   * @return
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
  }

  /**
   * Returns the verification code for this account.
   *
   * @return
   */
  public String getVerificationCode() {
    return verificationCode;
  }

  /**
   * Convert UserInfo to and from the format which is used to store it in the DB. 
   * Use a JSON Object which is put as the first
   * element of an ArrayList
   * @return 
   * @throws org.json.JSONException 
   */
  public ResultValue toDBFormat() throws JSONException {
    return new ResultValue(Arrays.asList(toJSONObject().toString()));
  }

  /**
   * Creates an AccountInfo instance from a ResultValueString which is the format
   * used to store the object in the database.
   *
   * @param queryResult
   * @throws JSONException
   * @throws ParseException
   */
  public AccountInfo(ResultValueString queryResult) throws JSONException, ParseException {
    this(new JSONObject(queryResult.get(0)));
  }
  // JSON Conversion
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

  /**
   * Creates an AccountInfo instance from a JSONObject.
   *
   * @param json
   * @throws JSONException
   * @throws ParseException
   */
  public AccountInfo(JSONObject json) throws JSONException, ParseException {
    this.primaryName = json.getString(USERNAME);
    this.primaryGuid = json.getString(GUID);
    this.type = json.getString(TYPE);
    this.aliases = JSONUtils.JSONArrayToHashSet(json.getJSONArray(ALIASES));
    this.guids = JSONUtils.JSONArrayToHashSet(json.getJSONArray(GUIDS));
    this.created = Format.parseDateUTC(json.getString(CREATED));
    this.updated = Format.parseDateUTC(json.getString(UPDATED));
    this.password = json.optString(PASSWORD, null);
    this.verified = json.getBoolean(VERIFIED);
    this.verificationCode = json.optString(CODE, null);
  }

  /**
   * Converts this instance into a JSONObject.
   * @return
   * @throws JSONException
   */
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(USERNAME, primaryName);
    json.put(GUID, primaryGuid);
    json.put(TYPE, type);
    json.put(ALIASES, new JSONArray(aliases));
    json.put(GUIDS, new JSONArray(guids));
    json.put(CREATED, Format.formatDateUTC(created));
    json.put(UPDATED, Format.formatDateUTC(updated));
    if (password != null) {
      json.put(PASSWORD, password);
    }
    json.put(VERIFIED, verified);
    if (verificationCode != null) {
      json.put(CODE, verificationCode);
    }
    return json;
  }

  /**
   * Returns a informational string version of the instance.
   * @return
   */
  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      return "AccountInfo{" + primaryGuid + "}";
    }
  }
}
