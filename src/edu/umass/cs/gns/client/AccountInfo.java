/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.client;

import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.nameserver.ResultValueString;
import edu.umass.cs.gns.util.Format;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.*;

/**
 * Stores the Human Readable Name (HRN), GUID and public key for an account plus
 * other stuff we need to keep like a password, aliases and additional guids.
 * 
 * The account can have multiple aliases which are extra HRNs. The account can
 * also have additional associated GUIDs. For certain things we also keep an encrypted
 * password. The primary authentication mechanism is the public key.
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

  public String getPrimaryName() {
    return primaryName;
  }

  public String getPrimaryGuid() {
    return primaryGuid;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getType() {
    return type;
  }

  public ArrayList<String> getAliases() {
    return new ArrayList<String>(aliases);
  }

  public boolean containsAlias(String alias) {
    return aliases.contains(alias);
  }

  public void addAlias(String alias) {
    aliases.add(alias);
  }

  public boolean removeAlias(String alias) {
    return aliases.remove(alias);
  }

  public ArrayList<String> getGuids() {
    return new ArrayList<String>(guids);
  }

  public void addGuid(String guid) {
    guids.add(guid);
  }

  public boolean removeGuid(String guid) {
    return guids.remove(guid);
  }

  public Date getCreated() {
    return created;
  }

  public Date getUpdated() {
    return updated;
  }

  public void noteUpdate() {
    this.updated = new Date();
  }

  public void setVerified(boolean verified) {
    this.verified = verified;
  }

  public boolean isVerified() {
    return verified;
  }

  public void setVerificationCode(String verificationCode) {
    this.verificationCode = verificationCode;
  }

  public String getVerificationCode() {
    return verificationCode;
  }

  /**
   * Convert UserInfo to and from the format which is used to store it in the DB. 
   * Use a JSON Object which is put as the first
   * element of an ArrayList
   */
  public ResultValue toDBFormat() throws JSONException {
    return new ResultValue(Arrays.asList(toJSONObject().toString()));
  }

  public AccountInfo(ResultValueString queryResult) throws JSONException, ParseException {
    this(new JSONObject(queryResult.get(0)));
  }
  public static final String USERNAME = "username";
  public static final String GUID = "guid";
  public static final String TYPE = "type";
  public static final String ALIASES = "aliases";
  public static final String GUIDS = "guids";
  public static final String CREATED = "created";
  public static final String UPDATED = "updated";
  public static final String PASSWORD = "password";
  public static final String VERIFIED = "verified";
  public static final String CODE = "code";

  public AccountInfo(JSONObject json) throws JSONException, ParseException {
    this.primaryName = json.getString(USERNAME);
    this.primaryGuid = json.getString(GUID);
    this.type = json.getString(TYPE);
    this.aliases = JSONUtils.JSONArrayToHashSet(json.getJSONArray(ALIASES));
    this.guids = JSONUtils.JSONArrayToHashSet(json.getJSONArray(GUIDS));
    this.created = Format.parseDateUTC(json.getString(CREATED));
    this.updated = Format.parseDateUTC(json.getString(UPDATED));
    this.password = json.optString(PASSWORD, null);
    this.verified = Boolean.parseBoolean(json.optString(VERIFIED, null));
    this.verificationCode = json.optString(CODE, null);
  }

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

  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      return "AccountInfo{" + primaryGuid + "}";
    }
  }
}
