package edu.umass.cs.gns.client;

import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.util.Format;
import edu.umass.cs.gns.util.JSONUtils;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Stores the username, GUID and public key for a user
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

  public AccountInfo(String userName, String guid, String password) {
    this.primaryName = userName;
    this.primaryGuid = guid;
    this.type = "DEFAULT"; // huh? :-)
    this.aliases = new HashSet<String>();
    this.guids = new HashSet<String>();
    this.created = new Date();
    this.updated = new Date();
    this.password = password;
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

  public void addAlias(String alias) {
    aliases.add(alias);
  }

  public void removeAlias(String alias) {
    aliases.remove(alias);
  }

  public ArrayList<String> getGuids() {
    return new ArrayList<String>(guids);
  }

  public void addGuid(String guid) {
    guids.add(guid);
  }

  public void removeGuid(String guid) {
    guids.remove(guid);
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

  /**
   * Convert UserInfo to and from the format which is used to store it in the DB. 
   * Use a JSON Object which is put as the first
   * element of an ArrayList
   */
  public ArrayList<String> toDBFormat() throws JSONException {
    return new ArrayList<String>(Arrays.asList(toJSONObject().toString()));
  }

  public AccountInfo(QueryResultValue queryResult) throws JSONException, ParseException {
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

  public AccountInfo(JSONObject json) throws JSONException, ParseException {
    this.primaryName = json.getString(USERNAME);
    this.primaryGuid = json.getString(GUID);
    this.type = json.getString(TYPE);
    this.aliases = JSONUtils.JSONArrayToHashSet(json.getJSONArray(ALIASES));
    this.guids = JSONUtils.JSONArrayToHashSet(json.getJSONArray(GUIDS));
    this.created = Format.parseDateUTC(json.getString(CREATED));
    this.updated = Format.parseDateUTC(json.getString(UPDATED));
    this.password = json.optString(PASSWORD, null);
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
    return json;
  }

  @Override
  public String toString() {
    return "AccountInfo{" + "userName=" + primaryName + ", guid=" + primaryGuid + ", type=" + type + ", aliases=" + aliases + ", guids=" + guids + ", created=" + created + ", updated=" + updated + ", password=" + password + '}';
  }

}
