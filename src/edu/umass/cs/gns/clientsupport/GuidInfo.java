package edu.umass.cs.gns.clientsupport;

//import edu.umass.cs.gns.packet.QueryResultValue;
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
 * Stores the username, GUID and public key for a user
 *
 * @author westy
 */
public class GuidInfo {

  /**
   * The GUID.
   */
  private String guid;
  /**
   * The human readable name corresponding to this GUID.
   * There will be an entry in the GNS that reads <name> : _GNS_GUID -> <guid>
   */
  private String name;
  /**
   * A base64 encoded public key.
   */
  private String publicKey;
  // This is reserved for future use.
  private String type;
  /** 
   * Special uses.
   */
  private Set<String> tags;
  private Date created;
  private Date updated;
  

  public GuidInfo(String userName, String guid, String publicKey) {
    this.name = userName;
    this.guid = guid;
    this.type = "DEFAULT"; // huh? :-)
    this.publicKey = publicKey;
    this.created = new Date();
    this.updated = new Date();
    this.tags = new HashSet<String>();
  }

  /**
   * Convert GuidInfo to and from the format which is used to store it in the DB. 
   * Use a JSON Object which is put as the first element of an ArrayList
   */
  public ResultValue toDBFormat() throws JSONException {
    return new ResultValue(Arrays.asList(toJSONObject().toString()));
  }

  public GuidInfo(ResultValueString queryResult) throws JSONException, ParseException {
    this(new JSONObject(queryResult.get(0)));
  }
  public static final String PUBLICKEY = "publickey";
  public static final String NAME = "name";
  public static final String GUID = "guid";
  public static final String TYPE = "type";
  public static final String CREATED = "created";
  public static final String UPDATED = "updated";
  public static final String TAGS = "tags";

  public GuidInfo(JSONObject json) throws JSONException, ParseException {
    this.publicKey = json.getString(PUBLICKEY);
    this.name = json.getString(NAME);
    this.guid = json.getString(GUID);
    this.type = json.getString(TYPE);
    this.created = Format.parseDateUTC(json.getString(CREATED));
    this.updated = Format.parseDateUTC(json.getString(UPDATED));
    this.tags = JSONUtils.JSONArrayToHashSet(json.getJSONArray(TAGS));
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(PUBLICKEY, publicKey);
    json.put(NAME, name);
    json.put(GUID, guid);
    json.put(TYPE, type);
    json.put(CREATED, Format.formatDateUTC(created));
    json.put(UPDATED, Format.formatDateUTC(updated));
    json.put(TAGS, new JSONArray(tags));
    return json;
  }

  
  public String getName() {
    return name;
  }

  public String getGuid() {
    return guid;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public String getType() {
    return type;
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

  public boolean containsTag(String tag) {
    return tags.contains(tag);
  }

  public void addTag(String tag) {
    tags.add(tag);
  }

  public void removeTag(String tag) {
    tags.remove(tag);
  }

  @Override
  public String toString() {
    return "GuidInfo{" + "guid=" + guid + ", name=" + name + ", publicKey=" + publicKey + ", type=" + type + ", tags=" + tags + ", created=" + created + ", updated=" + updated + '}';
  }
  
}
