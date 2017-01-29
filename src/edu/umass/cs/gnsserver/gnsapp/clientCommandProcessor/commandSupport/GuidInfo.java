
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ResultValueString;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;


public class GuidInfo {


  private String guid;

  private String name;

  private String publicKey;
  // This is reserved for future use.
  private String type;

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


  public ResultValue toDBFormat() throws JSONException {
    return new ResultValue(Arrays.asList(toJSONObject().toString()));
  }


  public GuidInfo(ResultValueString queryResult) throws JSONException, ParseException {
    this(new JSONObject(queryResult.get(0)));
  }

  // JSON Conversion
  private static final String PUBLICKEY = "publickey";
  private static final String NAME = "name";
  private static final String GUID = "guid";
  private static final String TYPE = "type";
  private static final String CREATED = "created";
  private static final String UPDATED = "updated";
  private static final String TAGS = "tags";


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


  public void setPublicKey(String publicKey) {
    this.publicKey = publicKey;
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
