
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
import java.util.Set;


public class AccountInfo {

  private final String name;
  private final String guid;
  // This is reserved for future use.
  private final String type;
  private final Set<String> aliases;
  private final Set<String> guids;
  private final Date created;
  private Date updated;

  private String password;

  private boolean verified;

  private String verificationCode;

  private Date codeTime;


  public AccountInfo(String name, String guid, String password) {
    this.name = name;
    this.guid = guid;
    this.type = "DEFAULT"; // huh? :-)
    this.aliases = new HashSet<String>();
    this.guids = new HashSet<String>();
    this.created = new Date();
    this.updated = new Date();
    this.password = password;
    // FIXME: TEST HACK
    //this.verified = true;
    this.verified = false;
    this.verificationCode = null;
    this.codeTime = null;
  }


  public String getName() {
    return name;
  }


  public String getGuid() {
    return guid;
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


	public AccountInfo addGuid(String guid) {
		guids.add(guid);
		return this;
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


	public AccountInfo noteUpdate() {
		this.updated = new Date();
		return this;
	}


	public void setVerified(boolean verified) {
		this.verified = verified;
	}


	public boolean isVerified() {
		return verified;
	}


	public void setVerificationCode(String verificationCode) {
		this.verificationCode = verificationCode;
		this.codeTime = new Date();
	}


	public String getVerificationCode() {
		return verificationCode;
	}


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


	public JSONObject toJSONObject() throws JSONException {
		return toJSONObject(false);
	}

	private static final int TOO_MANY_GUIDS = 50000;


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
		if (verificationCode != null) {
			json.put(CODE, verificationCode);
		}
		if (codeTime != null) {
			json.put(CODE_TIME, Format.formatDateUTC(codeTime));
		}
		return json;
	}


	@Override
	public String toString() {
		try {
			return toJSONObject().toString();
		} catch (JSONException e) {
			return "AccountInfo{" + guid + "}";
		}
	}
}
