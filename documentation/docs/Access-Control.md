// TODO: Bring uniformity in terminology. Ensure all terms are already defined in the "terminology" page beforehand.

#Access Control#

### Need for Access Control ###

GNS is not just intended to be used to read and write your own data. Often, other trusted applications or users (represented by GUIDs) may want to write data on your behalf. For example, a calendar application may want to write new appointments for you. Other GUIDs may also want to read data that you do not want to be visible to the whole world. For example, your fitness application may want to read your activity data to generate reports. Access Control Lists enable you to specify appropriate permissions for other GUIDs to access or update your data.

### What exactly are ACLs in GNS? ###

Access control lists or ACLs are a list of permissions associated with a particular field of a GNS record. You can specify separate lists for granting read and write privileges. Every field can therefore have a read ACL and a write ACL. It should be noted that a GUID has implicit read and write access to all the fields of its entire record.

### The special "ALL" GUID ###

If you would like to grant read or write access for a particular field to all GUIDs instead of executing the command for each GUID explicitly, you can use the special "ALL" GUID. It should be noted that this "ALL" GUID is tracked separately and execution of commands with "ALL" and regular GUIDs cannot be mixed. For example, you cannot grant read access for field "location" to all GUIDs and then remove GUID G from it.
(Example)

### The special "ALL" field ###

If you would like to grant read or write access for all fields to a particular GUID, you can use the special "ALL" field instead of executing the command multiple times for each field. It should be noted that this "ALL" field is tracked separately and execution of commands with "ALL" and regular fields cannot be mixed. For example, you cannot grant read access to a GUID for "ALL" fields and then remove this privilege for a particular field called "private".
(Example)

### Default ACL ###
Whenever a new GUID is created, it is associated with a read ACL for "ALL" fields to "ALL" GUIDs. This happens only once at the time of GUID creation. Therefore, if you would like to make some information private in your record, you are expected to first remove this default ACL and then specify privileges for individual fields.
(Example)

### ACL Inheritance ###

It is not necessary for you to specify ACLs explicitly for every field in every level (for nested fields) of the record. If you do not specify any ACL for a nested field, it assumes the ACL of its closest parent field with an ACL. If a nested field has only write ACL specified, it inherits the read ACL from its closest parent field having non-empty value for such an ACL.
(Example)

### Read ACL ###
If a particular field X has a read ACL, every GUID listed under it will have explicit read access to the value of X. This is true even if a parent field of X does not grant such an access.
(Example)

### Write ACL ###
If a particular field X has a write ACL, every GUID listed under it will have explicit write access to the value of X. This is true even if a parent field of X does not grand such an access.
(Example)

The following simple sequence of steps can be used to determine if a particular field X belonging to a record of GUID R can be read by GUID G:

- If R has a read ACL for "ALL" fields with G or "ALL" GUID in it, allow access.
- If field X has a read ACL
    - If the read ACL contains G or "ALL", allow access.
    - Otherwise, deny access.
- If field X does not has a read ACL, recursively check steps 1 and 2 for parent fields of X until a field with read ACL is found.
- If we reach the top level field which doesn't satisfy steps 1 and 2, deny access.

(Example)

What happens when access is denied?
When access is denied, appropriate exception is communicated to the client.

Coming soon:
- ACL behavior in Groups
- ACL operations - explanation of each operation with an example demonstrating its use in each client library.
- A link to the @ACL Reference page that provides an exhaustive listing of access types that can be set, ACL Operations and the default values.
