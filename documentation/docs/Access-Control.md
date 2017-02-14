_// TODO: Bring uniformity in terminology. Ensure all terms are already defined in the "terminology" page beforehand._

Access Control
==============

GNS is not just intended to be used to read and write your own data. Often, other trusted applications or users (represented by GUIDs) may want to write data on your behalf. For example, a calendar application may want to write new appointments for you. Other GUIDs may also want to read data that you do not want to be visible to the whole world. For example, your fitness application may want to read your activity data to generate reports. Access Control Lists enable you to specify appropriate permissions for other GUIDs to access or update your data.

### What exactly are ACLs in GNS? ###

Access control lists or ACLs are a list of permissions associated with a particular field of a GNS record. You can specify separate lists for granting read and write privileges. Every field can therefore have a read ACL and a write ACL. It should be noted that a GUID has implicit read and write access to all the fields of its entire record. Additionally, an account GUID has implicit read and write access to the fields of all GUIDs created under it.

### The special `ALL` GUID ###

If you would like to grant read or write access for a particular field to all GUIDs instead of executing the command for each GUID explicitly, you can use the special `ALL` GUID. 
(Example)

### The special `ALL` field ###

If you would like to grant read or write access for all fields to a particular GUID, you can use the special `ALL` field instead of executing the command multiple times for each field. 
(Example)

### Default ACL ###
Whenever a new GUID is created, it is associated with a default read ACL for `ALL` fields to `ALL` GUIDs. This happens only once at the time of GUID creation. Therefore, if you would like to make some information private in your record, you are expected to specify privileges for individual fields.
(Example)

### ACL Inheritance ###

It is not necessary for you to specify ACLs explicitly for every field in every level (for nested fields) of the record. If you do not specify any ACL for a nested field, it assumes the ACL of its closest parent field with an ACL. If a nested field has only write ACL specified, it inherits the read ACL from its closest parent field having non-empty value for such an ACL. If no such parent field exists, it defaults to the `ALL` field.
(Example)

### Read ACL ###
If a particular field `X` has a read ACL, every GUID listed under it will have explicit read access to the value of `X`. This is true even if a parent field of `X` does not grant such an access.
(Example)

### Write ACL ###
If a particular field `X` has a write ACL, every GUID listed under it will have explicit write access to the value of `X`. This is true even if a parent field of `X` does not grant such an access.
(Example)

### Determining access ###
The following simple sequence of steps can be used to determine if a particular field `X` belonging to a record of GUID `R` can be read by a GUID `G`:

<p align="center">
  <img src="/assets/images/acl_algorithm.svg"/>
</p>

<!---
- Does `X` has a read ACL?
    - If yes 
        - Does it contain `G` or `ALL` GUIDs?
            - If yes, allow access
            - Otherwise, deny access
    - If not
        - Is `X` the top-most field in dotted notation?
            - If yes
                - Does the `ALL` field has a read ACL?
                    - If yes
                        - Does it contain `G` or `ALL` fields?
                            - If yes, allow access
                            - Otherwise, deny access
                    - If not
                        - Deny access
            - If not
                - Repeat these steps for the parent field of `X`
--->
(Example)


### Important note ###
1. Having no ACL at all is different from having an empty ACL. If a field has empty ACL, it is considered private and other GUIDs will not be allowed to access it. However, if no ACL is present, the default behavior is to check the parent field's ACL and then finally default to the `ALL` field's ACL.

### ACL Behavior in Groups ###

_//Add a hyperlink to groups page_

Members of a group will automatically inherit ACLs from the group GUID. Therefore, if a field has an ACL with explicit read access to a group GUID `G`, all GUIDs in `G`'s group will also have read access. This is helpful if you would like to grant access to multiple GUIDs in a similar way. For example, you can create a calendar group and put all your calendar applications (represented by their GUIDs) in this group. Granting write access for the `appointments` field in your GUID record to the group GUID will also allow all GUIDs under it to access and update your appointments.

(More detailed illustration/example)

### ACL Exceptions ###
What happens when access is denied?
When access is denied, appropriate exception is communicated to the client.
(Example)

### Coming soon: ###
- ACL behavior in Groups
- ACL operations - explanation of each operation with an example demonstrating its use in each client library.
- A link to the @ACL Reference page that provides an exhaustive listing of access types that can be set, ACL Operations and the default values.
