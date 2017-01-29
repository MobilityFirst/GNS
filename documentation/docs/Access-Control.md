// TODO: Bring uniformity in terminology. Ensure all terms are already defined in the "terminology" page.
// Page still under construction

//Why are ACLs necessary?
GNS is not just intended to be used to read and write your own data. Often, other trusted applications or users (represented by GUIDs) may want to write data on your behalf. For example, a calendar application may want to write new appointments for you. Other GUIDs may also want to read data that you do not want to be visible to the whole world. For example, your fitness application may want to read your activity data to generate reports.
ACLs enable you to specify appropriate permissions for other GUIDs to access your data.

//What exactly are ACLs?
Access control lists or ACLs are a list of permissions associated with a particular field of a GNS record. Every ACL can be a whitelist or a blacklist. Further, each whitelist or blacklist can be used to control read or write access. Therefore, for every field, four separate lists can be specified: read whitelist, read blacklist, write whitelist and write blacklist.

//It should be implied that ACLs apply only to "other" GUIDs, not to self (GUID to which the field record belongs)
If an ACL for a particular field X is a read whitelist, every GUID listed under this ACL will have explicit read access to the value of X. This is true even if a parent field of X in dotted notation has an ACL preventing such an access. (Add example 1). If any sub-field of X, say Y, has a read blacklist ACL for a GUID already under the read whitelist of X, that GUID will be unable to read the value of Y. (Add example 2). The same holds good for write whitelist.

If an ACL for a particular field X is a read blacklist, every GUID listed under this ACL will be prevented explicitly from reading the value of X, even if X has an ACL that grants read access to everyone. This is true even if a parent field of X in dotted notation has an ACL allowing such an access. (Add example 3). If any sub-field of X, say Y, has a read whitelist ACL for a GUID already under the read blacklist of X, that GUID will still be able to read the value of Y. (Add example 4). The same holds good for write blacklist.

ACL Inheritance
It is not necessary to specify ACLs for every field in a record. If any field is not associated with an ACL, it inherits ACLs from its parent field. For example, if a field Y has only write blacklist associated with it, it inherits read whitelist, read blacklist and write whitelist from its parent field.
(Add example 5)

Default ACL
When a field is created in a record and no ACL is specified, it will be associated with the following default ACLs:
read whitelist: ALL GUIDs
write whitelist: Account GUID
read blacklist: <empty>
write blacklist: <empty>
//Verify if this is true even if parent has prohibitive access, like blacklist ALL
(Add example 6)

The following simple sequence of steps can be used to determine if a particular field X can be read by GUID G:

- If G or ALL is present in read blacklist of X, deny access.
- If G or ALL is present in read whitelist of X or if G is the account GUID of X, allow access.
- Repeat the steps for ACLs of parent field of X until access is allowed or denied.

(Add example 7)

//Cases where a GUID is in both whitelist and blacklist?
//Cases where the record GUID (itself) is in blacklist?
//Cases where account GUID is blacklisted by parent field?

//What happens when a value is inaccessible?
No exception is raised (//Verify). The behavior of GNS will be as if the inaccessible field never exists in the record.

- ACL operations - explanation of each operation with an example demonstrating its use in each client library.
- A link to the @ACL Reference page that provides an exhaustive listing of access types that can be set, ACL Operations and the default values.
