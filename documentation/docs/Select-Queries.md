_// TODO: Bring uniformity in terminology. Ensure all terms are already defined in the "terminology" page beforehand._

Querying the GNS database
==============

It is possible to query the GNS database for records based on a particular property or condition just like how relational databases
can be queried. However, GNS responds only with a list of GUIDs that match a given condition instead of responding with the entire record of
each matched GUID. Precisely, GNS responds with a JSON list of GUIDs whose records match the given condition. (*subject to change?)

### Access Control ###
It has to be noted that access control is still enforced if you query the GNS database using this method instead of directly requesting a field by its name.
GUIDs for matching records will be returned only if you have read access to the field you are querying for.

_// This is a non-exhaustive list, but try to add as much information as possible_
### Syntax ###
The GNS query syntax draws several similarities from the MongoDB query syntax. Every field name in a query has to be preceded by the tilde ('~') character.
The query follows a JSON name-value syntax. You can pass a name and a value to be matched with, or you can pass a list of name and value pairs to be matched using `and` or `or`.

The general query syntax is `"~field" : value`. 
`value` can be one of the following:
- a string 
- a number 
- a condition

If a string or a number is specified, an exact match will be performed.

Ex: `"~name": "Frank"` will return a list of GUIDs whose record has a `name` field with its value __exactly__ "Frank".
    `"~age" : 30` will return a list of GUIDs whose record has an `age` field with a value of `30`

If you specify a condition instead, the condition is evaluated on the field in question.

Ex: `"~age" : {$gt: 30 }` will return a list of GUIDs whose record has an `age` field with a value greater than `30`.

The entire list of conditional operators shown below: __//TODO: Provide a cleaner explanation (generic condition format) and complete this table__

| Operator        | Description           | Example  |
|:---------------:|:---------------------:|:--------:|
| $gt        | Greater than | `"~age" : {$gt: 30 }` |
| $lt     | Less than       |  `"~age" : {$lt: 30 }`|


### Coming Soon ###
- Projection in Query
