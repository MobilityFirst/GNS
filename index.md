---
sidebar: doc_sidebar
type: homepage
toc: false
---

## GNS Overview

The GNS (Global Name Service) is a logically centralized server that maintains the list of mappings from keys to values. As a database, the GNS shares some semantics with a [NoSQL database](http://en.wikipedia.org/wiki/NoSQL) with each document (a key value container) having a default index which is a Globally Unique Identifier (GUID).

A GUID is similar in spirit of [Host Identity Protocol](http://en.wikipedia.org/wiki/Host_Identity_Protocol) and is a self-certifying public key with an associated human readable name. In the GNS a GUID is a 160bit self-certifying identifier derived from a public / private key pair - specifically it is the hash of the public key. GUIDs also have an associated human readable name sometimes referred to as its alias.

Certain GUIDs, called account GUIDs have an alias which must be an email address. Upon creation the email address is used to authenticate the account GUID. Account GUIDs can be used by the GUID owner to create additional GUIDs. All GUIDs can also have additional associated human readable names (alias) by which they can be referenced.

## How Can We Use the GNS?

The GNS can be a replacement for the [https://en.wikipedia.org/wiki/Domain_Name_System](DNS). Similar to GNS, DNS associates various information with user friendly names. Looking up IP addresses used to locate computer services and devices is the prominent example. Unfortunately, DNS conflates naming with location and also is subject to a variety of [security] and other issues.

A GNS based lookup service would keep the location and name of the device or service separate. It would also provide protection against spoofing of names because GUIDs are protected by the use of the private/public key.

GNS enable routing would be able to find any device or service anywhere (if it wanted to be found). The current mix of NATâ€™s, cell networks, VPNs and all the infrastructure that has been built graft mobility and security on top of the internet as well as deal with the issue of dwindling IP addresses could be replaced by a GNS internet.

If a GNS GUID were associated with every piece of content on the internet, finding, storing and distributing content could be simplified and made more efficient. One example of this is content storing routers exploiting less expensive storage and keeping data close to the place it is used.
