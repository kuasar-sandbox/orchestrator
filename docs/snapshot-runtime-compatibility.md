# Snapshot runtime reference compatibility

An SNP template may carry a terminal
`@runtime:<basename>:digest:<64-lowercase-hex>` on its snapshot reference. It uses
the existing `<profile>-<kind>-<base64url(ref)>` TemplateID encoding. There is no
additional segment or separate template catalog. IMG and SBX templates do not
carry a snapshot runtime declaration.

Valid legacy SNP references remain accepted and retain their previous scheduling
and Node restore checks. They cannot receive runtime prefiltering from absent
metadata and may still fail compatibility at actual restore. A present malformed
declaration is rejected instead of being treated as a legacy reference. The
declaration does not replace Node runtime validation or require fetching an
artifact to obtain scheduling information.

The base reference budget stays at 256 bytes. A runtime basename may use at most
255 bytes; its complete suffix uses at most 336 bytes. The maximum annotated
reference is therefore 592 bytes and the maximum TemplateID is 799 bytes.
Routes, migration tokens, retained identities and publication keep the full
TemplateID. Build consumes the existing `snapshot --json` capture result and
passes the returned reference to publication, preserving the captured runtime
declaration instead of reconstructing a result from a filename.

The Proxy route mmap schema is 10. Its primary record grows from 2664 to 3112
bytes; the default 65536-slot mapping adds 28 MiB. Mapping size is not resident
memory usage. Existing schema checks reject older mappings; the Proxy master
recreates the route view and resynchronizes it through the existing lifecycle.
Upgrade the master and its workers together, as required for previous schema
changes.

Deploy compatible SDKs and actual consumer binaries before producers emit new
references. A new reader accepting old inputs does not make an old reader capable
of accepting new outputs. This consumer change does not enable annotated capture
production or impose a new rejection policy on legacy SNP templates.
