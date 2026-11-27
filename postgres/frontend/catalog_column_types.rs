//! The columns of the catalog tables whose declared type stands for the type
//! PostgreSQL gives them. postgres/conformance/tools/gen_catalog_column_types.py
//! writes this file.

/// FIXME(registered-types): The catalog tables declare SQLite types, so only these columns
/// are typed by their declared type. Catalog columns of registered types replace this list.
#[rustfmt::skip]
pub(crate) const COLUMNS_OF_THEIR_POSTGRES_TYPE: &[(&str, &[&str])] = &[
    ("pg_am", &["oid", "amname", "amtype"]),
    ("pg_amop", &["oid", "amopfamily", "amoplefttype", "amoprighttype", "amopstrategy", "amoppurpose", "amopopr", "amopmethod", "amopsortfamily"]),
    ("pg_amproc", &["oid", "amprocfamily", "amproclefttype", "amprocrighttype", "amprocnum"]),
    ("pg_attrdef", &["oid", "adrelid", "adnum"]),
    ("pg_attribute", &["attrelid", "attname", "atttypid", "attstattarget", "attlen", "attnum", "attndims", "atttypmod", "attbyval", "attstorage", "attalign", "attcompression", "attnotnull", "atthasdef", "atthasmissing", "attidentity", "attgenerated", "attisdropped", "attislocal", "attinhcount", "attcollation"]),
    ("pg_auth_members", &["oid", "roleid", "member", "grantor", "admin_option", "inherit_option", "set_option"]),
    ("pg_cast", &["oid", "castsource", "casttarget", "castfunc", "castcontext", "castmethod"]),
    ("pg_class", &["oid", "relname", "relnamespace", "reltype", "reloftype", "relowner", "relam", "relfilenode", "reltablespace", "relpages", "reltuples", "relallvisible", "reltoastrelid", "relhasindex", "relisshared", "relpersistence", "relkind", "relnatts", "relchecks", "relhasrules", "relhastriggers", "relhassubclass", "relrowsecurity", "relforcerowsecurity", "relispopulated", "relreplident", "relispartition", "relrewrite"]),
    ("pg_collation", &["oid", "collname", "collnamespace", "collowner", "collprovider", "collencoding", "collcollate", "collctype", "collicurules", "collversion"]),
    ("pg_constraint", &["oid", "conname", "connamespace", "contype", "condeferrable", "condeferred", "convalidated", "conrelid", "contypid", "conindid", "conparentid", "confrelid", "confupdtype", "confdeltype", "confmatchtype", "conislocal", "coninhcount", "connoinherit"]),
    ("pg_conversion", &["oid", "conname", "connamespace", "conowner", "conforencoding", "contoencoding", "condefault"]),
    ("pg_database", &["oid", "datname", "datdba", "encoding", "datlocprovider", "datistemplate", "datallowconn", "datconnlimit", "dattablespace", "datcollate", "datctype", "daticurules"]),
    ("pg_default_acl", &["oid", "defaclrole", "defaclnamespace", "defaclobjtype"]),
    ("pg_depend", &["classid", "objid", "objsubid", "refclassid", "refobjid", "refobjsubid", "deptype"]),
    ("pg_event_trigger", &["oid", "evtname", "evtevent", "evtowner", "evtfoid", "evtenabled", "evttags"]),
    ("pg_extension", &["oid", "extname", "extowner", "extnamespace", "extrelocatable", "extversion", "extcondition"]),
    ("pg_foreign_data_wrapper", &["oid", "fdwname", "fdwowner", "fdwhandler", "fdwvalidator", "fdwoptions"]),
    ("pg_foreign_server", &["oid", "srvname", "srvowner", "srvfdw", "srvtype", "srvversion", "srvoptions"]),
    ("pg_foreign_table", &["ftrelid", "ftserver"]),
    ("pg_index", &["indexrelid", "indrelid", "indnatts", "indnkeyatts", "indisunique", "indnullsnotdistinct", "indisprimary", "indisexclusion", "indimmediate", "indisclustered", "indisvalid", "indcheckxmin", "indisready", "indislive", "indisreplident"]),
    ("pg_indexes", &["schemaname", "tablename", "indexname", "tablespace", "indexdef"]),
    ("pg_inherits", &["inhrelid", "inhparent", "inhseqno"]),
    ("pg_init_privs", &["objoid", "classoid", "objsubid", "privtype"]),
    ("pg_language", &["oid", "lanname", "lanowner", "lanispl", "lanpltrusted", "lanplcallfoid", "laninline", "lanvalidator"]),
    ("pg_largeobject_metadata", &["oid", "lomowner"]),
    ("pg_namespace", &["oid", "nspname", "nspowner"]),
    ("pg_opclass", &["oid", "opcmethod", "opcname", "opcnamespace", "opcowner", "opcfamily", "opcintype", "opcdefault", "opckeytype"]),
    ("pg_operator", &["oid", "oprname", "oprnamespace", "oprowner", "oprkind", "oprcanmerge", "oprcanhash", "oprleft", "oprright", "oprresult", "oprcom", "oprnegate"]),
    ("pg_opfamily", &["oid", "opfmethod", "opfname", "opfnamespace", "opfowner"]),
    ("pg_partitioned_table", &["partrelid", "partstrat", "partnatts", "partdefid"]),
    ("pg_policies", &["schemaname", "tablename", "policyname", "permissive", "roles", "cmd", "qual", "with_check"]),
    ("pg_policy", &["oid", "polname", "polrelid", "polcmd", "polpermissive"]),
    ("pg_proc", &["oid", "proname", "pronamespace", "proowner", "prolang", "procost", "prorows", "provariadic", "prokind", "prosecdef", "proleakproof", "proisstrict", "proretset", "provolatile", "proparallel", "pronargs", "pronargdefaults", "prorettype", "prosrc", "probin"]),
    ("pg_publication", &["oid", "pubname", "pubowner"]),
    ("pg_publication_namespace", &["oid", "pnpubid", "pnnspid"]),
    ("pg_publication_rel", &["oid", "prpubid", "prrelid"]),
    ("pg_range", &["rngtypid", "rngsubtype", "rngmultitypid", "rngcollation", "rngsubopc"]),
    ("pg_rewrite", &["oid", "rulename", "ev_class", "ev_type", "ev_enabled"]),
    ("pg_roles", &["oid", "rolname", "rolconnlimit", "rolpassword"]),
    ("pg_seclabels", &["objoid", "classoid", "objsubid", "objtype", "objnamespace", "objname", "provider", "label"]),
    ("pg_sequence", &["seqrelid", "seqtypid", "seqstart", "seqincrement", "seqmax", "seqmin", "seqcache", "seqcycle"]),
    ("pg_sequences", &["schemaname", "sequencename", "sequenceowner", "start_value", "min_value", "max_value", "increment_by", "cache_size", "last_value"]),
    ("pg_settings", &["name", "setting", "unit", "category", "short_desc", "extra_desc", "context", "vartype", "source", "min_val", "max_val", "enumvals", "boot_val", "reset_val", "sourcefile", "sourceline", "pending_restart"]),
    ("pg_statistic_ext", &["oid", "stxrelid", "stxname", "stxnamespace", "stxowner", "stxstattarget"]),
    ("pg_subscription", &["oid", "subdbid", "subname", "subowner", "subenabled", "subbinary", "substream", "subtwophasestate", "subdisableonerr", "subpasswordrequired", "subrunasowner", "subconninfo", "subslotname", "subsynccommit", "subpublications", "suborigin"]),
    ("pg_tables", &["schemaname", "tablename", "tableowner", "tablespace"]),
    ("pg_tablespace", &["oid", "spcname", "spcowner", "spcoptions"]),
    ("pg_transform", &["oid", "trftype", "trflang"]),
    ("pg_trigger", &["oid", "tgrelid", "tgname", "tgfoid", "tgtype", "tgenabled", "tgconstrrelid", "tgconstrindid", "tgconstraint", "tgnargs", "tgoldtable", "tgnewtable", "tgparentid"]),
    ("pg_ts_config", &["oid", "cfgname", "cfgnamespace", "cfgowner", "cfgparser"]),
    ("pg_ts_dict", &["oid", "dictname", "dictnamespace", "dictowner", "dicttemplate", "dictinitoption"]),
    ("pg_ts_parser", &["oid", "prsname", "prsnamespace"]),
    ("pg_ts_template", &["oid", "tmplname", "tmplnamespace"]),
    ("pg_type", &["oid", "typname", "typnamespace", "typowner", "typlen", "typbyval", "typtype", "typcategory", "typispreferred", "typisdefined", "typdelim", "typrelid", "typelem", "typarray", "typalign", "typstorage", "typnotnull", "typbasetype", "typtypmod", "typndims", "typcollation", "typdefault"]),
];

/// Whether `column` of the catalog table that `create_sql` creates holds
/// values of the type PostgreSQL gives the column.
pub(crate) fn has_postgres_type(create_sql: &str, column: &str) -> bool {
    let table = create_sql
        .trim_start()
        .trim_start_matches("CREATE TABLE")
        .split(|c: char| c == '(' || c.is_whitespace())
        .find(|word| !word.is_empty());
    COLUMNS_OF_THEIR_POSTGRES_TYPE
        .iter()
        .find(|(name, _)| Some(*name) == table)
        .is_some_and(|(_, columns)| columns.contains(&column))
}
