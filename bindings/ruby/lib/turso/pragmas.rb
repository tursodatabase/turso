# frozen_string_literal: true

module Turso
  # Pragma support for Turso databases.
  # Provides a Ruby-idiomatic API for accessing and manipulating database pragmas.
  # This module is intended to be included in the {Turso::Database} class.
  module Pragmas
    # The enumeration of valid auto vacuum modes.
    AUTO_VACUUM_MODES = {
      "none" => 0,
      "full" => 1,
      "incremental" => 2
    }.freeze

    # The list of valid journaling modes.
    JOURNAL_MODES = {
      "delete" => "delete",
      "truncate" => "truncate",
      "persist" => "persist",
      "memory" => "memory",
      "wal" => "wal",
      "off" => "off"
    }.freeze

    # The list of valid locking modes.
    LOCKING_MODES = {
      "normal" => "normal",
      "exclusive" => "exclusive"
    }.freeze

    # The list of valid WAL checkpoints.
    WAL_CHECKPOINTS = {
      "passive" => "passive",
      "full" => "full",
      "restart" => "restart",
      "truncate" => "truncate"
    }.freeze


    def get_boolean_pragma(name)
      get_first_value("PRAGMA #{name}") != 0
    end

    def set_boolean_pragma(name, mode)
      execute("PRAGMA #{name}=#{normalize_boolean(mode)}")
    end

    def get_int_pragma(name)
      get_first_value("PRAGMA #{name}").to_i
    end

    def set_int_pragma(name, value)
      execute("PRAGMA #{name}=#{value.to_i}")
    end

    def get_enum_pragma(name)
      get_first_value("PRAGMA #{name}")
    end

    def set_enum_pragma(name, mode, enums)
      match = if mode.is_a?(String) || mode.is_a?(Symbol)
        enums.fetch(mode.to_s.downcase) { mode }
      else
        mode
      end

      if match.is_a?(Integer)
        execute("PRAGMA #{name}=#{match}")
      else
        execute("PRAGMA #{name}='#{match}'")
      end
    end

    def set_string_pragma(name, mode, enums)
      set_enum_pragma(name, mode, enums)
    end

    def get_query_pragma(name, *args, &block)
      sql = if args.empty?
              "PRAGMA #{name}"
            else
              "PRAGMA #{name}(#{args.map { |a| quote_pragma_arg(a) }.join(",")})"
            end
      execute(sql, &block)
    end


    def application_id;                get_int_pragma("application_id"); end
    def application_id=(v);            set_int_pragma("application_id", v); end
    def auto_vacuum;                   get_int_pragma("auto_vacuum"); end
    def auto_vacuum=(v);               set_int_pragma("auto_vacuum", v); end
    def busy_timeout;                  get_int_pragma("busy_timeout"); end
    def busy_timeout=(ms);             set_int_pragma("busy_timeout", ms); end
    def cache_size;                    get_int_pragma("cache_size"); end
    def cache_size=(v);                set_int_pragma("cache_size", v); end
    def freelist_count;                get_int_pragma("freelist_count"); end
    def max_page_count;                get_int_pragma("max_page_count"); end
    def max_page_count=(v);            set_int_pragma("max_page_count", v); end
    def mvcc_checkpoint_threshold;     get_int_pragma("mvcc_checkpoint_threshold"); end
    def mvcc_checkpoint_threshold=(v); set_int_pragma("mvcc_checkpoint_threshold", v); end
    def page_count;                    get_int_pragma("page_count"); end
    def page_size;                     get_int_pragma("page_size"); end
    def page_size=(v);                 set_int_pragma("page_size", v); end
    def schema_version;                get_int_pragma("schema_version"); end
    def schema_version=(v);            set_int_pragma("schema_version", v); end
    def user_version;                  get_int_pragma("user_version"); end
    def user_version=(v);              set_int_pragma("user_version", v); end


    def data_sync_retry;               get_boolean_pragma("data_sync_retry"); end
    def data_sync_retry=(v);           set_boolean_pragma("data_sync_retry", v); end
    def foreign_keys;                  get_boolean_pragma("foreign_keys"); end
    def foreign_keys=(v);              set_boolean_pragma("foreign_keys", v); end
    def query_only;                    get_boolean_pragma("query_only"); end
    def query_only=(v);                set_boolean_pragma("query_only", v); end



    def encoding
      "UTF-8"
    end

    def encoding=(v)
      if v.to_s.downcase != "utf-8"
        warn "[Turso] encoding is always 'UTF-8', ignoring '#{v}'"
      end
      nil
    end

    def synchronous
      get_first_value("PRAGMA synchronous").to_i
    end

    # In Turso, synchronous mode is limited to Off (0) or Full (2), essentially on/off.
    # Other values (like NORMAL) are treated as Off by the core, so we issue a warning.
    def synchronous=(v)
      if v.to_s.downcase == "normal" || v == 1
        warn "Turso synchronous='NORMAL' is not supported in Turso and is treated as 'OFF'. " \
             "Use true/false or 'FULL'/'OFF' instead."
      end
      set_boolean_pragma("synchronous", v)
    end

# always wal anyway
    def journal_mode
      "wal"
    end

    def journal_mode=(mode)
      if mode.to_s.downcase != "wal"
        warn "Turso journal_mode is always 'wal', ignoring '#{mode}'"
      end
      nil
    end


    def database_list(&block);         get_query_pragma("database_list", &block); end
    def module_list(&block);           get_query_pragma("module_list", &block); end

    def integrity_check(*args, &block)
      get_query_pragma("integrity_check", *args, &block)
    end

    def table_info(table, &block)
      columns = ["cid", "name", "type", "notnull", "dflt_value", "pk"]
      results = get_query_pragma("table_info", table)

      rows = results.map do |row|
        new_row = columns.zip(row).to_h
        tweak_default(new_row)
        new_row["type"] = new_row["type"]&.downcase
        new_row
      end

      if block_given?
        rows.each(&block)
      else
        rows
      end
    end

    def table_xinfo(table, &block)
      columns = ["cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"]
      results = get_query_pragma("table_xinfo", table)

      rows = results.map do |row|
        new_row = columns.zip(row).to_h
        tweak_default(new_row)
        new_row["type"] = new_row["type"]&.downcase
        new_row
      end

      if block_given?
        rows.each(&block)
      else
        rows
      end
    end

    def wal_checkpoint(mode = nil)
      if mode
        execute("PRAGMA wal_checkpoint(#{mode})")
      else
        execute("PRAGMA wal_checkpoint")
      end
    end

# encryption - todo: way to enable encryption perhaps
    def encryption_key=(key)
      execute("PRAGMA encryption_key='#{key}'")
    end

    def encryption_cipher=(cipher)
      execute("PRAGMA encryption_cipher='#{cipher}'")
    end

    private

    def normalize_boolean(mode)
      case mode
      when true, 1, "on", "yes", "true", "y", "t" then 1
      when false, 0, nil, "off", "no", "false", "n", "f" then 0
      else
        raise Turso::Error, "unrecognized pragma parameter #{mode.inspect}"
      end
    end

    def quote_pragma_arg(arg)
      "'#{arg.to_s.gsub("'", "''")}'"
    end

    def tweak_default(hash)
      case hash["dflt_value"]
      when /^null$/i
        hash["dflt_value"] = nil
      when /^'(.*)'$/m
        hash["dflt_value"] = $1.gsub("''", "'")
      when /^"(.*)"$/m
        hash["dflt_value"] = $1.gsub('""', '"')
      end
    end
  end
end
