# frozen_string_literal: true

require "turso/turso_ruby"

require_relative "turso/row"
require_relative "turso/result_set"
require_relative "turso/statement"
require_relative "turso/database"
require_relative "turso/transaction"
require_relative "turso/version"

module Turso
  Error = Exception
end
