# frozen_string_literal: true

module Turso
  class DB < Database
    def initialize(path = ":memory:", **options)
      warn "Turso::DB is deprecated; use Turso::Database"
      options[:experimental_features] = normalize_experimental_features(options[:experimental_features])
      super
    end

    private

    def normalize_experimental_features(features)
      return nil if features.nil? || features.empty?

      Array(features).join(",")
    end
  end
end
