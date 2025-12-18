# frozen_string_literal: true

require_relative "lib/turso/version"

Gem::Specification.new do |spec|
  spec.name = "turso"
  spec.version = Turso::VERSION
  spec.authors = ["Turso Team"]
  spec.email = ["support@turso.tech"]

  spec.summary = "Ruby bindings for Turso embedded database"
  spec.description = "Turso is a work-in-progress, in-process OLTP database management system, compatible with SQLite."
  spec.homepage = "https://github.com/tursodatabase/turso"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/tursodatabase/turso"
  spec.metadata["changelog_uri"] = "https://github.com/tursodatabase/turso/blob/main/CHANGELOG.md"

  spec.files = Dir[
    "lib/**/*.rb",
    "ext/**/*.{rs,toml,lock}",
    "ext/**/Cargo.toml",
    "LICENSE",
    "README.md"
  ]
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/turso/extconf.rb"]

  spec.add_dependency "rb_sys", "~> 0.9"
end
