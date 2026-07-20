# frozen_string_literal: true

require_relative "lib/turso/version"

Gem::Specification.new do |spec|
  spec.name = "turso"
  spec.version = Turso::VERSION
  spec.summary = "Ruby bindings for Turso"
  spec.authors = ["Turso Team"]
  spec.license = "MIT"
  spec.files = Dir["lib/**/*", "ext/**/*", "README.md"]
  spec.extensions = ["ext/turso_ruby/extconf.rb"]
  spec.required_ruby_version = ">= 3.0.0"
  spec.add_dependency "rb_sys", "~> 0.9"
  spec.add_development_dependency "irb"
end
