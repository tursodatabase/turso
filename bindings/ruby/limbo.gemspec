# frozen_string_literal: true

require_relative "lib/limbo/version"

Gem::Specification.new do |spec|
  spec.name = "limbo"
  spec.version = Limbo::VERSION
  spec.summary = "modern evolution of SQLite"
  spec.description = "Limbo is a _work-in-progress_, in-process OLTP database engine library written in Rust"
  spec.authors = ["the Limbo authors"]
  spec.email = ["penberg@iki.fi"]

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github appveyor Gemfile])
    end
  end
  spec.homepage = "https://github.com/tursodatabase/limbo"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.0"
  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage + "/bindings/ruby"
  spec.metadata["changelog_uri"] = spec.homepage + "/bindings/ruby"

  spec.extensions = ["ext/limbo/extconf.rb"]

  # needed until rubygems supports Rust support is out of beta
  spec.add_dependency "rb_sys", "~> 0.9.39"
end
