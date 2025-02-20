Gem::Specification.new do |s|
  s.name        = "limbo"
  s.version     = "0.0.0"
  s.summary     = "modern evolution of SQLite"
  s.description = "Limbo is a _work-in-progress_, in-process OLTP database engine library written in Rust"
  s.authors     = ["the Limbo authors"]
  s.email       = "penberg@iki.fi"
  s.files       = ["lib/limbo.rb"]
  s.homepage    =
    "https://rubygems.org/gems/#{s.name}"
  s.license       = "MIT"
  s.required_ruby_version = ">= 3.0"

  s.extensions = ["ext/limbo/extconf.rb"]

  # needed until rubygems supports Rust support is out of beta
  s.add_dependency "rb_sys", "~> 0.9.39"
end
