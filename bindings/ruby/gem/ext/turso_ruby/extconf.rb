# frozen_string_literal: true

require "mkmf"
require "rb_sys"

create_rust_makefile("turso_ruby/turso_ruby") do |r|
  r.auto_install_rust = true
end
