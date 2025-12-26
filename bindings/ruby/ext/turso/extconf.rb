# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("turso/turso") do |r|
  r.features = ["extension-module"]
  r.extra_rustflags = ["--cfg", "rb_sys_gem"]
end
