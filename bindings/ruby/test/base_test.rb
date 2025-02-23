require "minitest/autorun"
require "limbo"

describe Limbo do
  it "has a version number" do
    expect(Limbo::CORE_VERSION.class).must_equal String
  end
end
