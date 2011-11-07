require 'helper'

class Person < Riaktor::Model
end

class TestRiaktor < MiniTest::Unit::TestCase
  def setup
    Riak.disable_list_keys_warnings = true
    Person.bucket.keys.each do |k|
      Person.bucket.delete(k)
    end; sleep 0.1
  end

  def test_model
    p = Person.new
    p.set "first_name", "Ben"
    p.set "last_name", "Myles"
    p.incr "cups_of_coffee", 2
    p.save

    p2 = Person.find(p.id)

    assert_equal "Ben",   p2.get("first_name")
    assert_equal "Myles", p2.get("last_name")
    assert_equal 2,       p2.get("cups_of_coffee")

    p2.set "middle_name", "Luke"
    p2.incr "cups_of_coffee", 1
    p2.save

    p3 = Person.find(p.id)

    assert_equal "Ben",   p3.get("first_name")
    assert_equal "Myles", p3.get("last_name")
    assert_equal "Luke",  p3.get("middle_name")
    assert_equal 3,       p3.get("cups_of_coffee")

    p.incr "cups_of_coffee", 1
    p.unset "first_name"
    p.save

    p4 = Person.find(p.id)

    assert_equal nil,     p4.get("first_name")
    assert_equal "Myles", p4.get("last_name")
    assert_equal "Luke",  p4.get("middle_name")
    assert_equal 4,       p4.get("cups_of_coffee")

    p.reload

    assert_equal nil,     p.get("first_name")
    assert_equal "Myles", p.get("last_name")
    assert_equal "Luke",  p.get("middle_name")
    assert_equal 4,       p.get("cups_of_coffee")
  end

  def test_document
    d = Riaktor::Document.new
    d.set :foo, "bar"
    10.times { d.incr :views, 2 }
    5.times { |i| d.push :members, "m#{i}" }

    d2 = Riaktor::Document.new
    5.times { |i| d.push :members, "m#{i+5}" }
    d2.incr :views, -3
    d2.incr :views, 1

    d.merge! d2.oplog
    res = d.document

    d2.merge! d.oplog
    res2 = d2.document

    assert_equal res, res2
  end
end
