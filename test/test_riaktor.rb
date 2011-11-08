require 'helper'

class Person < Riaktor::Model
  def add_friend(friend)
    push "friend_ids", friend.id
    save
  end

  def remove_friend(friend)
    remove_pushed "friend_ids", friend.id
    save
  end

  def friend_ids
    get("friend_ids") || []
  end

  def friends
    all = []; each_friend { |f| all << f }; all
  end

  def each_friend
    friend_ids.each do |friend_id|
      yield Person.find(friend_id)
    end
  end
end

class TestRiaktor < MiniTest::Unit::TestCase
  def setup
    Riak.disable_list_keys_warnings = true
    Person.bucket.keys.each do |k|
      Person.bucket.delete(k, {r: 3, w: 3, dw: 3})
    end
  end

  def test_threaded_counters
    p = Person.new
    p.incr "counter", 1
    p.save
    p.reload
    assert_equal 1, p.get("counter")

    threads = []

    19.times do
      threads << Thread.new do
        person = Person.find(p.id)
        person.incr "counter", 1
        person.save({r: 3, w: 3, dw: 3})
      end
    end

    threads.each { |th| th.join }

    p.reload
    assert_equal 20, p.get("counter")
  end

  def test_counter_conflicts
    p = Person.new
    p.incr "counter", 1
    p.save
    p.reload
    assert_equal 1, p.get("counter")

    people = []
    29.times { people << Person.find(p.id) }

    people.each do |person|
      person.incr "counter", 1
      person.save
    end

    p.reload
    assert_equal 30, p.get("counter")
  end

  def test_relationships_in_model
    ben = Person.new
    ben.set "first_name", "Ben"
    ben.save

    chris = Person.new
    chris.set "first_name", "Chris"
    chris.save

    jos = Person.new
    jos.set "first_name", "Jos"
    jos.save

    ben.add_friend chris
    assert_equal [chris], ben.friends
    ben.add_friend jos
    assert_equal [chris, jos], ben.friends
    ben.reload
    assert_equal [chris, jos], ben.friends

    ben.remove_friend chris
    assert_equal [jos], ben.friends
    ben.reload
    assert_equal [jos], ben.friends

    ben.add_friend chris
    assert_equal [jos, chris], ben.friends
    ben.reload
    assert_equal [jos, chris], ben.friends
  end

  def test_conflict_resolution_in_model
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

    p.push "friend_ids", "100"
    p.push "friend_ids", "101"
    p.push "friend_ids", "102"

    p.save

    p4.remove_pushed "friend_ids", "101"
    p4.save

    p.push "friend_ids", "103"
    p.save

    p.reload
    assert_equal %w(100 102 103), p.get("friend_ids")

    p4.reload
    assert_equal %w(100 102 103), p4.get("friend_ids")
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
