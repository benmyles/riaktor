module Riaktor
  class Model

    def self.bucket_name
      self.to_s.tableize
    end

    def self.client
      @client ||= Riak::Client.new(http_backend: :Excon, http_port: 8091)
    end

    def self.bucket
      @bucket ||= begin
        b = Riak::Bucket.new(self.client, self.bucket_name)
        b.allow_mult = true; b
      end
    end

    def self.find(id)
      robj = resolve_any_conflicts self.bucket.get(id)
      new id, robj
    end

    def self.resolve_any_conflicts(robj)
      return robj unless robj && robj.conflict?

      resolved_robj = robj.siblings.first.dup
      resolved_robj.content_type = "application/json"

      document = Riaktor::Document.new(resolved_robj.data)

      robj.siblings[1..-1].each do |sibling|
        document.merge! sibling.data
      end

      resolved_robj.data = document.oplog
      resolved_robj.store

      resolved_robj
    end

    attr_accessor :id, :robj

    def initialize(id=BSON::ObjectId.new.to_s, robj=nil)
      self.id   = id
      self.robj = robj || self.class.bucket.new(id)

      self.robj.content_type = "application/json"
      self.robj.data ||= []
    end

    def document
      @document ||= Riaktor::Document.new(self.robj.data)
    end

    def save
      self.robj.data = document.oplog
      self.robj.store
    end

    def reload
      @document = nil
      self.robj = self.class.find(self.id).robj
      true
    end

    %w(get set unset incr push).each do |meth|
      define_method(meth) do |*args|
        document.send(meth, *args)
      end
    end

    def to_hash
      document.document
    end

    def to_json
      to_hash.to_json
    end

  end
end