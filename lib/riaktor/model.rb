module Riaktor
  class Model

    def self.bucket_name
      self.to_s.tableize
    end

    def self.reset_client
      @client = nil
      @bucket = nil
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

    def self.find(id, opts={}, resolve_opts={})
      robj = resolve_any_conflicts self.bucket.get(id, opts), resolve_opts
      new id, robj
    end

    def self.resolve_any_conflicts(robj, resolve_opts={})
      return robj unless robj && robj.conflict?

      resolved_robj = robj.siblings.first.dup
      resolved_robj.content_type = "application/json"

      document = Riaktor::Document.new(resolved_robj.data)

      robj.siblings[1..-1].each do |sibling|
        document.merge! sibling.data
      end

      resolved_robj.data = document.oplog
      resolved_robj.store(resolve_opts)

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

    def save(opts={})
      self.robj.data = document.oplog
      self.robj.store(opts)
    end

    def reload(opts={})
      @document = nil
      self.robj = self.class.find(self.id, opts).robj
      true
    end

    %w(get set unset incr push remove_pushed).each do |meth|
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

    def ==(other)
      other.is_a?(self.class) && to_hash == other.to_hash
    end

  end
end