require "bson"

module Riaktor
  class Document
    def self.build_ts
      [Time.now.utc.to_f, BSON::ObjectId.new.to_s]
    end

    def initialize(oplog=[])
      @oplog = oplog
      build_document_from_oplog
    end

    def merge!(other_oplog)
      @oplog.concat(other_oplog)
      @oplog.uniq!
      @oplog.sort! { |x,y| x[0][0] <=> y[0][0] }
      build_document_from_oplog
    end

    def get(k)
      document[k.to_s]
    end

    def set(k, v, ts=nil)
      ts ||= self.class.build_ts
      merge! [[ts, ["set", k.to_s, v]]]
    end

    def unset(k, ts=nil)
      ts ||= self.class.build_ts
      merge! [[ts, ["unset", k.to_s]]]
    end

    def incr(k, v=1, ts=nil)
      ts ||= self.class.build_ts
      merge! [[ts, ["incr", k.to_s, v]]]
    end

    def push(k, v, ts=nil)
      ts ||= self.class.build_ts
      merge! [[ts, ["push", k.to_s, v]]]
    end

    def remove_pushed(k, v, ts=nil)
      ts ||= self.class.build_ts
      merge! [[ts, ["remove_pushed", k.to_s, v]]]
    end

    def document
      @document
    end

    def oplog
      @oplog
    end

    def ==(other_doc)
      other_doc.is_a?(Riaktor::Document) && other_doc.oplog == oplog
    end

  protected

    def build_document_from_oplog
      @document = {}
      oplog.each { |op| apply_op(op) }
      @document
    end

    def apply_op(op)
      ts, op = op

      case op[0]
      when "set" then
        op, k, v = op
        @document[k] = v
      when "unset" then
        op, k = op
        @document.delete k
      when "incr" then
        op, k, v = op
        @document[k] ||= 0
        @document[k]  += v
      when "push" then
        op, k, v = op
        @document[k] ||= []
        @document[k]  << v
      when "remove_pushed" then
        op, k, v = op
        @document[k] ||= []
        @document[k].delete v
      end

      true
    end
  end
end