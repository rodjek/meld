require 'mongo'
require 'stomp'
require 'json'

module Meld
  class Event
    def self.db
      @db ||= Mongo::Connection.new.db('meld')
      @coll ||= @db.collection('events')
    end

    def self.create(object)
      time = object['created_at'] || Time.now.utc
      db.save(object.merge('created_at' => time))
    end

    def self.find(conditions = {}, options = {})
      default_conditions = {
      # probably going to want to default to just showing the last
      # 24 hours here
      }
      default_options = {
        :sort => [['created_at', 'descending']],
      }

      db.find(default_conditions.merge(conditions),
              default_options.merge(options)).to_a
    end

    def self.emit(object)
      data = object.respond_to?(:to_json) ? object.to_json : object.to_s
      client = Stomp::Client.new 'guest', 'guest', 'localhost', 61613, true
      client.send('/queue/meld-events', data, {:persistent => true})
      client.close
    end
  end
end
