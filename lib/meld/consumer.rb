require 'stomp'
require 'json'

module Meld
  class Consumer
    def self.run
      queue = '/queue/meld-events'

      client = Stomp::Client.new 'guest', 'guest', 'localhost', 61613, true
      client.subscribe queue, { :ack => :client } do |message|
        data = JSON.parse(message.body)
        Meld::Event.create(data)
        client.acknowledge message
      end
      client.join
    end
  end
end
