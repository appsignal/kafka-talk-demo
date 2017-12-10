KAFKA_CONFIG = {
  :"bootstrap.servers" => "localhost:9092",
  :"enable.auto.commit" => false,
  :"enable.partition.eof" => false
}

namespace :processor do
  task :create_topics do
    `kafka-topics --create --topic=raw-page-views --zookeeper=127.0.0.1:2181 --partitions=8 --replication-factor=1`
    `kafka-topics --create --topic=page-views --zookeeper=127.0.0.1:2181 --partitions=8 --replication-factor=1`
  end

  task :import => :environment do
    puts "Start importing"

    # Create  producer
    producer = Rdkafka::Config.new(KAFKA_CONFIG).producer

    # Loop through all logs and produce messages
    Dir.glob('log/access/*') do |file|
      delivery_handles = []
      File.read(file).lines.each do |line|
        puts line
        handle = producer.produce(
          payload: line,
          topic: 'raw-page-views'
        )
        delivery_handles.push(handle)
      end
      puts "Produced lines in #{file}, waiting for delivery"
      delivery_handles.each(&:wait)
    end

    puts 'Imported all available logs in log/accesss'
  end

  task :preprocess => :environment do
    puts 'Started processor'

    # Set up log parsing and geoip
    log_line_regex = %r{^(\S+) - - \[(\S+ \+\d{4})\] "(\S+ \S+ [^"]+)" (\d{3}) (\d+|-) "(.*?)" "([^"]+)"$}
    geoip = GeoIP.new('GeoLiteCity.dat')

    # Create  producer and consumer
    producer = Rdkafka::Config.new(KAFKA_CONFIG).producer
    consumer = Rdkafka::Config.new(KAFKA_CONFIG.merge(:"group.id" => "preprocessor")).consumer
    consumer.subscribe("raw-page-views")

    # To keep track of our last tick
    @last_tick_time = Time.now.to_i

    delivery_handles = []
    consumer.each do |message|
      # We've received a message, parse the log line
      log_line = message.payload.split(log_line_regex)

      city = geoip.city(log_line[1])
      next unless city

      user_agent = UserAgent.parse(log_line[7])
      next unless user_agent

      url = log_line[3].split[1]

      # Convert it to an intermediary format
      page_view = {
        'time' => log_line[2],
        'ip' => log_line[1],
        'country' => city.country_name,
        'browser' => user_agent.browser,
        'url' => url
      }

      puts page_view

      # Write it to a topic
      handle = producer.produce(
        key: city.country_code2, # MAGIC HERE
        payload: page_view.to_json,
        topic: "page-views"
      )
      delivery_handles.push(handle)

      now_time = Time.now.to_i
      # Run this code every 5 seconds
      if @last_tick_time + 5 < now_time
        puts "Waiting for delivery and comitting consumer"

        # Wait for delivery of all messages
        delivery_handles.each(&:wait)
        delivery_handles.clear

        # Commit consumer position
        consumer.commit

        # Sleep so we can see the tick in the console
        sleep 2

        @last_tick_time = now_time
      end
    end
  end

  task :aggregate => :environment do
    # Create consumer
    consumer = Rdkafka::Config.new(KAFKA_CONFIG.merge(:"group.id" => "processor")).consumer
    consumer.subscribe("page-views")

    # Set up in-memory storage
    @count = 0
    @country_counts = Hash.new(0)

    # To keep track of our last tick
    @last_tick_time = Time.now.to_i

    # Consume and aggregate all messages. Update the database with
    # the new counts every 10 seconds.
    consumer.each do |message|
      page_view = JSON.parse(message.payload)

      @count += 1
      @country_counts[page_view['country']] += 1

      now_time = Time.now.to_i
      # Run this code every 5 seconds
      if @last_tick_time + 5 < now_time
        # Print current state
        puts "#{Time.now}: Running for #{@country_counts.map(&:first)}"

        # Update stats in the database
        CountryStat.update_country_counts(@country_counts)

        # Commit consumer position
        consumer.commit

        # Clear aggregation
        @count = 0
        @country_counts.clear

        @last_tick_time = now_time
      end
    end
  end
end
