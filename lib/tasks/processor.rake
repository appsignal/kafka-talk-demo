namespace :processor do
  task :import => :environment do
    Dir.glob('log/access/*') do |file|
      File.read(file).lines.each do |line|
        puts line
        begin
          $kafka.deliver_message(
            line,
            topic: 'raw_page_views'
          )
        rescue Kafka::LeaderNotAvailable
          # This happens the first time we produce sometimes
        end
      end
    end
    puts 'Imported all available logs in log/accesss'
  end

  task :preprocess => :environment do
    puts 'Started processor'

    log_line_regex = %r{^(\S+) - - \[(\S+ \+\d{4})\] "(\S+ \S+ [^"]+)" (\d{3}) (\d+|-) "(.*?)" "([^"]+)"$}
    geoip = GeoIP.new('GeoLiteCity.dat')

    consumer = $kafka.consumer(group_id: 'preprocesser')
    consumer.subscribe('raw_page_views')

    consumer.each_message do |message|
      # We've received a message, parse the log line
      log_line = message.value.split(log_line_regex)

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
      $kafka.deliver_message(
        page_view.to_json,
        topic: 'page_views',
        partition_key: city.country_code2 # MAGIC HERE
      )
    end
  end

  task :aggregate => :environment do
    consumer = $kafka.consumer(group_id: 'aggregator')
    consumer.subscribe('page_views')

    @count = 0
    @country_counts = Hash.new(0)
    @last_tick_time = Time.now.to_i

    # Consume and aggregate all messages. Update the database with
    # the new counts every 10 seconds.
    consumer.each_message do |message|
      page_view = JSON.parse(message.value)

      @count += 1
      @country_counts[page_view['country']] += 1

      now_time = Time.now.to_i
      # Run this code every 5 seconds
      if @last_tick_time + 5 < now_time
        # Print current state
        puts "#{Time.now}: Running for #{@country_counts.map(&:first)}"

        # Update stats in the database
        CountryStat.update_country_counts(@country_counts)

        # Clear aggregation
        @count = 0
        @country_counts.clear

        @last_tick_time = now_time
      end
    end
  end
end
