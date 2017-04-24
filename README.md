# Kafka talk demo


Make sure you have Kafka and Zookeeper installed, on mac:

```
brew install zookeeper
brew install kafka
```

Then run:

```
bundle
bin/rake db:migrate
```

Start foreman, which will start Kafka/Zookeeper, and a Rails server:

```
bin/foreman start
bin/rails s
```

You need some access log files that we can process. Download some from a
server you run into `log/access`.

Then run these in separate tabs:

```
bin/rake processor:import
bin/rake processor:preprocess
bin/rake processor:aggregate
```

Then you can see the result on http://localhost:3000
