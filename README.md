# Kafka talk demo


Make sure you have Kafka and Zookeeper installed, on mac:

```
brew install zookeeper
brew install kafka
```

Then run:

```
bundle
```

Start foreman, which will start Kafka/Zookeeper, and a Rails server:

```
bin/foreman start
bin/rails s
```

Then run these in separate tabs:

```
bin/rake processor:import
bin/rake processor:preprocess
bin/rake processor:aggregate
```

Then you can see the result on http://localhost:3000
