Samza newsfeed demo
===================

This is an experimental sample project for [Samza](http://samza.incubator.apache.org/),
demonstrating how to implement a Twitter-like real-time news feed. It uses an asymmetric
follower model, where each user sees the messages posted by all users they are following.

Each time someone follows someone, that's a *follow* event. Each time someone posts a message,
that's a *message* event. If you don't have real data, this project includes Samza jobs for
generating a random social graph and random messages, respectively.

The *fan-out* job uses Samza's
[state management](http://samza.incubator.apache.org/learn/documentation/latest/container/state-management.html)
facilities to store the list of followers for each user. Every time that user posts a message,
the fan-out job makes a copy of that message for each follower, and sends it to the follower
on a separate *deliveries* stream. A downstream job then groups together all the messages
that need to be delivered to the same user (called a *home timeline*, again implemented using
Samza state).


How to run
----------

Start YARN, Zookeeper and Kafka through
[Hello Samza](http://samza.incubator.apache.org/startup/hello-samza/latest/)'s
`bin/grid start all` script.

This job depends on Samza 0.8.0, which is not yet released. So you need to build it from source
and put it in your local Maven repository, as follows:

```bash
git clone https://github.com/apache/incubator-samza samza
cd samza
./gradlew publishToMavenLocal
```

Then you can build and run the newsfeed jobs as follows:

* `mvn clean package && rm -rf deploy && mkdir -p deploy && tar xzf target/newsfeed-0.0.1-dist.tar.gz -C deploy`
* `deploy/bin/run-job.sh --config-path=file://$PWD/deploy/config/newsfeed-fan-out.properties`
* `deploy/bin/run-job.sh --config-path=file://$PWD/deploy/config/newsfeed-home-timeline.properties`
* `deploy/bin/run-job.sh --config-path=file://$PWD/deploy/config/newsfeed-generate-follows.properties`
  (if necessary; automatically terminates after creating a social graph of a certain size)
* `deploy/bin/run-job.sh --config-path=file://$PWD/deploy/config/newsfeed-generate-messages.properties`
  (if necessary; keeps running and continually generates random messages)

You can use Hello Samza's kafka-console-consumer to inspect the output, e.g. the
`newsfeed-home-timeline-changelog` topic.


Meta
----

This is experimental code, not fit for production use. It is released under the terms of the MIT license.
