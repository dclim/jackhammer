/*
 * Copyright 2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.imply.jackhammer;

import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.regex.Pattern;

@Command(name = "jackhammer", description = "It's hammer time")
public class Main
{
  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static final Pattern brokersPattern = Pattern.compile("^([^,:\\s]+?:\\d+?)(,[^,:\\s]+?:\\d+?)*");

  @Inject
  public HelpOption helpOption;

  @Option(name = {"-b", "--brokers"},
      description = "Comma-delimited list of Kafka brokers (e.g. 'broker1:9092,broker2:9092') [required]")
  public String brokers;

  @Option(name = {"-t", "--topic"}, description = "Kafka topic to publish to (e.g. 'twitter') [required]")
  public String topic;

  @Option(name = {"-r", "--rate"}, description = "Events per second (default: max)")
  public int rate;

  @Option(name = {"-e", "--events"}, description = "Events to generate per thread (default: unlimited)")
  public long eventsPerThread;

  @Option(name = {"-n", "--threads"}, description = "Number of threads (default: 1)")
  public int numThreads = 1;

  @Option(name = {"-nl"}, description = "Number of low cardinality dimensions (default: 1)")
  public int numLowCardDims = 1;

  @Option(name = {"-nh"}, description = "Number of high cardinality dimensions (default: 0)")
  public int numHighCardDims = 0;

  @Option(name = {"-rl"}, description = "Low cardinality range (default: 10)")
  public int lowCardRange = 10;

  @Option(name = {"-rh"}, description = "High cardinality range (default: 1000000)")
  public int highCardRange = 1000000;

  @Option(name = {"-m"}, description = "Number of metric columns (default: 1)")
  public int numMetrics = 1;


  public static void main(String[] args) throws Exception
  {
    Main main = SingleCommand.singleCommand(Main.class).parse(args);

    if (main.helpOption.showHelpIfRequested()) {
      return;
    }

    main.run();
  }

  public void run() throws InterruptedException
  {
    if (brokers == null || topic == null || brokers.isEmpty() || topic.isEmpty()) {

      helpOption.help = true;
      helpOption.showHelpIfRequested();

      log.warn("Missing required parameters, aborting.");
      return;
    } else if (!brokersPattern.matcher(brokers).matches()) {
      log.warn("List of brokers must be of the form: broker1:port1,broker2:port2,broker3:port3");
      return;
    }

    log.info("Application initializing");
    final Runner runner = new Runner(
        new KafkaEventWriter(brokers, topic),
        rate,
        eventsPerThread,
        numThreads,
        numLowCardDims,
        numHighCardDims,
        lowCardRange,
        highCardRange,
        numMetrics
    );
    runner.run();

    Runtime.getRuntime().addShutdownHook(
        new Thread()
        {
          @Override
          public void run()
          {
            log.info("Shutting down...");
            runner.shutdown();
          }
        }
    );
  }
}
