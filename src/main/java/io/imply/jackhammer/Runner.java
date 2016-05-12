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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Runner
{
  private static final Logger log = LoggerFactory.getLogger(Runner.class);

  private final KafkaEventWriter writer;
  private final long eventsPerThread;
  private final int rate;
  private final int numThreads;
  private final int cardinality;
  private final ExecutorService executorService;
  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

  private volatile long lastReportTime;
  private volatile long lastReportCount;
  private long startTime;
  private List<LittleHammer> littleHammers = new ArrayList<>();

  public Runner(KafkaEventWriter writer, int rate, long eventsPerThread, int numThreads, int cardinality)
  {
    this.writer = writer;
    this.eventsPerThread = eventsPerThread;
    this.rate = rate;
    this.numThreads = numThreads;
    this.executorService = Executors.newFixedThreadPool(numThreads);
    this.cardinality = cardinality;
  }

  public void run() throws InterruptedException
  {
    log.info(
        String.format(
            "Threads [%d] / EventsPerThread [%d] / RatePerThread [%s events/sec]",
            numThreads,
            eventsPerThread,
            rate > 0 ? rate : "MAX"
        )
    );

    startTime = System.currentTimeMillis();

    for (int i = 0; i < numThreads; i++) {
      littleHammers.add(new LittleHammer());
    }

    scheduledExecutorService.scheduleAtFixedRate(
        new Runnable()
        {
          @Override
          public void run()
          {
            long current = System.currentTimeMillis();
            long eventCounter = writer.getMessageCount();

            log.info(
                String.format(
                    "Messages sent: [%d] / Rate: [%.2f] events/sec",
                    eventCounter,
                    (eventCounter - lastReportCount) / ((current - lastReportTime) / 1000.0)
                )
            );

            lastReportTime = current;
            lastReportCount = eventCounter;
          }
        }, 0, 10, TimeUnit.SECONDS
    );

    for (LittleHammer item : littleHammers) {
      executorService.submit(item);
    }

    // wait for tasks to finish, or wait forever, whatever, either is cool
    executorService.shutdown();
    if (executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)) {
      scheduledExecutorService.shutdown();

      writer.flush();

      log.info(
          String.format(
              "Sent [%d] messages in [%ds]",
              writer.getMessageCount(),
              (System.currentTimeMillis() - startTime) / 1000
          )
      );
    }
  }

  public void shutdown()
  {
    scheduledExecutorService.shutdown();
    executorService.shutdown();
    writer.shutdown();
  }

  private class LittleHammer implements Runnable
  {
    private long counter;

    @Override
    public void run()
    {
      while (true) {
        if (Thread.interrupted() || counter >= eventsPerThread) {
          break;
        }

        writer.write(EventGenerator.generate(cardinality));
        counter++;

        if (rate > 0) {
          try {
            Thread.sleep(1000 / rate);
          }
          catch (InterruptedException e) {
            break;
          }
        }
      }
    }
  }
}
