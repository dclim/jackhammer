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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaEventWriter
{
  private static final Logger log = LoggerFactory.getLogger(KafkaEventWriter.class);

  private final KafkaProducer<String, String> producer;
  private final String topic;

  private volatile long messageCount;

  public KafkaEventWriter(String brokers, String topic)
  {
    this.topic = topic;

    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("acks", "0");
    props.put("batch.size", 100000);

    this.producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
  }

  public void write(String event)
  {
    try {
      producer.send(
          new ProducerRecord<String, String>(topic, event), new Callback()
          {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception)
            {
              messageCount++;
            }
          }
      );
    }
    catch (Exception e) {
      log.error("Exception while sending event to Kafka: ", e);
    }
  }

  public long getMessageCount()
  {
    return messageCount;
  }

  public void shutdown()
  {
    log.info("Shutting down Kafka producer");
    producer.close();
  }

  public void flush()
  {
    producer.flush();
  }
}
