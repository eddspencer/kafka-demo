/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package myapps;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.javatuples.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * In this example, we we use the word count output stream as well as the plain text input stream
 * to calculate the TF-IDF vectors of the plain text sentences.
 */
public class TFIDF {

  /**
   * The entry point of application.
   *
   * @param args the input arguments 
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-tfidf");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    final StreamsBuilder builder = new StreamsBuilder();

    final AtomicLong docCount = new AtomicLong(0L);

    final KStream<String, Pair<String, Long>> docWords =
        builder.<String, String>stream("streams-plaintext-input")
            // Flatmap the sentences into words, counting sentences
            .flatMapValues(sentence -> {
              final String lower = sentence.toLowerCase(Locale.getDefault());
              final Long docNum = docCount.incrementAndGet();
              final List<Pair<String, Long>> words =
                  Arrays.stream(lower.split("\\W+")).map(word -> Pair.with(word, docNum))
                      .collect(Collectors.toList());
              return words;
            });

    docWords.groupBy((key, value) -> value.getValue0()).count().toStream()
        .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));
        
        
//        .to("streams-tfidf-output", Produced
//        .with(Serdes.String(), Serdes.serdeFrom(new PairSerializer(), new PairDeserializer())));

    //    // Document frequency - number of times word appears in document
    //    final KStream<String, Long> docFreq = builder.<String, String>stream
    //    ("streams-plaintext-input")
    //        // Flatmap the sentences into words
    //        .flatMapValues(TFIDF::toWords)
    //        // group by word
    //        .groupBy((key, value) -> value)
    //        // count words
    //        .count().toStream();
    //
    //    final KStream<String, Double> tfidf = docFreq
    //        .leftJoin(termFreq, (docF, termF) -> termF * Math.log((double) docCount.get() / docF),
    //            JoinWindows.of(Duration.ofSeconds(5)));
    
    final Topology topology = builder.build();
    final KafkaStreams streams = new KafkaStreams(topology, props);
    final CountDownLatch latch = new CountDownLatch(1);

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }

  static class PairSerializer implements Serializer<Pair<String, Long>> {
    @Override
    public byte[] serialize(String s, Pair<String, Long> pair) {
      return String.format("%s:%s", pair.getValue0(), pair.getValue1()).getBytes();
    }
  }

  static class PairDeserializer implements Deserializer<Pair<String, Long>> {

    @Override
    public Pair<String, Long> deserialize(String s, byte[] bytes) {
      final String[] values = new String(bytes).split(":");
      return Pair.with(values[0], Long.parseLong(values[1]));
    }
  }
}
