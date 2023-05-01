package com.meroxa.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // Build Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("servers")
                .setTopics("topics")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        // Provide the source to your DataStream
        DataStream<String> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        // Create sink to write to
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("brokers")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("topic")
                                .setKafkaValueSerializer(StringSerializer.class)
                                .build()
                )
                .build();

        // connect input to sink (with no modification between the two)
        input.sinkTo(sink);

        // Execute program, beginning computation.
        env.execute("Flink Java Application");
    }
}
