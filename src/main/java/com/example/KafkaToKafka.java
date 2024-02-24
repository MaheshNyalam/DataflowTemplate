/*/*from  w  w w  .  j  ava 2  s . co  m*/
/* Licensed to the Apache Software Foundation (ASF) under one
		* or more contributor license agreements.  See the NOTICE file
		* distributed with this work for additional information
		* regarding copyright ownership.  The ASF licenses this file
		* to you under the Apache License, Version 2.0 (the
		* "License"); you may not use this file except in compliance
		* with the License.  You may obtain a copy of the License at
		*
		*     http://www.apache.org/licenses/LICENSE-2.0
		*
		* Unless required by applicable law or agreed to in writing, software
		* distributed under the License is distributed on an "AS IS" BASIS,
		* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
		* See the License for the specific language governing permissions and
		* limitations under the License.
		*/

package com.example;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToKafka {

	private static final Logger LOG = LoggerFactory
			.getLogger(KafkaToKafka.class);

	/**
	 * Specific pipeline options.
	 */
	public interface KafkaToKafkaOptions extends PipelineOptions {

		@Description("Kafka bootstrap server address")
		@Default.String("localhost:9092")
		String getBootstrapServer();
		void setBootstrapServer(String bootstrapServer);

		@Description("Kafka Topic Name")
		@Default.String("test-topic")
		String getInputTopic();
		void setInputTopic(String inputTopic);

		@Description("Kafka Output Topic Name")
		@Default.String("test-output")
		String getOutputTopic();
		void setOutputTopic(String outputTopic);

		@Description("bq table project name")
		@Default.String("mahesh-8573956365")
		String getProjectName();
		void setProjectName(String projectName);

		@Description("bq table dataset name")
		@Default.String("kafka_output")
		String getDatasetName();
		void setDatasetName(String datasetName);

		@Description("reference table name")
		@Default.String("reference")
		String getReferenceTable();
		void setReferenceTable(String referenceTable);

//		Output Topics declaration
		String TXOutputTopic = "TX_topic";
		String AMOutputTopic = "AM_Topic";


		class GDELTFileFactory implements DefaultValueFactory<String> {
			public String create(PipelineOptions options) {
				SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
				return format.format(new Date());
			}
		}
	}


	public static void main(String[] args) throws Exception {
		KafkaToKafkaOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation().as(KafkaToKafkaOptions.class);
		LOG.info(options.toString());
		System.out.println(options.toString());
		run(options);
//		Pipeline pipeline = Pipeline.create(options);
	}


	public static PipelineResult run(KafkaToKafkaOptions options) {
		Pipeline pipeline = Pipeline.create(options);
		LOG.info("Starting pipeline");

		// bq side input
		PCollectionView<Map<String, String>> bqSideInputView = pipeline
				.apply("Read data from bq",
						BigQueryIO.readTableRows()
								.fromQuery("SELECT Facility, Division FROM `%s.%s.%s` group by Facility, Division".formatted(options.getProjectName(),options.getDatasetName(),
										options.getReferenceTable()))
								.usingStandardSql())
				.apply("TableRow to KV", ParDo.of(new DoFn<TableRow, KV<String, String>>() {
					@ProcessElement
					public void processElement(@Element TableRow row, OutputReceiver<KV<String, String>> receiver)
							throws Exception {
						KV<String, String> kv = KV.of((String) row.get("Facility"), (String) row.get("Division"));
						receiver.output(kv);
					}
				}))
				.apply(View.asMap());



        // Read data from kafka input topic
		PCollection<KV<String, String>> input = pipeline.apply(
				"ReadFromKafka",
				KafkaIO.<String, String> read()
						.withBootstrapServers(options.getBootstrapServer())
						.withTopics(
								Collections.singletonList(options
										.getInputTopic()))
						.withKeyDeserializer(StringDeserializer.class)
						.withValueDeserializer(StringDeserializer.class)
						.withConsumerConfigUpdates(ImmutableMap.of(
								ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
								ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-latest",
								ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true"))
						.withoutMetadata());
//						apply("ExtractPayload",
//				// The Values.create() method extracts the values from the Kafka records read from the topic.
//				Values.<String> create());

		input.apply(ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				System.out.printf("** element |%s| **%n",
						c.element());
			}
		}));



		PCollection<KV<String, KV<String,String>>> taggedOutput =
				input.apply("Tag the record with Division",ParDo
						.of(new DoFn<KV<String, String>, KV<String, KV<String,String>>>() {
							@ProcessElement
							public void processElement(ProcessContext c) {
								KV<String, String> message = c.element();
								Map<String, String> bqSideInput = c.sideInput(bqSideInputView);
								System.out.printf("** sideInput |%s| **%n",
										bqSideInput);
								try{
									//Retrieving key and value from the message
									String key = message.getKey();

									//Raising the exception if the key of the message is null
									if (key.isEmpty() || key.toLowerCase().equals("null")) {
										throw new IllegalArgumentException("Inside 'Tag the record with Division transform': the key of the messsage is null ");
									}
									System.out.printf("** elementkey |%s| **%n",
											key);
									String[] keyParts = key.split(":");
									if (keyParts.length < 5) {
										throw new IllegalArgumentException("Inside 'Tag the record with Division transform': fifth position in key is not found ");
									}
									System.out.printf("** FacilityMnemonic |%s| **%n",
												keyParts[4]);
									String divisionName = bqSideInput.get(keyParts[4]);
									System.out.printf("** divisionName |%s| **%n",
												divisionName);
									if (divisionName.equals("texas")) {
											// Emit to main output, which is the output with tag startsWithATag.
										c.output(KV.of("TX", c.element()));
									} else if (divisionName.equals("AM")) {
											// Emit to output with tag startsWithBTag.
										c.output(KV.of("AM", c.element()));
										}
									}
								catch (Exception e){
									c.output(KV.of("ERROR", c.element()));
									LOG.error("ERROR Inside 'Tag the record with Division transform' with exception: ",e);
								}
							}
						}).withSideInputs(bqSideInputView));




		taggedOutput.apply("Fetch Texas records and write to Texas O/p Kafka",ParDo
				.of(new DoFn<KV<String, KV<String,String>>, KV<String,String>>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						if (c.element().getKey().equals("TX")) {
							// Emit to main output, which is the output with tag startsWithATag.
							c.output(c.element().getValue());
							}
						}
					}
				))
				.apply("WriteToKafka",
						KafkaIO.<String, String>write()
								.withBootstrapServers(
										options.getBootstrapServer())
//								.withTopic(options.getOutputTopic())
								.withTopic(options.TXOutputTopic)
								.withKeySerializer(StringSerializer.class)
								.withValueSerializer(StringSerializer.class));



		taggedOutput.apply("Fetch AM records and write to AM O/p Kafka",ParDo
						.of(new DoFn<KV<String, KV<String,String>>, KV<String,String>>() {
								@ProcessElement
								public void processElement(ProcessContext c) {
									if (c.element().getKey().equals("AM")) {
										// Emit to main output, which is the output with tag startsWithATag.
										c.output(c.element().getValue());
									}
								}
							}
						))
				.apply("WriteToKafka",
						KafkaIO.<String, String>write()
								.withBootstrapServers(
										options.getBootstrapServer())
//								.withTopic(options.getOutputTopic())
								.withTopic(options.AMOutputTopic)
								.withKeySerializer(StringSerializer.class)
								.withValueSerializer(StringSerializer.class));


//		PCollection<KV<String, String>> eventsWithXYZ = input.apply("FilterByCountry",
//				ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
//					@ProcessElement
//					public void processElement(ProcessContext c) {
//						KV<String, String> message = c.element();
//						String key = message.getKey();
//						System.out.printf("** elementkey |%s| **%n",
//								key);
//						String[] keyParts = key.split(":");
//						System.out.printf("** elementkeyParts |%s| **%n",
//								keyParts);
//
//						if (keyParts.length >= 5 && keyParts[4].equals("XYZ")){
//							c.output(KV.of(message.getKey(),message.getValue()));
//						}
//					}
//				}));



//		eventsWithXYZ
//				.apply("WriteToKafka",
//						KafkaIO.<String, String>write()
//								.withBootstrapServers(
//										options.getBootstrapServer())
//								.withTopic(options.getOutputTopic())
//								.withKeySerializer(StringSerializer.class)
//								.withValueSerializer(StringSerializer.class));
//
//
////		PipelineResult run = pipeline.run();
		return pipeline.run();
	}
}