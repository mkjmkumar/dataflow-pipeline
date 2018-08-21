
/**
 * ===========================================
 *            + + + DISCLAIMER + + +
 * ===========================================
 * The code is for DEMO purpose only and it is 
 * not intended to be put in production
 * ===========================================
 * 
 * Copyright 2018 Elena Cuevas, Thomas Frantzen All Rights Reserved.
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
 * 
 */

package com.ceaas.pubsub2bq;

import com.ceaas.pubsub2bq.shared.Constants;

import com.ceaas.pubsub2bq.transforms.*;
import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.Entity;

import java.text.SimpleDateFormat;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.*;

public class SamplePipeline {
  // data format for job name
  public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
  
  public static void main(String[] args) {
	//define pipeline options
	DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setAppName(Constants.APP_NAME);
		options.setStagingLocation(Constants.GCS_URL_BASE + Constants.GCS_BUCKET_NAME + "/"+Constants.GCS_STAGING_LOCATION);
		options.setTempLocation(Constants.GCS_URL_BASE + Constants.GCS_BUCKET_NAME + "/"+Constants.GCS_TEMP_LOCATION);
		options.setRunner(DataflowRunner.class);
		options.setStreaming(true);
		options.setProject(Constants.PROJECT_ID);
		options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
		options.setMaxNumWorkers(Constants.MAX_NUM_WORKER);
		options.setJobName(Constants.DF_BASE_JOB_NAME+dateFormat.format(new java.util.Date()));
	
	//create Pipeline with options
    Pipeline p = Pipeline.create(options);
    
    //read messages from Pub/Sub using a window
    PCollection<String> pubSubMessages=p.apply("Read messages from PubSub",PubsubIO.readStrings().fromSubscription("projects/" + Constants.PROJECT_ID + "/subscriptions/" + Constants.PUBSUB_SUBSCRIPTION).withTimestampAttribute(Msg.TIME));
    
    // Process aggregations
    pubSubMessages
    	.apply("Apply window",Window.<String>into(SlidingWindows.of(Duration.standardSeconds(Constants.DURATION)).every(Duration.standardSeconds(Constants.EVERY))).withTimestampCombiner(TimestampCombiner.EARLIEST))
    	.apply("Map keys to values", ParDo.of(new Msg2KV()))
    	.apply("Group by Keys", GroupByKey.<String, Double>create())
    	.apply("Calculate Averages", ParDo.of(new Average()))
		.apply("Convert Map to Table Rows", ParDo.of(new KV2Row()))
		.apply("Write aggregations into BigQuery",
    		BigQueryIO.writeTableRows()
    			.to(Constants.PROJECT_ID + ":" + Constants.DATASET + "." + KV2Row.TABLE_NAME)
    			.withSchema(KV2Row.getSchema())
    			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
    			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
			);
			
    // Insert raw data
    pubSubMessages
    	.apply("Raw Data to Table Row", ParDo.of(new Msg()))
    	.apply("Write raw data into BigQuery",
    		BigQueryIO.writeTableRows()
    			.to(Constants.PROJECT_ID + ":" + Constants.DATASET + "." + Msg.TABLE_NAME)
    			.withSchema(Msg.getSchema())
    			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
    			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
			);
			
	p.run();
  }
}
