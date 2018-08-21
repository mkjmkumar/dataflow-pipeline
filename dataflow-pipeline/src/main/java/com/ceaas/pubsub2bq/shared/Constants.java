package com.ceaas.pubsub2bq.shared;

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

public class Constants {
	// UPDATE
	public static final String GCS_BUCKET_NAME="<bucket-name>";
	public static final String PROJECT_ID="<project-id>";
	public static final String PUBSUB_SUBSCRIPTION="<subscription-name>";
  
  	// DON'T UPDATE
	public static final String APP_NAME="Sample Dataflow Aggregation to BigQuery";
	public static final String DF_BASE_JOB_NAME="ps-df-bq";
	public static final String GCS_URL_BASE="gs://";
	public static final String GCS_TEMP_LOCATION="dataflow/temp";
	public static final String GCS_STAGING_LOCATION="dataflow/staging";
	

	public static final int MAX_NUM_WORKER=10;
	
	// size of window
	public static final int DURATION = 300;
	// frequency of window
	public static final int EVERY = 60;
	
	// bigquery dataset
	public static final String DATASET="data";
}
