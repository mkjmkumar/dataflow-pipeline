package com.ceaas.pubsub2bq.transforms;

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
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import org.json.*;

public class Msg extends DoFn<String, TableRow> {
	private static final Logger LOG = LoggerFactory.getLogger(Msg.class);
    public static final String TABLE_NAME = "raw_data";
	
	public static final String TIME = "time";
	public static final String DEVICE_ID = "deviceId";
	public static final String METRIC = "metric";
	public static final String VALUE = "value";
	
    /**
     *  Convert String into Table Rows for BigQuery consumption
     */
	@ProcessElement
	public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {		
		String input = c.element();

		List<String> dataFields=new ArrayList<String>();
		JSONObject obj = new JSONObject(input);
		
		String dateTime = c.timestamp().toString();
		c.output(new TableRow()
				 .set(TIME, dateTime.substring(0, dateTime.length() - 1))
                 .set(DEVICE_ID, obj.getString(DEVICE_ID))
                 .set(METRIC, obj.getString(METRIC))
                 .set(VALUE, obj.getInt(VALUE))
    		  	);
	}
	
	/**
	 * Schema represenation for BigQuery raw data table
	 */
	public static TableSchema getSchema() {
	      return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
	            {
		             add(new TableFieldSchema().setName(TIME).setType("DATETIME"));
                     add(new TableFieldSchema().setName(DEVICE_ID).setType("STRING"));
                     add(new TableFieldSchema().setName(METRIC).setType("STRING"));
                     add(new TableFieldSchema().setName(VALUE).setType("INTEGER"));
	            }
	      });
	}
}