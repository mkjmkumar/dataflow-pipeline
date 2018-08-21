package com.ceaas.pubsub2bq.transforms;

/**
 * ===========================================
 *            + + + DISCLAIMER + + +
 * ===========================================
 * The code is for DEMO purpose only and it is 
 * not intended to be put in production
 * ===========================================
 * 
 * Copyright 2018 Thomas Frantzen All Rights Reserved.
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
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import org.json.*;

public class KV2Row extends DoFn<KV<String, Double>, TableRow> {
    public static final String TABLE_NAME = "aggregations";
    
    /**
     *  Convert Key/Value pairs into Table Rows for BigQuery consumption
     */
	@ProcessElement
	public void processElement(DoFn<KV<String, Double>, TableRow>.ProcessContext c) throws Exception {
		String[] byIdbyMetric = c.element().getKey().split(Msg2KV.KEY_SEPERATOR);
		
		String dateTime = c.timestamp().toString();
        c.output(new TableRow()
				 .set(Msg.TIME, dateTime.substring(0, dateTime.length() - 1))
                 .set(Msg.DEVICE_ID, byIdbyMetric[0])
                 .set(Msg.METRIC, byIdbyMetric[1])
                 .set("average", c.element().getValue().intValue()));
	}
	
	/**
	 * Schema represenation for BigQuery averages table
	 */
	public static TableSchema getSchema() {
	      return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
	            {
		             add(new TableFieldSchema().setName(Msg.TIME).setType("DATETIME"));
                     add(new TableFieldSchema().setName(Msg.DEVICE_ID).setType("STRING"));
                     add(new TableFieldSchema().setName(Msg.METRIC).setType("STRING"));
                     add(new TableFieldSchema().setName("average").setType("INTEGER"));
	              
	            }
	      });
	}
}