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
import java.text.SimpleDateFormat;
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

public class Msg2KV extends DoFn<String, KV<String, Double>> {
	public static final String KEY_SEPERATOR = "##";
	
	/**
	 *  Splits String object into Key/Value pairs for aggregations
	 */
	@ProcessElement
	public void processElement(DoFn<String, KV<String, Double>>.ProcessContext c) throws Exception {
		JSONObject obj = new JSONObject(c.element());
		String key = obj.getString(Msg.DEVICE_ID) + KEY_SEPERATOR + obj.getString(Msg.METRIC);
		
		c.output(KV.of(key, obj.getDouble(Msg.VALUE)));
	}
}