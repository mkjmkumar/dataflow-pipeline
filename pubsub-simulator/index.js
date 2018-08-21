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

// Input Variables
const projectId = '<project-id>';
const pubsubTopicName = '<topic-name>';

const throughput = 100; // messages, how many?
const every = 1000; // milliseconds, how often?

const ids = ['device-id-01', 'device-id-02', 'device-id-03', 'device-id-04'];
const metrics = ['milesPerHour', 'elevation'];


/**
 * Pub/Sub Simulator
 */
const PubSub = require(`@google-cloud/pubsub`);
const pubsub = new PubSub();

function run() {
	var genId = ids[Math.floor(Math.random()*ids.length)];
	var genMetric = metrics[Math.floor(Math.random()*metrics.length)];
	
	var genValue = 0;
	switch(genMetric) {
		case "milesPerHour":
			genValue = Math.floor(Math.random() * (100)) + 1;
			break;
		case "elevation":
			genValue = Math.floor(Math.random() * (5000)) + 1;
			break;
	}
	
	const topicName = `projects/${projectId}/topics/${pubsubTopicName}`;
	const data = JSON.stringify(
		{
			deviceId: genId, // STRING, NULLABLE, ex. dl1-receptor01 through 12
			metric: genMetric, // STRING, NULLABLE, NULLABLE, ex. lcaas
			value: genValue, // INTEGER, REQUIRED
		}
	);
	var _time = new Date().toISOString();
	const attributes = {
		time: _time.substring(0, _time.length-1)
	};

	const dataBuffer = Buffer.from(data);
	
	// Wrap timer around this for throughput and duration
	pubsub
	  .topic(topicName)
	  .publisher()
	  .publish(dataBuffer, attributes)
	  .then(messageId => {
	    //console.log(`Message ${messageId} published.`);
	  })
	  .catch(err => {
	    console.error('ERROR:', err);
	  });
}

setInterval(function(){
	var i;
	for(i = 0; i < throughput; i++) {
		run();
	}
	console.log('Published 100 Pub/Sub messages');
}, every);
