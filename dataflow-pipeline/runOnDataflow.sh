#!/bin/bash

mvn compile exec:java \
	-Dexec.mainClass=com.ceaas.pubsub2bq.SamplePipeline \
	-Dexec.args="--runner=DataflowRunner"

