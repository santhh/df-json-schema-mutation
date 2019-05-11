/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.swarm.importpipeline;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.swarm.importpipeline.common.GCSImportPipelineOptions;

public class GCSImportPipeline {

	public static final Logger LOG = LoggerFactory.getLogger(GCSImportPipeline.class);

	private static final Duration POLLING_INTERVAL = Duration.standardSeconds(120);

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		GCSImportPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(GCSImportPipelineOptions.class);
		Pipeline pipeline = Pipeline.create(options);

		PCollection<ReadableFile> jsonFile = pipeline
				.apply("Poll Input Files",
						FileIO.match().filepattern(options.getInputFile()).continuously(POLLING_INTERVAL,
								Watch.Growth.never()))
				.apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.UNCOMPRESSED));


	
		//Publish to PubSub
		jsonFile.apply("File Processor", ParDo.of(new DoFn<ReadableFile, PubsubMessage>() {

			@ProcessElement
			public void processElement(ProcessContext c) throws IOException {
				ReadableFile file = c.element();
				String fileName = file.getMetadata().resourceId().getFilename();
				String eventType = String.format("%s_%s", fileName.split("_")[0].trim(),
						fileName.split("_")[1].trim());
				if (eventType!=null) {
					try (BufferedReader br = getReader(file)) {
		                  
						br.lines().forEach(line->{
							PubsubMessage message = new PubsubMessage(line.getBytes(),
									Collections.singletonMap("event_type", eventType));
							LOG.debug("EventType {} , Message {}",message.getAttribute("event_type"), line);
							c.output(message);
						});

	                   } catch (IOException e) {
	                     LOG.error("Failed to Read File {}", e.getMessage());
	                     throw new RuntimeException(e);
	                   }	
					
				} else {
					 LOG.error("Failed to Parse Event Type {}",fileName);
                     throw new RuntimeException("Failed to Parse Event Type"+fileName);
					
				}
					
			} 
				

		}))
		.apply("Publish Events", PubsubIO.writeMessages().to(options.getTopic()));

		pipeline.run();

	}
	
	private static BufferedReader getReader(ReadableFile eventFile) {
	    BufferedReader br = null;
	    ReadableByteChannel channel = null;
	    /** read the file and create buffered reader */
	    try {
	      channel = eventFile.openSeekable();

	    } catch (IOException e) {
	      LOG.error("Failed to Open File {}", e.getMessage());
	      throw new RuntimeException(e);
	    }

	    if (channel != null) {

	      br = new BufferedReader(Channels.newReader(channel, Charsets.ISO_8859_1.name()));
	    }

	    return br;
	  }

	
}



