/*
 * Cloud9: A MapReduce Library for Hadoop
 *  
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.umd.cloud9.collection.IndexableFileInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat.XMLRecordReader;
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;

/**
 * @author tuan
 *
 */
public class EnglishWikipediaPageInputFormat extends 
IndexableFileInputFormat<LongWritable, WikipediaPage> {

	@Override
	public RecordReader<LongWritable, WikipediaPage> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {	
		context.setStatus(split.toString());
		WikipediaPageRecordReader reader = new WikipediaPageRecordReader();
		reader.initialize(split, context);
		return reader;
	}

	public static class WikipediaPageRecordReader extends 
	RecordReader<LongWritable, WikipediaPage> {

		private final XMLRecordReader reader = new XMLRecordReader();
		private final WikipediaPage page = new EnglishWikipediaPage();
		
		public WikipediaPageRecordReader() {
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf.set(XMLInputFormat.START_TAG_KEY, WikipediaPage.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, WikipediaPage.XML_END_TAG);
			reader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {		
			return reader.nextKeyValue();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
		InterruptedException {			
			return reader.getCurrentKey();
		}

		@Override
		public WikipediaPage getCurrentValue() throws IOException,
		InterruptedException {
			WikipediaPage.readPage(page, reader.getCurrentValue().toString());
			return page;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}
	}
}
