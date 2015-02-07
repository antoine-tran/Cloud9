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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.umd.cloud9.collection.IndexableFileInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat.XMLRecordReader;
import edu.umd.cloud9.collection.wikipedia.language.WikipediaPageFactory;

/**
 * Hadoop {@code InputFormat} for processing Wikipedia pages from the XML dumps.
 *
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class WikiExtractorInputFormat extends FileInputFormat<LongWritable, Text> {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new WikiExtractorRecordReader();
	}

	public static class WikiExtractorRecordReader extends RecordReader<LongWritable, Text> {
		private XMLRecordReader reader = new XMLRecordReader();
		private Text page;
		private String language;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf.set(XMLInputFormat.START_TAG_KEY, WikipediaPage.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, WikipediaPage.XML_END_TAG);

			language = conf.get("wiki.language", "en"); // Assume 'en' by default.
			page = new Text();
			reader.initialize(split, context);
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return reader.getCurrentValue();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return reader.nextKeyValue();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}
	}
}