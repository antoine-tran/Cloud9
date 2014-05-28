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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.net.NetworkTopology;
import org.wikimedia.wikihadoop.ByteMatcher;
import org.wikimedia.wikihadoop.SeekableInputStream;

import edu.umd.cloud9.collection.IndexableFileInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat.XMLRecordReader;
import edu.umd.cloud9.collection.wikipedia.language.WikipediaPageFactory;

/**
 * Hadoop {@code InputFormat} for processing Wikipedia pages from the XML dumps.
 *
 * @author Jimmy Lin
 * @author Peter Exner
 * 
 * @since 29.05.2014 _ (@author tuan: Add splitting support for compressed file taken from
 * wikihadoop project)
 */
public class WikipediaPageInputFormat extends IndexableFileInputFormat<LongWritable, WikipediaPage> {
	
	protected CompressionCodecFactory compressionCodecs = null;

	@Override
	public boolean isSplitable(JobContext context, Path file) {
		Configuration conf = context.getConfiguration();
		if (compressionCodecs == null)
			compressionCodecs = new CompressionCodecFactory(conf);
		CompressionCodec codec = compressionCodecs.getCodec(file);		
		if (null == codec) {
			return true;
		}
		return codec instanceof SplittableCompressionCodec;
	}
	
	/** 
	 * This code is copied from StreamWikiDumpNewInputFormat.java by Yusuke Matsubara.
	 * Thanks to Tu Meteora for adjusting the code to the new mapreduce framework
	 * @param job the job context
	 * @throws IOException
	 */
	@Override
	public List<InputSplit> getSplits(JobContext jc) throws IOException {
		List<InputSplit> splits = super.getSplits(jc);
		List<FileStatus> files = listStatus(jc);
		// Save the number of input files for metrics/loadgen
		
		// check we have valid files
		for (FileStatus file: files) {                
			if (file.isDirectory()) {
				throw new IOException("Not a file: "+ file.getPath());
			}
		}
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(jc));
		long maxSize = getMaxSplitSize(jc);
		for (FileStatus file: files) {
			if (file.isDirectory()) {
				throw new IOException("Not a file: "+ file.getPath());
			}
			long blockSize = file.getBlockSize();
			long splitSize = computeSplitSize(blockSize, minSize, maxSize);
			for (InputSplit x: getSplits(jc, file, WikipediaPage.XML_START_TAG, splitSize)) 
				splits.add(x);
		}
		return splits;
	}

	/** 
	 * This code is copied from StreamWikiDumpNewInputFormat.java by Yusuke Matsubara.
	 * Thanks to Tu Meteora for adjusting the code to the new mapreduce framework
	 * @param job the job context
	 * @throws IOException
	 */
	public List<InputSplit> getSplits(JobContext jc, FileStatus file, String pattern, 
			long splitSize) throws IOException {

		NetworkTopology clusterMap = new NetworkTopology();
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Path path = file.getPath();
		Configuration conf = jc.getConfiguration();
		if (compressionCodecs == null)
			compressionCodecs = new CompressionCodecFactory(conf);

		long length = file.getLen();
		FileSystem fs = file.getPath().getFileSystem(conf);
		BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
		if ((length != 0) && isSplitable(jc, path)) { 
			long bytesRemaining = length;

			SeekableInputStream in = SeekableInputStream.getInstance(path, 0, length, fs, 
					this.compressionCodecs);
			SplitCompressionInputStream is = in.getSplitCompressionInputStream();
			long start = 0;
			long skip = 0;
			if ( is != null ) {
				start = is.getAdjustedStart();
				length = is.getAdjustedEnd();
				is.close();
				in = null;
			}
			FileSplit split = null;
			Set<Long> processedPageEnds = new HashSet<Long>();
			float factor = 1.2F;

			READLOOP:
				while (((double) bytesRemaining)/splitSize > factor  &&  bytesRemaining > 0) {
					// prepare matcher
					ByteMatcher matcher;
					{
						long st = Math.min(start + skip + splitSize, length - 1);
						split = makeSplit(path, st, Math.min(splitSize, length - st), 
								clusterMap, blkLocations);
						if ( in != null )
							in.close();
						if ( split.getLength() <= 1 ) {
							break;
						}
						in = SeekableInputStream.getInstance(split, fs, this.compressionCodecs);
					}
					matcher = new ByteMatcher(in);

					// read until the next page end in the look-ahead split
					while ( !matcher.readUntilMatch(WikipediaPage.XML_END_TAG, null, split.getStart() 
							+ split.getLength()) ) {
						if (matcher.getPos() >= length  ||  split.getLength() == length 
								- split.getStart())
							break READLOOP;
						split = makeSplit(path,
								split.getStart(),
								Math.min(split.getLength() + splitSize, length - split.getStart()),
								clusterMap, blkLocations);
					}
					if ( matcher.getLastUnmatchPos() > 0
							&&  matcher.getPos() > matcher.getLastUnmatchPos()
							&&  !processedPageEnds.contains(matcher.getPos()) ) {
						splits.add(makeSplit(path, start, matcher.getPos() - start, clusterMap, 
								blkLocations));
						processedPageEnds.add(matcher.getPos());
						long newstart = Math.max(matcher.getLastUnmatchPos(), start);
						bytesRemaining = length - newstart;
						start = newstart;
						skip = 0;
					} else {
						skip = matcher.getPos() - start;
					}
				}

			if (bytesRemaining > 0 && !processedPageEnds.contains(length)) {
				splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining, 
						blkLocations[blkLocations.length-1].getHosts()));
			}
			if ( in != null )
				in.close();
		} else if (length != 0) {
			splits.add(makeSplit(path, 0, length, clusterMap, blkLocations));
		} else { 
			//Create empty hosts array for zero length files
			splits.add(makeSplit(path, 0, length, new String[0]));
		}
		return splits;
	}

	private FileSplit makeSplit(Path path, long start, long size, NetworkTopology clusterMap, 
			BlockLocation[] blkLocations) throws IOException {
		String[] hosts = blkLocations[blkLocations.length-1].getHosts();
		return makeSplit(path, start, size,hosts);
	}

	/**
	 * Tuan, Tu (22.05.2014) - For some reasons, the Pig version in the Hadoop@L3S does not 
	 * recognize this method in FileInputFormat. We need to hard-code and copied the source
	 * code over here
	 */
	@Override
	protected FileSplit makeSplit(Path file, long start, long length, String[] hosts) {
		return new FileSplit(file, start, length, hosts);
	}
	
	@Override
	public RecordReader<LongWritable, WikipediaPage> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new WikipediaPageRecordReader();
	}

	public static class WikipediaPageRecordReader extends RecordReader<LongWritable, WikipediaPage> {
		private XMLRecordReader reader = new XMLRecordReader();
		private WikipediaPage page;
		private String language;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf.set(XMLInputFormat.START_TAG_KEY, WikipediaPage.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, WikipediaPage.XML_END_TAG);

			language = conf.get("wiki.language", "en"); // Assume 'en' by default.
			page = WikipediaPageFactory.createWikipediaPage(language);

			reader.initialize(split, context);
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public WikipediaPage getCurrentValue() throws IOException, InterruptedException {
			WikipediaPage.readPage(page, reader.getCurrentValue().toString());
			return page;
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
