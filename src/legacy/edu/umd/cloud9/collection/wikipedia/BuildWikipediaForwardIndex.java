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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.wikipedia.language.WikipediaPageFactory;

/**
 * Tool for building a document forward index for Wikipedia.
 * This code has been re-factored to work with new Hadoop MR API
 *
 * @author Tuan Tran
 */
public class BuildWikipediaForwardIndex extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(BuildWikipediaForwardIndex.class);

	private static final String INPUT_OPTION = "input";
	private static final String INDEX_FILE_OPTION = "index_file";
	private static final String LANGUAGE_OPTION = "wiki_language";

	@SuppressWarnings("static-access") @Override
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index file").create(INDEX_FILE_OPTION));
		options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
				.withDescription("two-letter language code").create(LANGUAGE_OPTION));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			return -1;
		}

		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(INDEX_FILE_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Path inputPath = new Path(cmdline.getOptionValue(INPUT_OPTION));
		String indexFile = cmdline.getOptionValue(INDEX_FILE_OPTION);

		if ( !inputPath.isAbsolute()) {
			System.err.println("Error: " + INPUT_OPTION + " must be an absolute path!");
			return -1;
		}

		String language = null;
		if (cmdline.hasOption(LANGUAGE_OPTION)) {
			language = cmdline.getOptionValue(LANGUAGE_OPTION);
			if(language.length()!=2){
				System.err.println("Error: \"" + language + "\" unknown language!");
				return -1;
			}
		}

		LOG.info("Tool name: " + this.getClass().getName());
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - index file: " + indexFile);
		LOG.info("Note: This tool only works on block-compressed SequenceFiles!");
		LOG.info(" - language: " + language);

		FSDataOutputStream out = null;
		SequenceFile.Reader reader = null;
		try {
			FileSystem fs = FileSystem.get(getConf());
			FileStatus[] status = fs.listStatus(inputPath, new PathFilter() {				
				public boolean accept(Path p) {	return p.getName().startsWith("part-m-");}});

			out = fs.create(new Path(indexFile), true);
			out.writeUTF("edu.umd.cloud9.collection.wikipedia.WikipediaForwardIndex");
			out.writeUTF(inputPath.toString());

			int blocks = 0;

			IntArrayList docNos = new IntArrayList();
			LongArrayList offsets = new LongArrayList();
			ShortArrayList fileNos = new ShortArrayList();

			for (FileStatus s : status) {
				Path path = s.getPath();
				String name = path.getName();
				LOG.info("processing file " + name);
				short fileNo = Short.parseShort(name.substring(name.lastIndexOf("part-m-")+7));

				try {
					reader = new SequenceFile.Reader(fs, path, getConf());
					IntWritable key = new IntWritable();
					long pos = -1;
					long prevPos = -1;

					int prevDocno = 0;
					pos = reader.getPosition();

					while (reader.next(key)) {
						blocks++;
						if (prevPos != -1 && prevPos != pos) {
							LOG.info("- beginning of block at " + prevPos + ", docno:" + prevDocno + ", file:" + fileNo);
							docNos.add(prevDocno);
							offsets.add(prevPos);
							fileNos.add(fileNo);
						}
						prevPos = pos;
						pos = reader.getPosition();
						prevDocno = key.get();
					}	
				} finally {
					if (reader != null) reader.close();
				}	
			}

			out.writeInt(blocks);
			int[] docsNo = docNos.toIntArray();
			long[] offset = offsets.toLongArray();
			short[] filesNo = fileNos.toShortArray();
			
			LOG.info(docsNo.length + ", " + offset.length + ", " + filesNo.length);

			// we did not use MapReduce so we have to manually sort the arrays
			sort(docsNo, offset, filesNo, 0, blocks);

			for (int i = 0; i < blocks; i++) {
				out.writeInt(docsNo[i]);
				out.writeLong(offset[i]);
				out.writeShort(filesNo[i]);
			}

		} catch (IOException e) {
			LOG.error(e.getMessage());
		} finally {
			if (out != null) out.close();
		}
		return 0;
	}

	// The following code snippets are copied from Java openJDK 6-b14 source code,
	// with some adaptations to make it able to sort two arrays in parallel
	/**
	 * Sorts the specified sub-array of integers into ascending order.
	 */
	private static void sort(int x[], long[] y, short[] z, int off, int len) {
		// Insertion sort on smallest arrays
		if (len < 7) {
			for (int i=off; i<len+off; i++)
				for (int j=i; j>off && x[j-1]>x[j]; j--)
					swap(x, y, z, j, j-1);
			return;
		}

		// Choose a partition element, v
		int m = off + (len >> 1);       // Small arrays, middle element
		if (len > 7) {
			int l = off;
			int n = off + len - 1;
			if (len > 40) {        // Big arrays, pseudomedian of 9
				int s = len/8;
				l = med3(x, l,     l+s, l+2*s);
				m = med3(x, m-s,   m,   m+s);
				n = med3(x, n-2*s, n-s, n);
			}
			m = med3(x, l, m, n); // Mid-size, med of 3
		}
		int v = x[m];

		// Establish Invariant: v* (<v)* (>v)* v*
		int a = off, b = a, c = off + len - 1, d = c;
		while(true) {
			while (b <= c && x[b] <= v) {
				if (x[b] == v)
					swap(x, y, z, a++, b);
				b++;
			}
			while (c >= b && x[c] >= v) {
				if (x[c] == v)
					swap(x, y, z, c, d--);
				c--;
			}
			if (b > c)
				break;
			swap(x, y, z, b++, c--);
		}

		// Swap partition elements back to middle
		int s, n = off + len;
		s = Math.min(a-off, b-a  );  vecswap(x, y, z, off, b-s, s);
		s = Math.min(d-c,   n-d-1);  vecswap(x, y, z, b,   n-s, s);

		// Recursively sort non-partition-elements
		if ((s = b-a) > 1)
			sort(x, y, z, off, s);
		if ((s = d-c) > 1)
			sort(x, y, z, n-s, s);
	}

	/**
	 * Swaps x[a] with x[b].
	 */
	private static void swap(int x[], long[] y, short[] z, int a, int b) {
		int t = x[a];
		x[a] = x[b];
		x[b] = t;
		long s = y[a];
		y[a] = y[b];
		y[b] = s;
		short r = z[a];
		z[a] = z[b];
		z[b] = r;
	}

	/**
	 * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
	 */
	private static void vecswap(int x[], long[] y, short[] z, int a, int b, int n) {
		for (int i=0; i<n; i++, a++, b++)
			swap(x, y, z, a, b);
	}

	/**
	 * Returns the index of the median of the three indexed integers.
	 */
	private static int med3(int x[], int a, int b, int c) {
		return (x[a] < x[b] ?
				(x[b] < x[c] ? b : x[a] < x[c] ? c : a) :
					(x[b] > x[c] ? b : x[a] > x[c] ? c : a));
	}

	public BuildWikipediaForwardIndex() {}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BuildWikipediaForwardIndex(), args);
	}
}