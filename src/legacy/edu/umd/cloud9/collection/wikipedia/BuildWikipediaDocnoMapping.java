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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Tool for building the mapping between Wikipedia internal ids (docids) and sequentially-numbered
 * ints (docnos).
 * This code has been re-factored to use new Hadoop MR API
 *
 * @author Jimmy Lin
 * @author Peter Exner
 * @author Tuan Tran
 */
public class BuildWikipediaDocnoMapping extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildWikipediaDocnoMapping.class);

  private static enum PageTypes {
    TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE, OTHER
  };

  private static class MyMapper extends Mapper<LongWritable, WikipediaPage, IntWritable, IntWritable> {

    private final static IntWritable keyOut = new IntWritable();
    private final static IntWritable valOut = new IntWritable(1);

    private boolean keepAll;
    
    @Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		keepAll = context.getConfiguration().getBoolean(KEEP_ALL_OPTION, false);
    }

	@Override
    protected void map(LongWritable key, WikipediaPage p, Context context) 
    		throws IOException, InterruptedException {
      context.getCounter(PageTypes.TOTAL).increment(1);

      // If we're keeping all pages, don't bother checking.
      if (keepAll) {
        keyOut.set(Integer.parseInt(p.getDocid()));
        context.write(keyOut, valOut);
        return;
      }
      
      if (p.isRedirect()) {
        context.getCounter(PageTypes.REDIRECT).increment(1);
      } else if (p.isEmpty()) {
        context.getCounter(PageTypes.EMPTY).increment(1);
      } else if (p.isDisambiguation()) {
    	context.getCounter(PageTypes.DISAMBIGUATION).increment(1);
      } else if (p.isArticle()) {
        // heuristic: potentially template or stub article
        if (p.getTitle().length() > 0.3*p.getContent().length()) {
          context.getCounter(PageTypes.OTHER).increment(1);
          return;
        }
        
        context.getCounter(PageTypes.ARTICLE).increment(1);

        if (p.isStub()) {
          context.getCounter(PageTypes.STUB).increment(1);
        }

        keyOut.set(Integer.parseInt(p.getDocid()));
        context.write(keyOut, valOut);
      } else {
    	context.getCounter(PageTypes.NON_ARTICLE).increment(1);
      }
    }
  }

  private static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private final static IntWritable cnt = new IntWritable(1);

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
    		throws IOException, InterruptedException {
      context.write(key, cnt);
      cnt.set(cnt.get() + 1);
    }
  }

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_PATH_OPTION = "output_path";
  public static final String OUTPUT_FILE_OPTION = "output_file";
  public static final String KEEP_ALL_OPTION = "keep_all";
  public static final String LANGUAGE_OPTION = "wiki_language";

  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("tmp output directory").create(OUTPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("output file").create(OUTPUT_FILE_OPTION));
    options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
        .withDescription("two-letter language code").create(LANGUAGE_OPTION));
    options.addOption(KEEP_ALL_OPTION, false, "keep all pages");

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_PATH_OPTION)
        || !cmdline.hasOption(OUTPUT_FILE_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
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
    
    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_PATH_OPTION);
    String outputFile = cmdline.getOptionValue(OUTPUT_FILE_OPTION);
    boolean keepAll = cmdline.hasOption(KEEP_ALL_OPTION);
    

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - output file: " + outputFile);
    LOG.info(" - keep all pages: " + keepAll);
    LOG.info(" - language: " + language);

    Job job = new Job(getConf(), 
    	String.format("BuildWikipediaDocnoMapping[%s: %s, %s: %s, %s: %s]", INPUT_OPTION,
    	inputPath, OUTPUT_FILE_OPTION, outputFile, LANGUAGE_OPTION, language));
    
    job.setJarByClass(BuildWikipediaDocnoMapping.class);

    getConf().setBoolean(KEEP_ALL_OPTION, keepAll);
    
    if(language != null){
    	getConf().set("wiki.language", language);
    }
    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.setInputFormatClass(EnglishWikipediaPageInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem fs = FileSystem.get(getConf());
    fs.delete(new Path(outputPath), true);

    job.waitForCompletion(true);
    Counters c = job.getCounters();
    long cnt = keepAll ? c.findCounter(PageTypes.TOTAL).getValue() : 
    	c.findCounter(PageTypes.ARTICLE).getValue();

    WikipediaDocnoMapping.writeDocnoMappingData(fs, outputPath + "/part-r-00000", (int) cnt, outputFile);

    return 0;
  }

  public BuildWikipediaDocnoMapping() {}

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildWikipediaDocnoMapping(), args);
  }
}
