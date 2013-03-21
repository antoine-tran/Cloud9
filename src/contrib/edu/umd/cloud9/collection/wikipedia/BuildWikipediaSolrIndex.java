package edu.umd.cloud9.collection.wikipedia;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.WikipediaPageFactory;

/** Build Wikipedia Lucene Index from 4 fields: titles, first sentence, first
 * paragraph, content
 * @author Tuan Tran 
 */
public class BuildWikipediaSolrIndex extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(BuildWikipediaSolrIndex.class);
	
	private static final String BLOCK_COMPRESS_FILE_OPT = "input";
	private static final String SOLR_HTTP = "index";
	private static final String LANGUAGE_OPT = "wiki_language";
			
	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("input").hasArg()
				.withDescription("path of the block-compressed files").create(BLOCK_COMPRESS_FILE_OPT));
		options.addOption(OptionBuilder.withArgName("index").hasArg()
				.withDescription("HTTP address of the solr index").create(SOLR_HTTP));
		options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
				.withDescription("language of the Wikipedia corpus").create(LANGUAGE_OPT));
		
		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			return -1;
		}
		
		if (!cmdline.hasOption(BLOCK_COMPRESS_FILE_OPT) || !cmdline.hasOption(SOLR_HTTP)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Path inputPath =  new Path(cmdline.getOptionValue(BLOCK_COMPRESS_FILE_OPT));
		
		if ( !inputPath.isAbsolute()) {
			System.err.println("Error: " + BLOCK_COMPRESS_FILE_OPT + " must be an absolute path!");
			return -1;
		}
		
		String solrUrl = cmdline.getOptionValue(SOLR_HTTP);
		String lang = null;
		if (cmdline.hasOption(LANGUAGE_OPT)) {
			lang = cmdline.getOptionValue(LANGUAGE_OPT);	
		}
		
		LOG.info("Tool name: " + this.getClass().getName());
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - solr HTTP : " + solrUrl);
		LOG.info("Note: This tool only works on block-compressed SequenceFiles!");
		LOG.info(" - language: " + lang);

		HttpSolrServer solr = null;
		
		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] status = fs.listStatus(inputPath, new PathFilter() {				
			public boolean accept(Path p) {	return p.getName().startsWith("part-m-");}
		});

		try {						
			solr = new HttpSolrServer(solrUrl);			
			for (FileStatus s : status) {
				Path path = s.getPath();
				String name = path.getName();
				LOG.info("processing file " + name);
				SequenceFile.Reader reader = null;
				List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
				try {
					reader = new SequenceFile.Reader(fs, path, getConf());
					IntWritable key = new IntWritable();
					WikipediaPage page = WikipediaPageFactory.createWikipediaPage(lang);
					reader = new SequenceFile.Reader(fs, path, getConf());
					LOG.info("key class: " + reader.getKeyClassName());
					LOG.info("value class: " + reader.getValueClassName());
					while (reader.next(key, page)) {
						int i = key.get();
						String title = page.getTitle();				
						LOG.info("docno " + i + ", title: " + title);
						SolrInputDocument doc = new SolrInputDocument();
						doc.addField("title", title);
						doc.addField("titlext", title);
						doc.addField("docid", page.getDocid());
						doc.addField("docno", i);
						String content = page.getBodyContent();
						String par = WikipediaPage.firstParagraph(content);
						String sen = WikipediaPage.firstSentence(par);
						doc.addField("sen", sen);
						doc.addField("par", par);
						doc.addField("content", content);
						doc.addField("raw", page.getRawXML());
						docs.add(doc);
					}
					solr.add(docs);
				} catch (Exception e) {
					e.printStackTrace();
				}
				 finally {
					if (reader != null) reader.close();
				}
			}
		} finally {			
			if (solr != null) {
				solr.commit();
				solr.shutdown();
			}
		}
		return 0;
	}
	
	public static void main(String[] args) {
		int res = 0;
		try {
			res = ToolRunner.run(new BuildWikipediaSolrIndex(), args);
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			LOG.info("result: " + res);
			System.exit(res);
		}
	}
}
