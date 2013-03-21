package edu.umd.cloud9.collection.wikipedia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.util.Version;

import tuan.lucene.LuceneIndex;
import tuan.lucene.VectorTextField;

import edu.umd.cloud9.collection.wikipedia.WikipediaForwardIndex;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/** Build Wikipedia Lucene Index from 4 fields: titles, first sentence, first
 * paragraph, content */
public class BuildWikipediaIndex extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(BuildWikipediaIndex.class);
	
	@Override
	public int run(String[] args) throws Exception {
		IndexWriter lucene = null;
		try {
			if (args.length < 3) {
				System.out.println("usage: [forward-index-path] [docno-mapping-data-file] [lucene-index-dir]");
				ToolRunner.printGenericCommandUsage(System.out);
				return -1;
			}
			System.out.println(args[0]);
			Configuration conf = getConf();
			WikipediaForwardIndex f = new WikipediaForwardIndex(conf);
			f.loadIndex(new Path(args[1]), new Path(args[2]), FileSystem.get(conf));
			
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
			lucene = LuceneIndex.createIndexWriter(analyzer, args[3], OpenMode.CREATE);
			
			int beginId = f.getFirstDocno();
			LOG.info("Tuan: begin ID " + beginId);			
			int lastId = f.getLastDocno();
			LOG.info("Tuan: last ID " + lastId);
			
			for (int i = beginId; i <= lastId; i++) {
				WikipediaPage page = f.getDocument(i);
				String title = page.getTitle();				
				LOG.info("docno " + i + ", title: " + title);
				Document doc = new Document();
				doc.add(new StringField("title", title, Store.YES));
				doc.add(new StringField("titlext", title, Store.YES));
				doc.add(new StringField("docid", page.getDocid(), Store.YES));
				doc.add(new IntField("docno", i, Store.YES));
				String content = page.getBodyContent();
				String par = WikipediaPage.firstParagraph(content);
				String sen = WikipediaPage.firstSentence(par);
				doc.add(new VectorTextField("sen", sen, Store.YES));
				doc.add(new VectorTextField("par", par, Store.YES));
				doc.add(new VectorTextField("content", content, Store.YES));
				doc.add(new VectorTextField("raw", page.getRawXML(), Store.YES));
				lucene.addDocument(doc);
			}
		} finally {			
			if (lucene != null) lucene.close();
		}
		return 0;
	}
	
	public static void main(String[] args) {
		int res = 0;
		try {
			res = ToolRunner.run(new BuildWikipediaIndex(), args);
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			LOG.info("result: " + res);
			System.exit(res);
		}
	}
}
