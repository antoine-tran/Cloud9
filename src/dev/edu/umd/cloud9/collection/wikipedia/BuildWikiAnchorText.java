package edu.umd.cloud9.collection.wikipedia;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.IOException;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * A Hadoop job that extracts anchor text from Wikipedia dump
 */
public class BuildWikiAnchorText extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(BuildWikiAnchorText.class);

	private static final String LANG_OPTION = "lang";
	private static final String INPUT_OPTION = "input";
	private static final String REDUCE_NO = "reduce";
	private static final String PHASE = "phase";

	/** Map phase : Parse one single Wikipedia page and emits, for each outgoing 
	 * links in the text, a (destinationLink, anchor) pair */
	private static final class EmitAnchorMapper extends
	Mapper<LongWritable, WikipediaPage, Text, PairOfStringInt> {

		private Text outKey = new Text();
		private PairOfStringInt outVal = new PairOfStringInt();
		private Object2IntOpenHashMap<PairOfStrings> map = new Object2IntOpenHashMap<PairOfStrings>();

		@Override
		protected void map(LongWritable key, WikipediaPage p,
				Context context) throws IOException, InterruptedException {
			
			// only articles are emitted
			boolean redirected = false;
			if (p.isRedirect()) {
				redirected = true;
			} else if (!p.isArticle()) return;		
			String title = p.getTitle().trim();

			// to make the title case-sensitive, we will change all lower-cased
			// first characters to upper-case.
			if (title.isEmpty()) return;
			title = WordUtils.capitalize(title);
			
			// do not pass the structure message of a redirect article
			if (!redirected) {
				outKey.set(title);
				int id = Integer.parseInt(p.getDocid());
				outVal.set(title, id);
				context.write(outKey, outVal);	
			}			

			for (PairOfStrings t : p.extractAnchoredLinks()) {
				String link = t.getLeftElement().trim();
				if (link.isEmpty()) continue;
				link = WordUtils.capitalize(link);
				if (title.equals(link)) continue;
				if (redirected) {
					outKey.set(title);
					outVal.set(link, -1);
					context.write(outKey, outVal);	
					return;
				} else {			
					if (!map.containsKey(t)) {
						map.put(t, 1);
					}
					else {
						int v = map.getInt(t);
						map.put(t, v + 1);
					}
				}
			}
			PairOfStrings[] keys = map.keySet().toArray((new PairOfStrings[map.size()]));

			for (PairOfStrings k : keys) {
				if (k.getLeftElement().isEmpty()) continue;				
				outKey.set(k.getLeftElement());
				int cnt = map.get(k);
				outVal.set(k.getRightElement(), cnt);
				context.write(outKey, outVal);	
			}	
		}		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
