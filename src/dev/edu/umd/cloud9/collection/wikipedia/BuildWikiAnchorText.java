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
import edu.umd.cloud9.mapreduce.StructureMessageResolver;

/**
 * A Hadoop job that extracts anchor text from Wikipedia dump
 */
public class BuildWikiAnchorText extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(BuildWikiAnchorText.class);

	private static final String LANG_OPTION = "lang";
	private static final String INPUT_OPTION = "input";
	private static final String REDUCE_NO = "reduce";
	private static final String PHASE = "phase";

	/** Map phase 1: Parse one single Wikipedia page and emits, for each outgoing 
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

	
	/** Reduce phase 1: Resolve the redirect links */
	private static final class RedirectResolveReducer extends 
			StructureMessageResolver<Text, PairOfStringInt, Text, PairOfStringInt> {

		@Override
		// Update the outkey on-the-fly
		public boolean checkStructureMessage(Text key,
				Text keySingletonToUpdate, PairOfStringInt msg) {
			int v = msg.getValue();	
			boolean redirected = (v == -1);
			if (redirected) keySingletonToUpdate.set(msg.getKey());
			else keySingletonToUpdate.set(key);
			return redirected;
		}

		@Override
		public PairOfStringInt clone(PairOfStringInt t) {
			return new PairOfStringInt(t.getKey(), t.getValue());
		}

		@Override
		public PairOfStringInt newOutputValue() {
			return new PairOfStringInt();
		}

		@Override
		public Text newOutputKey() {
			return new Text();
		}

		@Override
		public void emit(Context context, Text key, PairOfStringInt structureMsg, 
				PairOfStringInt msg, Text keySingleton, PairOfStringInt valueSingleton) 
				throws IOException, InterruptedException {
			
			// There might be still redirect pages that emit their pageIds. Ignore those
			if ((key.toString().equals(msg.getKey()))) return;
			
			// no need to update the out key - we did it in checkStructureMessage() already
			else context.write(keySingleton, msg);
		}

		@Override
		// The destination page is not a redirect. Emit everything to the phase 2
		public void noHit(Context context, Text key, 
				Iterable<PairOfStringInt> cache, Text keySingleton, 
				PairOfStringInt valueSingleton)	throws IOException, InterruptedException {
			for (PairOfStringInt v : cache) context.write(keySingleton, v);
		}
	}
	
	private static final class PageIdResolveReducer 
			extends StructureMessageResolver<Text, PairOfStringInt, Text, Text> {

		@Override
		// Update the output key on-the-fly
		public boolean checkStructureMessage(Text key,
				Text keySingletonToUpdate, PairOfStringInt msg) {
			String dest = key.toString();
			String source = msg.getKey(); 
			boolean redirected = (dest.equals(source));
			if (redirected) keySingletonToUpdate.set(String.valueOf(msg.getValue()));
			else keySingletonToUpdate.set(key);
			return redirected;
		}

		@Override
		public PairOfStringInt clone(PairOfStringInt t) {
			return new PairOfStringInt(t.getKey(), t.getValue());
		}

		@Override
		public Text newOutputValue() {			
			return new Text();
		}

		@Override
		public Text newOutputKey() {
			return new Text();
		}

		@Override
		public void emit(Context context, Text key, PairOfStringInt structureMsg,
				PairOfStringInt msg, Text keySingleton, Text valueSingleton) 
				throws IOException, InterruptedException {
			valueSingleton.set(msg.getKey() + "\t" + msg.getValue());
			context.write(keySingleton, valueSingleton);
		}

		@Override
		// We lost the structure message of this page. Report it !
		public void noHit(Context context, Text key,
				Iterable<PairOfStringInt> cache, Text keySingleton,	Text valueSingleton)
				throws IOException, InterruptedException {
			log.info("No structure message found for : " + key.toString());
		}
		
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
