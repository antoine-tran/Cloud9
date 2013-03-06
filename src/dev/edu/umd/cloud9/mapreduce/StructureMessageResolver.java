package edu.umd.cloud9.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;


/**
 * A Hadoop reducer that accepts both content messages and structure 
 * messages, and update the title of all content messages with the 
 * info from the structure message.
 */ 
public abstract class StructureMessageResolver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
		extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	
	private VALUEOUT valueOut = newOutputValue();
	private KEYOUT keyOut = newOutputKey();
	
	@Override
	protected void reduce(KEYIN key, Iterable<VALUEIN> values, 
			Context context) throws IOException, InterruptedException {

		// the sentinel indicating whether we encounter the structure message
		// along the iterator
		VALUEIN smsg = null;
		
		// in-memory caches to emit content messages arriving before the 
		// structure message
		// TODO: Might use external-memory-based collection library (BerkeleyDB etc.)
		// if the iterator is too big
		List<VALUEIN> cache = new ArrayList<VALUEIN>();
		VALUEIN tmpItem;

		for (VALUEIN value : values) {
			if (checkStructureMessage(key, keyOut, value)) {
				smsg = value;
			}			

			// items before the structure messages in the iterator will
			// be copied and be emitted later
			else if (smsg == null) {
				tmpItem = clone(value);
				cache.add(tmpItem);
			}
			else emit(context, key, smsg, value, keyOut, valueOut);
		}

		// No structure messages found
		if (smsg == null) {
			noHit(context, key, cache, keyOut, valueOut);
		}
				
		// second run: update the remaining links with actual destination
		else for (VALUEIN v : cache) {
			emit(context, key, smsg, v, keyOut, valueOut);
		}		
	}	
	
	/** implement structure message checking logic. The output key might be updated
	 * right away */
	public abstract boolean checkStructureMessage(KEYIN key, KEYOUT keySingletonToUpdate,
			VALUEIN msg);
		
	/** clone the current message for possible subsequent emission */
	public abstract VALUEIN clone(VALUEIN t);
	
	/** instantiate one dummy VALUEOUT object to cache the emitted messages */
	public abstract VALUEOUT newOutputValue();
	
	/** instantiate one dummy KEYOUT object to cache the emitted messages */
	public abstract KEYOUT newOutputKey();
	
	/** after the structure message in known, subsequent messages are all content messages.
	 * Apply some post-checking logics and emit them immediately if passed.
	 * NOTE: At this point, structure message cannot be null !! */
	public abstract void emit(Context context, KEYIN key, VALUEIN structureMsg, 
		VALUEIN msg, KEYOUT keySingleton, VALUEOUT valueSingleton) throws IOException, InterruptedException;
	
	/** what to do if no structure messages found ? */
	public abstract void noHit(Context context, KEYIN key, Iterable<VALUEIN> cache,
		KEYOUT keySingleton, VALUEOUT valueSingleton) throws IOException, InterruptedException; 
}
