package hints;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import utilities.LookupService;
import utilities.TweetWritable;

/**
 * The SentimentMapper scores raw tweet data from Twitter.
 * 
 * 
 * The goal of the SentimentMapper is to examine every word in every given tweet, asking:
 * 
 * 1) does this word indicate a feeling, sentiment or attitude?
 * 
 * 2) if so, what is the affective score for the word? (Scores are found in a lookup table)
 * 
 * As the tweet is processed, the score for words found in the lookup table are summed.
 * 
 * When the Mapper is done, we will have the following information: - every tweet will have ---- a total score, ---- a
 * size, indicating how many words are in the tweet ---- a hit rate, indicating how many times a word in the tweet was
 * also found in the lookup table
 * 
 * To store this information, you may or may not find it useful to use the TweetWritable.class found in utilities.
 * 
 * 
 * 
 * In this first version of Sentiment processing, we are only processing one topic - tweets about the Clippers on the
 * evening of May 1, 2017
 * 
 * 
 * @author elizabeth corey
 *
 */
public class SentimentMapper extends Mapper<LongWritable, Text, Text, TweetWritable> {

	private static final String LOOKUP_TABLE_PROPERTY = "lookupTable";

	private static final Log LOG = LogFactory.getLog(SentimentMapper.class);

	public static LookupService lookupService;

	/**
	 * Before the Mapper starts processing any data, it needs to setup
	 * 
	 * 1) a new lookup service using the lookup table cached in the driver
	 * 
	 * 2) a key for the data being processed by this Mapper. We will use the input split's filename as the key.
	 * 
	 * In this case, the filename is "Clippers_5_1" - so we have information about the tweet topic embedded in the
	 * filename.
	 */
	@Override
	public void setup(Context context) throws IOException {

		// Get the name of the lookup table or use AFINN-111 if none set
		String lookupName = context.getConfiguration().get(LOOKUP_TABLE_PROPERTY, "AFINN-111");

		URI lookupFile = null;
		// retrieve the lookup table from the cache using context.getCacheFiles();
		/*
		 * TODO implement
		 */
		URI files[] = context.getCacheFiles();
		lookupFile = context.getCacheFiles()[0];
		LOG.info("Using lookup table " + lookupFile.getPath().toString());

		// Create and initialize the lookupService using lookupTableURI
		/*
		 * TODO implement
		 */
		lookupService = new LookupService();
		lookupService.initialize(lookupFile);

		// get the name of the input file from the split and use if for the Mapper's TOPIC
		Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
		TOPIC.set(inputPath.getName());

	}

	private static final Text TOPIC = new Text();
	private static final TweetWritable TWEET = new TweetWritable();

	@Override
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

		/*
		 * Convert the line, which is received as a Text object, to a String object.
		 */
		/*
		 * TODO implement
		 */
		String lines = line.toString();

		/*
		 * Use the parseText method to get the tweet's text.
		 */
		/*
		 * TODO implement
		 */
		// LOG.info(lines);

		String tweets = parseText(lines);
		if (tweets != null) {
			Long tweetID = getId(lines);

			/*
			 * The line.split("\\W+") call uses regular expressions to split the line up by non-word characters. You can
			 * use it and then iterate through resulting array of words
			 * 
			 */
			/*
			 * TODO implement - start your loop here
			 */
			int score = 0;
			int nHits = 0;
			String[] tweet = tweets.split("\\W+");
			for (int i = 0; i < tweet.length; i++) {

				/*
				 * Obtain the first word and use the lookupService that you initialized during setup to search for the
				 * word in your sentiment list
				 */
				/*
				 * TODO implement
				 */

				if (lookupService.get(tweet[i]) != null) {
					score = score + lookupService.get(tweet[i]);
					nHits += 1;
				}

				/*
				 * If the word is found in the sentiment list, get the number associated with that word. Add the number
				 * to the score for the tweet.
				 */
				/*
				 * TODO implement and then end your loop here
				 */
			}

			/*-
			 * Using the information you gathered when processing the tweet, create a new utilities.TweetWritable.
			 * Use the setters on TweetWritable to set the following: 
			 * 		tweetId
			 * 		tweet 
			 * 		nHits (the number of times a word in the tweet was found in the lookup table)
			 * 		size (the number of words in a tweet)
			 * 		score		
			 */
			// TweetWritable tw = new TweetWritable();
			TWEET.setTweetId(tweetID);
			TWEET.setTweet(tweets);
			TWEET.setNHits(nHits);
			TWEET.setScore(score);
			TWEET.setSize(tweet.length - 1);

			/*
			 * Call the write method on the Context object to emit a key and a value from the map method. The key is the
			 * TOPIC defined in the setter; the value is TweetWritable.
			 */
			/*
			 * TODO implement
			 */
			context.write(TOPIC, TWEET);
			// LOG.info(score);
		}

	}

	/**
	 * Parse our quirky data. This doesn't generalize.
	 * 
	 * @param string
	 * 
	 * @return String containing the textual part of the tweet (or null, if the record is invalid)
	 */

	String parseText(String string) {

		/*
		 * Make sure the first part is a id -- id example: 858950241151840256. An id is an integer with 18 digits
		 */
		try {
			if (string.length() < 18)
				return null;
			Long.parseLong(string.substring(0, 17));
		} catch (NumberFormatException e) {
			return null; // not a valid id
		}

		/*
		 * If the records starts with a valid id, then the rest of the record is the tweet's text and we can return it.
		 * 
		 */
		return string.substring(18);

	}

	long getId(String string) {
		return Long.parseLong(string.substring(0, 17));
	}

}
