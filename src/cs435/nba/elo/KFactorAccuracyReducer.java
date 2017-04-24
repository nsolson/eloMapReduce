package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KFactorAccuracyReducer extends Reducer<DoubleWritable, IntWritable, DoubleWritable, DoubleWritable> {

	@Override
	public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		// Key = K Factor
		// Value = 1 if correctly predicted, 0 if incorrectly predicted
		int correctPredictions = 0;
		int totalPredictions = 0;
		for (IntWritable value : values) {

			correctPredictions += value.get();
			++totalPredictions;
		}

		double percentCorrect = 100 * ((double) correctPredictions / (double) totalPredictions);
		context.write(key, new DoubleWritable(percentCorrect));
	}
}
