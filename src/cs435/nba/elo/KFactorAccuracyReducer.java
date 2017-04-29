package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KFactorAccuracyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		// Key = K Factor
		// Value = 1 if correctly predicted, 0 if incorrectly predicted
		double correctPredictions = 0;
		int totalPredictions = 0;
		for (IntWritable value : values) {

			correctPredictions += value.get();
			++totalPredictions;
		}

		// System.out.println("K: " + key.toString() + " correct: " +
		// correctPredictions + " total: " + totalPredictions);

		double percentCorrect = 100 * (correctPredictions / totalPredictions);
		context.write(key, new DoubleWritable(percentCorrect));
	}
}
