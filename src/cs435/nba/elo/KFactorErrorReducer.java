package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KFactorErrorReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		// Key = K Factor
		// Value = 1 if correctly predicted, 0 if incorrectly predicted
		double correctPredictions = 0;
		for (DoubleWritable value : values) {

			correctPredictions += value.get();
		}

		// System.out.println("K: " + key.toString() + " correct: " +
		// correctPredictions + " total: " + totalPredictions);

		context.write(key, new DoubleWritable(correctPredictions));
	}
}
