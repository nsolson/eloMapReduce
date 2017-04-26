package cs435.nba.elo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KFactorAccuracyRankReducer
		extends Reducer<Text, KFactorAccuracyWritable, NullWritable, KFactorAccuracyWritable> {

	@Override
	public void reduce(Text key, Iterable<KFactorAccuracyWritable> values, Context context)
			throws IOException, InterruptedException {

		// key = "key"
		// value = all k factor accuracies
		List<KFactorAccuracyWritable> kFactorList = new ArrayList<KFactorAccuracyWritable>();
		for (KFactorAccuracyWritable value : values) {

			kFactorList.add(new KFactorAccuracyWritable(value));
		}

		Collections.sort(kFactorList);
		for (KFactorAccuracyWritable kFactor : kFactorList) {
			context.write(NullWritable.get(), kFactor);
		}
	}
}
