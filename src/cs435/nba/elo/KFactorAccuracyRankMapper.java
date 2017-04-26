package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KFactorAccuracyRankMapper extends Mapper<LongWritable, Text, Text, KFactorAccuracyWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Value is: kFactor accuracy
		String[] tokens = value.toString().split("\\s+");
		if (tokens.length == 2) {

			double kFactor = Double.parseDouble(tokens[0]);
			double accuracy = Double.parseDouble(tokens[1]);

			KFactorAccuracyWritable kFactorAccuracy = new KFactorAccuracyWritable(kFactor, accuracy);

			context.write(new Text("key"), kFactorAccuracy);
		}
	}
}
