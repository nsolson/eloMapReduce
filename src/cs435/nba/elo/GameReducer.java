package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GameReducer extends Reducer<Text, GameWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<GameWritable> values, Context context)
			throws IOException, InterruptedException {

		// This should be able to calculate Elo values?
	}
}
