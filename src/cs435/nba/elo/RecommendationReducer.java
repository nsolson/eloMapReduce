package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecommendationReducer extends Reducer<Text, Text, PlayerEloSalaryWritable, NullWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// playerId is key
		// Gets 2 different inputs:
		// 1. teamId endElo
		// 2. firstName lastName salary
		String playerId = key.toString();
		String teamId = null;
		String name = null;
		String endElo = null;
		String salary = null;
		for (Text value : values) {

			String[] tokens = value.toString().split("\\s+");
			if (tokens.length == 2) {

				teamId = new String(tokens[0]);
				endElo = new String(tokens[1]);

			} else if (tokens.length == 3) {

				name = new String(tokens[0] + " " + tokens[1]);
				salary = new String(tokens[2]);
			}
		}

		if (teamId != null && name != null && endElo != null && salary != null) {

			PlayerEloSalaryWritable player = new PlayerEloSalaryWritable(playerId, name, teamId,
					Double.parseDouble(endElo), Double.parseDouble(salary));
			context.write(player, NullWritable.get());
		}
	}
}
