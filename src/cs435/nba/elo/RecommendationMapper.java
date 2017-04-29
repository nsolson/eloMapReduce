package cs435.nba.elo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// This map gets 2 files
		// First file is: playerId teamId endElo
		// Seoncd file is: firstName lastName playerId salary
		String[] tokens = value.toString().split("\\s+");
		if (tokens.length == 3) {

			// elo file
			String playerId = tokens[0];
			String teamId = tokens[1];
			String endElo = tokens[2];
			context.write(new Text(playerId), new Text(teamId + "\t" + endElo));

		} else if (tokens.length == 4) {

			// salary file
			String firstName = tokens[0];
			String lastName = tokens[1];
			String playerId = tokens[2];
			String salary = tokens[3];
			context.write(new Text(playerId), new Text(firstName + "\t" + lastName + "\t" + salary));
		}
	}

}
