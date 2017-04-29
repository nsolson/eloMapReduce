package cs435.nba.elo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecommendationRankReducer extends Reducer<Text, Text, NullWritable, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Everybody mapped to this reducer
		// value = first last team eloPerMillion elo salary

		List<PlayerEloSalaryWritable> playerList = new ArrayList<PlayerEloSalaryWritable>();
		for (Text value : values) {

			String[] tokens = value.toString().split("\\s+");
			if (tokens.length == 6) {

				String name = new String(tokens[0] + " " + tokens[1]);
				String team = new String(tokens[2]);
				// Dont care double eloPerMillion =
				// Double.parseDouble(tokens[3]);
				double elo = Double.parseDouble(tokens[4]);
				double salary = Double.parseDouble(tokens[5]);

				playerList.add(new PlayerEloSalaryWritable("", name, team, elo, salary));
			}
		}

		Collections.sort(playerList);
		for (PlayerEloSalaryWritable player : playerList) {

			String str = player.getName() + "\t" + player.getTeam() + "\t" + player.getEloPerMillion() + "\t"
					+ player.getElo() + "\t" + player.getSalaryInMillions();
			context.write(NullWritable.get(), new Text(str));
		}
	}

}
