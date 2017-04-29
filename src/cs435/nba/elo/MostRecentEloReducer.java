package cs435.nba.elo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostRecentEloReducer extends Reducer<KFactorDateWritable, Text, NullWritable, Text> {

	@Override
	public void reduce(KFactorDateWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// Games are sorted earliest to latest so we can calc Elo one by one

		double kFactor = key.getKFactor();

		if ((int) kFactor == Constants.FINAL_K_FACTOR) {

			Map<String, PlayerEloWritable> playerEloMap = new HashMap<String, PlayerEloWritable>();
			for (Text text : values) {

				// Elos are sorted earliest to latest, therefore we just need to
				// keep overwriting Elos
				String value = text.toString();
				String[] tokens = value.split("\\s+");
				if (tokens.length == 3) {

					String playerId = tokens[0];
					String teamId = tokens[1];
					double endElo = Double.parseDouble(tokens[2]);

					if (!playerEloMap.containsKey(playerId)) {

						// PlayerEloWritable expects player, name, elo
						// just use name spot for team
						playerEloMap.put(new String(playerId), new PlayerEloWritable(playerId, teamId, endElo));
					}

					playerEloMap.get(playerId).setElo(endElo);
				}
			}

			for (String playerId : playerEloMap.keySet()) {

				PlayerEloWritable playerEloWritable = playerEloMap.get(playerId);
				String str = playerEloWritable.getPlayerId() + "\t" + playerEloWritable.getName() + "\t"
						+ playerEloWritable.getElo();
				context.write(NullWritable.get(), new Text(str));

			}
		}

	}
}
