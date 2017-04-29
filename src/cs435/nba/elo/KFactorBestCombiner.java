package cs435.nba.elo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KFactorBestCombiner extends Reducer<DoubleWritable, IdEloWritable, DoubleWritable, IdEloWritable> {

	@Override
	public void reduce(DoubleWritable key, Iterable<IdEloWritable> values, Context context)
			throws IOException, InterruptedException {

		// Key is kFactor
		// values are IdEloWritable
		List<Double> highestValues = new ArrayList<Double>();
		int numElosAdded = 0;
		List<IdEloWritable> idEloList = new ArrayList<IdEloWritable>();
		for (IdEloWritable value : values) {

			// Add everything til we get 10
			if (numElosAdded < 10) {

				// Add it
				highestValues.add(value.getElo());
				// sort using DoubleGreaterComparator so highest values come
				// first instead of lowest
				Collections.sort(highestValues, new DoubleGreaterComparator());
				idEloList.add(new IdEloWritable(value));
				++numElosAdded;

			} else {

				// After we have 10 only add if it is higher than the tenth
				// lowest we have
				if (value.getElo() > highestValues.get(9)) {
					highestValues.add(value.getElo());
					// sort using DoubleGreaterComparator so highest values come
					// first instead of lowest
					Collections.sort(highestValues, new DoubleGreaterComparator());
					idEloList.add(new IdEloWritable(value));
				}

			}
		}

		Collections.sort(idEloList);

		// Output top 10
		Set<IdEloWritable> idsWritten = new HashSet<IdEloWritable>();
		for (int index = 0; index < idEloList.size() && idsWritten.size() < 10; ++index) {

			IdEloWritable idElo = idEloList.get(index);
			if (!idsWritten.contains(idElo)) {

				idsWritten.add(idElo);
				context.write(key, idEloList.get(index));
			}
		}

	}

}
