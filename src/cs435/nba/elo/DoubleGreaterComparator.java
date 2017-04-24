package cs435.nba.elo;

import java.util.Comparator;

public class DoubleGreaterComparator implements Comparator<Double> {

	@Override
	public int compare(Double one, Double two) {

		// This is to sort Doubles by descending value
		// We want the greater double to be "less than"
		if (one == null && two == null) {
			return 0;
		} else if (one == null) {
			return 1;
		} else if (two == null) {
			return -1;
		} else {

			return -1 * Double.compare(one, two);
		}

	}

}
