package cs435.nba.elo;

import java.util.HashMap;
import java.util.Map;

/**
 * Singleton class holds the league average for stats for season years Stats
 * from: http://www.basketball-reference.com/leagues/NBA_stats.html
 * 
 * @author nolson
 *
 */
public class LeagueStats {

	private static LeagueStats leagueStats = null;
	private Map<Integer, Stats> stats;

	private static final int MAX_SEASON = 2017;
	private static final int MIN_SEASON = 1947;

	/**
	 * Private constructor since this class is a singleton
	 */
	private LeagueStats() {

		stats = new HashMap<Integer, Stats>();
		stats.put(2017, new Stats(0.514, 12.7));
		stats.put(2016, new Stats(0.502, 13.2));
		stats.put(2015, new Stats(0.496, 13.3));
		stats.put(2014, new Stats(0.501, 13.6));
		stats.put(2013, new Stats(0.496, 13.7));
		stats.put(2012, new Stats(0.487, 13.8));
		stats.put(2011, new Stats(0.498, 13.4));
		stats.put(2010, new Stats(0.501, 13.3));
		stats.put(2009, new Stats(0.5, 13.3));
		stats.put(2008, new Stats(0.497, 13.2));
		stats.put(2007, new Stats(0.496, 14.2));
		stats.put(2006, new Stats(0.49, 13.7));
		stats.put(2005, new Stats(0.482, 13.6));
		stats.put(2004, new Stats(0.471, 14.2));
		stats.put(2003, new Stats(0.474, 14));
		stats.put(2002, new Stats(0.477, 13.6));
		stats.put(2001, new Stats(0.473, 14.1));
		stats.put(2000, new Stats(0.478, 14.2));
		stats.put(1999, new Stats(0.466, 14.6));
		stats.put(1998, new Stats(0.478, 14.5));
		stats.put(1997, new Stats(0.493, 14.8));
		stats.put(1996, new Stats(0.499, 14.7));
		stats.put(1995, new Stats(0.5, 14.6));
		stats.put(1994, new Stats(0.485, 14.3));
		stats.put(1993, new Stats(0.491, 14));
		stats.put(1992, new Stats(0.487, 13.6));
		stats.put(1991, new Stats(0.487, 13.9));
		stats.put(1990, new Stats(0.489, 13.9));
		stats.put(1989, new Stats(0.489, 14.5));
		stats.put(1988, new Stats(0.489, 14.3));
		stats.put(1987, new Stats(0.488, 14.3));
		stats.put(1986, new Stats(0.493, 14.9));
		stats.put(1985, new Stats(0.496, 14.9));
		stats.put(1984, new Stats(0.495, 15));
		stats.put(1983, new Stats(0.488, 15.8));
		stats.put(1982, new Stats(0.495, 15));
		stats.put(1981, new Stats(0.489, 15.6));
		stats.put(1980, new Stats(0.486, 15.5));
		stats.put(1979, new Stats(0.485, 16));
		stats.put(1978, new Stats(0.469, 16));
		stats.put(1977, new Stats(0.465, 16.5));
		stats.put(1976, new Stats(0.458, 16));
		stats.put(1975, new Stats(0.457, 16.3));
		stats.put(1974, new Stats(0.459, 16.5));
		stats.put(1973, new Stats(0.456, 16.5));
		stats.put(1972, new Stats(0.455, 16.5));
		stats.put(1971, new Stats(0.449, 16.5));
		stats.put(1970, new Stats(0.46, 16.5));
		stats.put(1969, new Stats(0.441, 16.5));
		stats.put(1968, new Stats(0.446, 16.5));
		stats.put(1967, new Stats(0.441, 16.5));
		stats.put(1966, new Stats(0.433, 16.5));
		stats.put(1965, new Stats(0.426, 16.5));
		stats.put(1964, new Stats(0.433, 16.5));
		stats.put(1963, new Stats(0.441, 16.5));
		stats.put(1962, new Stats(0.426, 16.5));
		stats.put(1961, new Stats(0.415, 16.5));
		stats.put(1960, new Stats(0.41, 16.5));
		stats.put(1959, new Stats(0.395, 16.5));
		stats.put(1958, new Stats(0.383, 16.5));
		stats.put(1957, new Stats(0.38, 16.5));
		stats.put(1956, new Stats(0.387, 16.5));
		stats.put(1955, new Stats(0.385, 16.5));
		stats.put(1954, new Stats(0.372, 16.5));
		stats.put(1953, new Stats(0.37, 16.5));
		stats.put(1952, new Stats(0.367, 16.5));
		stats.put(1951, new Stats(0.357, 16.5));
		stats.put(1950, new Stats(0.34, 16.5));
		stats.put(1949, new Stats(0.327, 16.5));
		stats.put(1948, new Stats(0.284, 16.5));
		stats.put(1947, new Stats(0.279, 16.5));

	}

	/**
	 * Method used to get the singleton instance of this class
	 * 
	 * @return The singleton instance of {@link LeagueStats}
	 */
	public static LeagueStats getInstance() {

		if (leagueStats == null) {
			leagueStats = new LeagueStats();
		}

		return leagueStats;
	}

	/**
	 * 
	 * @param season
	 *            The season to get the effective field goal percentage of
	 * @return The effective field goal percentage of the given season
	 */
	public double getEffectiveFieldGoalPercent(int season) {

		if (season > MAX_SEASON) {

			season = MAX_SEASON;

		} else if (season < MIN_SEASON) {

			season = MIN_SEASON;
		}

		return stats.get(season).getEFGPct();
	}

	/**
	 * @param season
	 *            The season to get the turnover percentage of
	 * @return The turnover percentage of the given season
	 */
	public double getTurnoverPercent(int season) {

		if (season > MAX_SEASON) {

			season = MAX_SEASON;

		} else if (season < MIN_SEASON) {

			season = MIN_SEASON;
		}

		return stats.get(season).getToPct();
	}

	private class Stats {

		/**
		 * The effective field goal percentage for the year
		 */
		private double eFGPct;

		/**
		 * The turnover percentage for the year
		 */
		private double toPct;

		/**
		 * Constructs the object
		 * 
		 * @param eFGPct
		 *            The effective field goal percentage for the year
		 * @param toPct
		 *            The turnover percentage for the year
		 */
		public Stats(double eFGPct, double toPct) {
			// For some reason FG pct is not a percent
			this.eFGPct = eFGPct * 100;
			this.toPct = toPct;
		}

		/**
		 * @return {@link Stats#eFGPct}
		 */
		public double getEFGPct() {
			return eFGPct;
		}

		/**
		 * @return {@link Stats#toPct}
		 */
		public double getToPct() {
			return toPct;
		}
	}

}
