package cs435.nba.elo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Represents a player in a single game. Implements {@link WritableComparable}
 * so it can be used in Hadoop's map and reduce.
 * 
 * TODO Need to override the toString method of this to make printing to files
 * easier
 * 
 * @author nate
 *
 */
public class PlayerGameWritable implements WritableComparable<PlayerGameWritable> {

	/**
	 * The ID of the player
	 */
	private String playerId;

	/**
	 * The name of the player
	 */
	private String name;

	/**
	 * Total points scored by this player
	 */
	private int points;

	/**
	 * Total minutes played by this player
	 */
	private int minPlayed;

	/**
	 * Total rebounds by this player
	 */
	private int rebounds;

	/**
	 * Total assists by this player
	 */
	private int assists;

	/**
	 * Total steals by this player
	 */
	private int steals;

	/**
	 * Total blocks by this player
	 */
	private int blocks;

	/**
	 * Total turnovers by this player
	 */
	private int turnovers;

	/**
	 * Default constructor, required for Hadoop
	 */
	public PlayerGameWritable() {
		this(Constants.INVALID_ID, Constants.EMPTY_STRING, Constants.INVALID_STAT, Constants.INVALID_STAT,
				Constants.INVALID_STAT, Constants.INVALID_STAT, Constants.INVALID_STAT, Constants.INVALID_STAT,
				Constants.INVALID_STAT);
	}

	/**
	 * Constructor that initializes all values
	 * 
	 * @param playerId
	 *            The ID of this player
	 * @param name
	 *            The name of the player
	 * @param points
	 *            The points scored by this player
	 * @param minPlayed
	 *            Total minutes played by this player
	 * @param rebounds
	 *            Total rebounds by this player
	 * @param assists
	 *            Total assists by this player
	 * @param steals
	 *            Total steals by this player
	 * @param blocks
	 *            Total blocks by this player
	 * @param turnovers
	 *            Total turnovers by this player
	 */
	public PlayerGameWritable(String playerId, String name, int points, int minPlayed, int rebounds, int assists,
			int steals, int blocks, int turnovers) {

		this.playerId = playerId;
		this.name = name;
		this.points = points;
		this.minPlayed = minPlayed;
		this.rebounds = rebounds;
		this.assists = assists;
		this.steals = steals;
		this.blocks = blocks;
		this.turnovers = turnovers;
	}

	/**
	 * @return {@link PlayerGameWritable#playerId}
	 */
	public String getPlayerId() {
		return playerId;
	}

	/**
	 * @return {@link PlayerGameWritable#name}
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return {@link PlayerGameWritable#points}
	 */
	public int getPoints() {
		return points;
	}

	/**
	 * @return {@link PlayerGameWritable#minPlayed}
	 */
	public int getMinPlayed() {
		return minPlayed;
	}

	/**
	 * @return {@link PlayerGameWritable#rebounds}
	 */
	public int getRebounds() {
		return rebounds;
	}

	/**
	 * @return {@link PlayerGameWritable#assists}
	 */
	public int getAssists() {
		return assists;
	}

	/**
	 * @return {@link PlayerGameWritable#steals}
	 */
	public int getSteals() {
		return steals;
	}

	/**
	 * @return {@link PlayerGameWritable#blocks}
	 */
	public int getBlocks() {
		return blocks;
	}

	/**
	 * @return {@link PlayerGameWritable#turnovers}
	 */
	public int getTurnovers() {
		return turnovers;
	}

	/**
	 * Reads all the member variables from HDFS
	 * 
	 * @param in
	 *            The {@link DataInput}
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		playerId = WritableUtils.readString(in);
		name = WritableUtils.readString(in);
		points = Integer.parseInt(WritableUtils.readString(in));
		minPlayed = Integer.parseInt(WritableUtils.readString(in));
		rebounds = Integer.parseInt(WritableUtils.readString(in));
		assists = Integer.parseInt(WritableUtils.readString(in));
		steals = Integer.parseInt(WritableUtils.readString(in));
		blocks = Integer.parseInt(WritableUtils.readString(in));
		turnovers = Integer.parseInt(WritableUtils.readString(in));
	}

	/**
	 * Writes all the member variables to HDFS
	 * 
	 * @param out
	 *            {@link DataOutput}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, playerId);
		WritableUtils.writeString(out, name);
		WritableUtils.writeString(out, Integer.toString(points));
		WritableUtils.writeString(out, Integer.toString(minPlayed));
		WritableUtils.writeString(out, Integer.toString(rebounds));
		WritableUtils.writeString(out, Integer.toString(assists));
		WritableUtils.writeString(out, Integer.toString(steals));
		WritableUtils.writeString(out, Integer.toString(blocks));
		WritableUtils.writeString(out, Integer.toString(turnovers));
	}

	/**
	 * We don't really care how these are sorted (at least I don't think so). So
	 * I am just going to return the comparison of the teamID
	 * 
	 * @param other
	 *            The other {@link PlayerGameWritable} object to compare this
	 *            one to
	 * @return Negative, positive or 0 if this object is less than, greater than
	 *         or equal to the given object
	 */
	@Override
	public int compareTo(PlayerGameWritable other) {

		if (other == null) {
			return -1;
		}

		return playerId.compareTo(other.getPlayerId());
	}

	/**
	 * This function compares equality. The only thing we care about when
	 * comparing players is if their ID's are equal. If this object's ID is
	 * equal to {@link Constants#INVALID_ID} it should always return false
	 *
	 * @param o
	 *            The object to compare to
	 * @return true if:
	 *         <ul>
	 *         <li>The given object is an {@link PlayerGameWritable} with a
	 *         {@link PlayerGameWritable#playerId} equal to this one</li>
	 *         <li>The given object is an {@link PlayerEloWritable} with a
	 *         {@link PlayerEloWritable#getPlayerId} equal to this
	 *         {@link PlayerGameWritable#playerId}</li>
	 *         <li>If it is a {@link String} equal to this
	 *         {@link PlayerGameWritable#playerId}
	 *         <li>
	 *         </ul>
	 *         false otherwise
	 */
	@Override
	public boolean equals(Object o) {

		if (playerId.equals(Constants.INVALID_ID)) {
			return false;
		}

		if (o == null) {
			return false;
		}

		if (o instanceof PlayerGameWritable) {

			PlayerGameWritable other = (PlayerGameWritable) o;
			return playerId.equals(other.getPlayerId());

		} else if (o instanceof PlayerEloWritable) {

			PlayerEloWritable other = (PlayerEloWritable) o;
			return playerId.equals(other.getPlayerId());

		} else if (o instanceof String) {

			String otherPlayerId = (String) o;
			return playerId.equals(otherPlayerId);

		} else {

			// o not the same class
			return false;
		}

	}

	/**
	 * It is required in Hadoop to return consistent hashcodes across instances
	 * of JVMs. Luckily for us {@link String#hashCode} already does this. This
	 * is also so when adding to HashSets or HashMaps, we properly add or don't
	 * add.
	 * 
	 * @return The hashCode of {@link PlayerGameWritable#playerId}
	 */
	@Override
	public int hashCode() {
		return playerId.hashCode();
	}
}
