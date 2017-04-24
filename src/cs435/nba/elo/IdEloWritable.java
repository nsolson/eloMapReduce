package cs435.nba.elo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class IdEloWritable implements WritableComparable<IdEloWritable> {

	/**
	 * The Id of the team or player
	 */
	private String id;

	/**
	 * The year of the elo
	 */
	private int year;

	/**
	 * The month of the elo
	 */
	private int month;

	/**
	 * The day of the elo
	 */
	private int day;

	/**
	 * The elo value
	 */
	private double elo;

	/**
	 * Required for hadoop
	 */
	public IdEloWritable() {
		this(Constants.INVALID_ID, Constants.INVALID_DATE, Constants.INVALID_DATE, Constants.INVALID_DATE,
				Constants.INVALID_STAT);
	}

	/**
	 * Constructor
	 * 
	 * @param id
	 *            The id of the team or player
	 * @param year
	 *            The year of the elo value
	 * @param month
	 *            The month of the elo value
	 * @param day
	 *            the day of the elo value
	 * @param elo
	 *            The elo value
	 */
	public IdEloWritable(String id, int year, int month, int day, double elo) {
		this.id = id;
		this.year = year;
		this.month = month;
		this.day = day;
		this.elo = elo;
	}

	/**
	 * Copy constructor
	 * 
	 * @param other
	 *            The other {@link IdEloWritable} objec tto copy
	 */
	public IdEloWritable(IdEloWritable other) {
		this.id = new String(other.getId());
		this.year = other.getYear();
		this.month = other.getMonth();
		this.day = other.getDay();
		this.elo = other.getElo();
	}

	/**
	 * @return {@link IdEloWritable#id}
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return {@link IdEloWritable#year}
	 */
	public int getYear() {
		return year;
	}

	/**
	 * @return {@link IdEloWritable#month}
	 */
	public int getMonth() {
		return month;
	}

	/**
	 * @return {@link IdEloWritable#day}
	 */
	public int getDay() {
		return day;
	}

	/**
	 * @return {@link IdEloWritable#elo}
	 */
	public double getElo() {
		return elo;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		id = WritableUtils.readString(in);
		year = Integer.parseInt(WritableUtils.readString(in));
		month = Integer.parseInt(WritableUtils.readString(in));
		day = Integer.parseInt(WritableUtils.readString(in));
		elo = Double.parseDouble(WritableUtils.readString(in));
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, id);
		WritableUtils.writeString(out, Integer.toString(year));
		WritableUtils.writeString(out, Integer.toString(month));
		WritableUtils.writeString(out, Integer.toString(day));
		WritableUtils.writeString(out, Double.toString(elo));
	}

	@Override
	public int compareTo(IdEloWritable other) {

		if (other == null) {
			return -1;
		}

		// We want the higher elo value to be ordered first or "less than"
		// otherElo - thisElo should give us this order
		return (int) (other.getElo() - elo);
	}

	@Override
	public boolean equals(Object o) {

		if (o == null) {
			return false;
		}

		if (o instanceof IdEloWritable) {

			IdEloWritable other = (IdEloWritable) o;
			return id.equals(other.getId());

		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}
}
