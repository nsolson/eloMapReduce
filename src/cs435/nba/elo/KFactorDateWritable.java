package cs435.nba.elo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This class is so that the games are sorted correctly when they get to our
 * mapper. We will override the equals to only compare the K-Factor so all K
 * factors still go to the same reducer, but they will show up in correct order
 * because we will override the compareTo method to compare the year
 * 
 * @author nate
 *
 */
public class KFactorDateWritable implements WritableComparable<KFactorDateWritable> {

	/**
	 * The K Factor
	 */
	private int kFactor;

	/**
	 * The year
	 */
	private int year;

	/**
	 * The month
	 */
	private int month;

	/**
	 * The day
	 */
	private int day;

	/**
	 * Default constructor, required by Hadoop
	 */
	public KFactorDateWritable() {
		this(Constants.INVALID_STAT, Constants.INVALID_DATE, Constants.INVALID_DATE, Constants.INVALID_DATE);
	}

	/**
	 * Constructor, sets all necessary member variables
	 * 
	 * @param kFactor
	 *            The K Factor
	 * @param year
	 *            The year of the game
	 * @param month
	 *            The month of the game
	 * @param day
	 *            The day of the game
	 */
	public KFactorDateWritable(int kFactor, int year, int month, int day) {
		this.kFactor = kFactor;
		this.year = year;
		this.month = month;
		this.day = day;
	}

	/**
	 * @return {@link KFactorDateWritable#kFactor}
	 */
	public int getKFactor() {
		return kFactor;
	}

	/**
	 * @return {@link KFactorDateWritable#year}
	 */
	public int getYear() {
		return year;
	}

	/**
	 * @return {@link KFactorDateWritable#month}
	 */
	public int getMonth() {
		return month;
	}

	/**
	 * @return {@link KFactorDateWritable#day}
	 */
	public int getDay() {
		return day;
	}

	/**
	 * Reads member variables from HDFS
	 * 
	 * @param in
	 *            The {@link DataInput}
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		kFactor = Integer.parseInt(WritableUtils.readString(in));
		year = Integer.parseInt(WritableUtils.readString(in));
		month = Integer.parseInt(WritableUtils.readString(in));
		day = Integer.parseInt(WritableUtils.readString(in));
	}

	/**
	 * Writes member variables to HDFS
	 * 
	 * @param out
	 *            The {@link DataOutput}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, Integer.toString(kFactor));
		WritableUtils.writeString(out, Integer.toString(year));
		WritableUtils.writeString(out, Integer.toString(month));
		WritableUtils.writeString(out, Integer.toString(day));
	}

	/**
	 * Compares using dates to ensure things end up in correct order
	 * 
	 * @param other
	 *            The other {@link KFactorDateWritable} to compare
	 * @return A negative, positive or zero if this instance is earlier than the
	 *         given one
	 */
	@Override
	public int compareTo(KFactorDateWritable other) {

		if (other == null) {
			return -1;
		}

		if (year == other.getYear()) {

			if (month == other.getMonth()) {

				if (day == other.getDay()) {

					// same date, return 0
					return 0;

				} else {

					// Month and year the saem
					// day - otherDay will be negative if our day is earlier,
					// positive if our day is later
					return day - other.getDay();
				}
			} else {

				// year is equal
				// month - otherMonth will be negative if our month is earlier,
				// positive if our month is later
				return month - other.getMonth();
			}

		} else {

			// year - otherYear will be negative if our year is earlier,
			// positive if our year is later
			return year - other.getYear();
		}
	}

	/**
	 * This function will only compare K-Factors, that way we ensure all of the
	 * same K values get sent to the same reducer
	 * 
	 * @param o
	 *            The object to compare this one too
	 * @return true If The given object is a {@link KFactorDateWritable} and has
	 *         the same {@link KFactorDateWritable#kFactor} as this one, false
	 *         otherwise
	 */
	@Override
	public boolean equals(Object o) {

		if (o == null) {
			return false;
		}

		if (o instanceof KFactorDateWritable) {

			KFactorDateWritable other = (KFactorDateWritable) o;
			return kFactor == other.getKFactor();

		} else {
			return false;
		}
	}

	/**
	 * We need to override the toString method to give consistent results across
	 * JVMs so we can use this string for {@link KFactorDateWritable#hashCode}
	 * 
	 * @return The {@link String} representation of this class
	 */
	@Override
	public String toString() {

		return kFactor + " " + year + "/" + month + "/" + day;
	}

	// Don't need hashCode because I am setting a custom partioner for this

}
