package cs435.nba.elo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class KFactorAccuracyWritable implements WritableComparable<KFactorAccuracyWritable> {

	/**
	 * The K Factor we are testing
	 */
	private double kFactor;

	/**
	 * The number of correct predictions we have made
	 */
	private double accuracyPercent;

	/**
	 * Required by HDFS
	 */
	public KFactorAccuracyWritable() {
		this(Constants.INVALID_STAT, 0);
	}

	/**
	 * Constructor
	 * 
	 * @param kFactor
	 *            The K Factor we are testing
	 * @param accuracyPercent
	 *            The accuracy of this KFactor
	 */
	public KFactorAccuracyWritable(double kFactor, double accuracyPercent) {

		this.kFactor = kFactor;
		this.accuracyPercent = accuracyPercent;
	}

	/**
	 * Copy constructor
	 * 
	 * @param other
	 *            The other {@link KFactorAccuracyWritable} to copy
	 */
	public KFactorAccuracyWritable(KFactorAccuracyWritable other) {

		if (other != null) {
			this.kFactor = other.getKFactor();
			this.accuracyPercent = other.getAccuracyPercent();
		} else {
			this.kFactor = Constants.INVALID_STAT;
			this.accuracyPercent = 0;
		}
	}

	/**
	 * @return {@link KFactorAccuracyWritable#kFactor}
	 */
	public double getKFactor() {
		return kFactor;
	}

	/**
	 * @return {@link KFactorAccuracyWritable#accuracyPercent}
	 */
	public double getAccuracyPercent() {
		return accuracyPercent;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		kFactor = Double.parseDouble(WritableUtils.readString(in));
		accuracyPercent = Double.parseDouble(WritableUtils.readString(in));
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, Double.toString(kFactor));
		WritableUtils.writeString(out, Double.toString(accuracyPercent));
	}

	@Override
	public int compareTo(KFactorAccuracyWritable other) {

		if (other == null) {
			return -1;
		}

		// We want the higher accuracy to be first
		// So higher accuracy is "less than"
		double thisAccuracy = getAccuracyPercent();
		double otherAccuracy = other.getAccuracyPercent();

		if (thisAccuracy > otherAccuracy) {
			return -1;
		} else if (thisAccuracy < otherAccuracy) {
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public boolean equals(Object o) {

		if (o == null) {
			return false;
		}

		if (o instanceof KFactorAccuracyWritable) {

			KFactorAccuracyWritable other = (KFactorAccuracyWritable) o;
			return kFactor == other.getKFactor();

		} else {

			return false;
		}
	}

	@Override
	public String toString() {
		return kFactor + "\t" + accuracyPercent;
	}

	@Override
	public int hashCode() {

		return (int) kFactor;
	}
}
