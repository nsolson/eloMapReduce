package cs435.nba.elo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Job;

public class JobGroup {

	private static final int SLEEP_SEC = 5;
	private static final int SLEEP_TIME = SLEEP_SEC * 1000;

	private String groupName;
	private List<Job> jobs;
	private List<String> jobNames;
	private List<Boolean> jobsComplete;
	private List<Boolean> jobsSuccessful;

	public JobGroup(String groupName) {

		this.groupName = groupName;
		this.jobs = new ArrayList<Job>();
		this.jobNames = new ArrayList<String>();
		this.jobsComplete = new ArrayList<Boolean>();
		this.jobsSuccessful = new ArrayList<Boolean>();
	}

	public void addJob(Job job, String jobName) {
		jobs.add(job);
		jobNames.add(jobName);
		jobsComplete.add(Boolean.FALSE);
		jobsSuccessful.add(Boolean.FALSE);
	}

	public void runAndWait() throws IOException, InterruptedException, ClassNotFoundException {

		for (Job job : jobs) {
			job.submit();
		}

		boolean allJobsComplete = false;

		float mapProgress = 0;
		float reduceProgress = 0;
		float prevMapProgress = -1;
		float prevReduceProgress = -1;
		System.out.println("Group " + groupName + ": Map (" + mapProgress * 100 + "%)" + "\tReduce ("
				+ reduceProgress * 100 + "%)");

		boolean progressUpdate = true;
		while (!allJobsComplete) {

			prevMapProgress = mapProgress;
			prevReduceProgress = reduceProgress;
			mapProgress = 0;
			reduceProgress = 0;

			allJobsComplete = true;
			for (int index = 0; index < jobs.size(); ++index) {

				Job job = jobs.get(index);

				if (!jobsComplete.get(index)) {

					jobsComplete.set(index, job.isComplete());

					if (jobsComplete.get(index)) {

						jobsSuccessful.set(index, job.isSuccessful());
						if (progressUpdate) {
							printJobStatus(jobNames.get(index), 1, 1);
						}
						mapProgress += 1;
						reduceProgress += 1;

					} else {

						float jobMapProgress = job.mapProgress();
						float jobReduceProgress = job.reduceProgress();
						if (progressUpdate) {
							printJobStatus(jobNames.get(index), jobMapProgress, jobReduceProgress);
						}
						mapProgress += jobMapProgress;
						reduceProgress += jobReduceProgress;

					}
				} else {

					mapProgress += 1;
					reduceProgress += 1;
				}

				allJobsComplete = allJobsComplete && jobsComplete.get(index);
			}

			mapProgress /= jobs.size();
			reduceProgress /= jobs.size();

			progressUpdate = prevMapProgress != mapProgress || prevReduceProgress != reduceProgress;
			Thread.sleep(SLEEP_TIME);

			if (progressUpdate) {
				System.out.println("Group " + groupName + ": Map (" + mapProgress * 100 + "%)" + "\tReduce ("
						+ reduceProgress * 100 + "%)");
			}
		}

	}

	public boolean isSuccessful() {
		boolean success = true;
		for (Boolean jobSuccessful : jobsSuccessful) {
			success = success && jobSuccessful;
		}
		return success;
	}

	public int getFirstUnsuccessful() {
		for (int index = 0; index < jobsSuccessful.size(); ++index) {
			boolean jobSuccessful = jobsSuccessful.get(index);
			if (!jobSuccessful) {
				return index + 1;
			}
		}
		return -1;
	}

	private void printJobStatus(String jobName, float mapProgress, float reduceProgress) throws IOException {
		System.out.println(
				"\t" + jobName + ": Map (" + mapProgress * 100 + "%) / Reduce (" + reduceProgress * 100 + "%)");
	}
}
