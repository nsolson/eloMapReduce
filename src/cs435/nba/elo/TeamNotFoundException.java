package cs435.nba.elo;

/**
 * Represents if a team is not found. Using this so we don't have to return and
 * check for null everywhere as that is easy to forget. This will force us to
 * think about a try/catch if this happens.
 * 
 * @author nate
 *
 */
public class TeamNotFoundException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7632274393735388229L;

	public TeamNotFoundException(String message) {
		super(message);
	}
}
