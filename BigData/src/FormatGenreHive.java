import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class FormatGenreHive extends UDF {
	public Text evaluate(Text genreList) {
		Text formattedGenre = new Text();
		if (genreList != null) {
			formattedGenre.set(formatGenre(genreList.toString()));
		}
		return formattedGenre;
	}

	private String formatGenre(String genreList) {
		String formattedGenre = null;
		String[] genreAsArray = genreList.split("\\|");
		int noOfGenres = genreAsArray.length;
		if (noOfGenres == 1) {
			formattedGenre = genreAsArray[0];
		}
		if (noOfGenres == 2) {
			formattedGenre = genreAsArray[0] + ", and " + genreAsArray[1];
		}
		if (noOfGenres > 2) {
			for (int i = 0; i < noOfGenres - 1; i++) {
				if (i > 0) {
					formattedGenre = formattedGenre + "," + genreAsArray[i];
				} else {
					formattedGenre = genreAsArray[i];
				}
			}
			formattedGenre = formattedGenre + ", and "
					+ genreAsArray[noOfGenres - 1];
		}

		return formattedGenre;

	}

}
