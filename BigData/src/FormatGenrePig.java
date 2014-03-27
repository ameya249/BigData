import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class FormatGenrePig extends EvalFunc<String> {

	@Override
	public String exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0) {
			return null;
		}
		String lineFromFile = (String) tuple.get(0);
		String formattedGenre = formatGenre(lineFromFile);
		return formattedGenre;
	}

	private String formatGenre(String genreList) {
		String formattedGenre = null;
		String[] genreAsArray = genreList.split("\\|");
		int noOfGenres = genreAsArray.length;
		ArrayUtils.reverse(genreAsArray);

		if (noOfGenres == 1) {
			formattedGenre = genreAsArray[0];
		}
		if (noOfGenres == 2) {
			formattedGenre = genreAsArray[0] + " and " + genreAsArray[1];
		}
		if (noOfGenres > 2) {
			for (int i = 0; i < noOfGenres - 1; i++) {

				if (i > 0) {
					formattedGenre = formattedGenre + "," + genreAsArray[i];
				} else {
					formattedGenre = genreAsArray[i];
				}

			}
			formattedGenre = formattedGenre + " and "
					+ genreAsArray[noOfGenres - 1];
		}

		return formattedGenre;

	}

}
