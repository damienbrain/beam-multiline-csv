package poc.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Config implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1239013818939321647L;
	private static final String inputSchemaFile = "input.schema";

	public static String getInputSchemaJson() {
		try {
			return(readJson(inputSchemaFile));
		}
		catch(IOException e) {
			System.out.println("Could not read input schema from file " + inputSchemaFile + "\n");
		}
		return null;
	}

	private static String readJson(String schemaPath) throws IOException {
		return new String(Files.readAllBytes(Paths.get(schemaPath)));
	}
}