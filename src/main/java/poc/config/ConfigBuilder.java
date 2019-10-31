package poc.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigBuilder {

	public static Config read(String path) {
		return convertConfig(readConfig(path));
	}

	private static String readConfig(String path) {
		String config = null;
		try {
			config = new String(Files.readAllBytes(Paths.get(path)));
		}
		catch(IOException e) {
			System.out.println("Cannot read config file");
			System.exit(1);
		}
		return config;
	}

	private static Config convertConfig(String configJson) {
		Config config = null;

		ObjectMapper jsonObjectMapper = new ObjectMapper();
		try {
			config = jsonObjectMapper.readValue(configJson, Config.class);
			System.out.println(config);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot parse config file");
			System.exit(2);
		}

		return config;
	}
}