package poc.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

public class Config implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1239013818939321647L;
	private Map<String, Field> fields;
	private String inputSchemaFile;

	public Config() {
		fields = new LinkedHashMap<String, Field>();
	}

	public Map<String, Field> getFields() {
		return fields;
	}

	public void setFields(Map<String, Field> fields) {
		this.fields = fields;
	}

	public String getInputSchemaFile() {
		return inputSchemaFile;
	}

	public void setInputSchemaFile(String inputSchemaFile) {
		this.inputSchemaFile = inputSchemaFile;
	}

	public String getInputSchemaJson() {
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

	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();

		for(Map.Entry<String, Field> field: fields.entrySet()) {
			output.append(field.getKey() + ": " + field.getValue().toString());
			output.append("\n");
		}
		
		output.append("inputSchemaFile: " + getInputSchemaFile() + "\n");
		return output.toString();
	}
}
