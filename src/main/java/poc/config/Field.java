package poc.config;

public class Field implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2700508124391179673L;
	private String dataType;
	private String inputFormat;
	private String outputFormat;
	
	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getInputFormat() {
		return inputFormat;
	}

	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}

	public String getOutputFormat() {
		return outputFormat;
	}

	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	@Override
	public String toString() {
		return getDataType();
	}
}