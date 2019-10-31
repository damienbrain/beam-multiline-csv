package poc.util;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class Avro {

	public static Schema fromJson(String json) {
		return new Schema.Parser().parse(json);
	}

	public static String toCsv(GenericRecord record) {
		StringBuilder sb = new StringBuilder();

		for(Field f : record.getSchema().getFields()) {
			if(sb.length() > 0) sb.append(",");
			sb.append(record.get(f.name()));
		}

		return sb.toString();
	}
}
