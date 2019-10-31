package poc.input;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;

public class CsvToAvro extends DoFn<String, GenericRecord> {

	private static final long serialVersionUID = 1L;
	private String delimiter;
    private String schemaJson;

    public CsvToAvro(String schemaJson, String delimiter) {
      this.schemaJson = schemaJson;
      this.delimiter = delimiter;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) throws IllegalArgumentException {
      // Split CSV row into using delimiter
      String[] rowValues = ctx.element().split(delimiter);

      Schema schema = new Schema.Parser().parse(schemaJson);

      // Create Avro Generic Record
      GenericRecord genericRecord = new GenericData.Record(schema);
      List<Schema.Field> fields = schema.getFields();

      for (int index = 0; index < fields.size(); ++index) {
        Schema.Field field = fields.get(index);
        String fieldType = field.schema().getType().getName().toLowerCase();

        switch (fieldType) {
          case "string":
            genericRecord.put(field.name(), rowValues[index]);
            break;
          case "boolean":
            genericRecord.put(field.name(), Boolean.valueOf(rowValues[index]));
            break;
          case "int":
            genericRecord.put(field.name(), Integer.valueOf(rowValues[index]));
            break;
          case "long":
            genericRecord.put(field.name(), Long.valueOf(rowValues[index]));
            break;
          case "float":
            genericRecord.put(field.name(), Float.valueOf(rowValues[index]));
            break;
          case "double":
            genericRecord.put(field.name(), Double.valueOf(rowValues[index]));
            break;
          default:
            throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
        }
      }
      ctx.output(genericRecord);
    }
}