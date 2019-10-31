package poc.output;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;

import poc.util.Avro;

public class AvroToCsv extends DoFn<GenericRecord, String> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(@Element GenericRecord input, OutputReceiver<String> output) {
		output.output(Avro.toCsv(input));
	}
}