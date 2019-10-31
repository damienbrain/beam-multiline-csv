package poc.input;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import poc.config.Config;

public class CurationInputPipelineBuilder extends PTransform<PBegin, PCollection<GenericRecord>> {

	private static final long serialVersionUID = 1L;
	private Config config;

	public CurationInputPipelineBuilder(Config config) {
		this.config = config;
	}

	@Override
	public PCollection<GenericRecord> expand(PBegin input) {
		return input.apply("Readlines", TextIO.read().from("testinput.txt"))
			.apply(ParDo.of(new CsvToAvro(config.getInputSchemaJson(), ",")))
			;
	}
}