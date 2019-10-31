package poc.output;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import poc.config.Config;
import poc.util.Avro;

public class CurationOutputPipelineBuilder extends PTransform<PCollection<GenericRecord>, PDone> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Config config;

	public CurationOutputPipelineBuilder(Config config) {
		this.config = config;
	}

	@Override
	public PDone expand(PCollection<GenericRecord> input) {
		input
			.apply(ParDo.of(new AvroToCsv()))
			.apply("Writelines", TextIO.write().to("testoutput.txt"));

		input
			.apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(Avro.fromJson(config.getInputSchemaJson()))).to("output.parquet"));

		return PDone.in(input.getPipeline());
	}
}
