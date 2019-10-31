package poc;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import poc.output.AvroToCsv;
import poc.config.Config;
import poc.input.CsvReader;
import poc.util.Avro;

public class Converter {

	static void runConverter(PipelineOptions options) {
		Pipeline p = Pipeline.create(options);
		String inputSchema = Config.getInputSchemaJson();
		
		PCollection<GenericRecord> records =
			p.apply(FileIO.match().filepattern("multilineinput.csv"))
				.apply(FileIO.readMatches())
				.apply(ParDo.of(new CsvReader()))
				.setCoder(AvroCoder.of(GenericRecord.class, Avro.fromJson(inputSchema)));

		records
			.apply(ParDo.of(new AvroToCsv()))
			.apply("Writelines", TextIO.write().to("testoutput.txt"));
			
		records
			.apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(Avro.fromJson(inputSchema))).to("output.parquet"));
	
		p.run().waitUntilFinish();
	}

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.create();
		runConverter(options);
	}
}