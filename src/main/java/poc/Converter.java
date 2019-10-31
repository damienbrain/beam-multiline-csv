package poc;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import poc.config.ConfigBuilder;
import poc.output.CurationOutputPipelineBuilder;
import poc.config.Config;
import poc.util.Avro;

import org.apache.beam.sdk.transforms.DoFn;

public class Converter {

	static void runConverter(PipelineOptions options) {
		Pipeline p = Pipeline.create(options);
		Config config = ConfigBuilder.read("config.json");
		String inputSchema = config.getInputSchemaJson();
		
		p.apply(FileIO.match().filepattern("multilineinput.csv"))
			.apply(FileIO.readMatches())
			.apply(ParDo.of(new DoFn<ReadableFile, GenericRecord>() {
				@ProcessElement
				public void process(ProcessContext c) throws IOException {
					try(InputStream is = Channels.newInputStream(c.element().open())) {
						try {
							CSVReader csvr = new CSVReader(new InputStreamReader(is));
							List<String[]> rows = csvr.readAll();
							csvr.close();

							Schema schema = new Schema.Parser().parse(inputSchema);

							for(String[] row : rows) {
								// Create Avro Generic Record
								GenericRecord genericRecord = new GenericData.Record(schema);
								List<Schema.Field> fields = schema.getFields();
								for (int index = 0; index < fields.size(); ++index) {
									Schema.Field field = fields.get(index);
									genericRecord.put(field.name(), row[index]);
								}
								/*
								for(String s : row) {
									System.out.println("<<<<<");
									System.out.println(s);
									System.out.println(">>>>>");
								}
								*/
								c.output(genericRecord);
							}
						}
						catch(CsvException e) {
							System.out.println("CSVException: " + e.toString());
						}
					}
				}
			}
			))
			.setCoder(AvroCoder.of(GenericRecord.class, Avro.fromJson(inputSchema)))
			.apply(new CurationOutputPipelineBuilder(config));
			;
	
		p.run().waitUntilFinish();
	}

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.create();
		runConverter(options);
	}
}