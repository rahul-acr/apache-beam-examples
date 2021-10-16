package org.rahul.schemas;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

public class SchemaSelectTest {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.getSchemaRegistry().registerPOJO(Transaction.class);


        pipeline.apply(
                Create.of(
                        new Transaction("john", 100.0),
                        new Transaction("doe", -50.0),
                        new Transaction("john", -20.0))
        )
                .apply(Select.fieldNames("name"))
                .apply(ParDo.of(new DoFn<Row, String>() {
                    @ProcessElement
                    public void processElement(@Element Row elem, OutputReceiver<String> out) {
                        String name = elem.getString("name");
                        System.out.println(name);

                    }
                }));

        pipeline.run().waitUntilFinish();

    }
}
