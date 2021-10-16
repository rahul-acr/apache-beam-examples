package org.rahul.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;

public class FlattenTest {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> names1 = pipeline.apply(Create.of("John", "Doe"));
        PCollection<String> names2 = pipeline.apply(Create.of("Steve", "Robert"));

        PCollectionList<String> collections = PCollectionList.of(names1).and(names2);

        PCollection<String> allNames = collections.apply(Flatten.pCollections()).apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String element, OutputReceiver<String> out) {
                System.out.println(element);
                out.output(element);
            }
        }));

        pipeline.run().waitUntilFinish();
    }

}
