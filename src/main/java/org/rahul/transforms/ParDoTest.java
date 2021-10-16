package org.rahul.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class ParDoTest {
    public static void main(String[] args) {
        // Start by defining the options for the pipeline.
        PipelineOptions options = PipelineOptionsFactory.create();

        // Then create the pipeline.
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> lines = pipeline.apply("quotes", TextIO.read().from("data/naruto_data.txt"))
                .apply("uppercase", ParDo.of(new UpperCase()))
                .apply("dededed", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(@Element String word, OutputReceiver<String> out) {
                        System.out.println(word);
                        out.output(word);
                    }
                }));


        pipeline.run().waitUntilFinish();
        
    }


    private static class UpperCase extends DoFn<String, String> {

        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<String> out) {
            if(word.startsWith("I")) {
                out.output(word.toUpperCase());
            }
        }
    }
}
