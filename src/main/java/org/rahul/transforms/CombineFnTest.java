package org.rahul.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class CombineFnTest {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Integer> numbers = List.of(1, 2, 3, 4, 5, 6);

        pipeline.apply(Create.of(numbers))
                .apply(Combine.globally(Sum.ofIntegers()))
                .apply(ParDo.of(new DoFn<Integer, String>() {
                    @ProcessElement
                    public void processElement(@Element Integer elem, OutputReceiver<String> out) {
                        System.out.println(elem);
                        out.output("Sum is " + elem);
                    }
                }));


        pipeline.run().waitUntilFinish();
    }
}
