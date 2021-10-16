package org.rahul.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


public class GroupByKeyAndCombineTest {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<KV<String, Integer>> data = pipeline
                .apply("readFromFile", TextIO.read().from("data/naruto_data2.txt"))
                .apply("keyRead", ParDo.of(new ReadKey()));


        data.apply(Combine.perKey(Sum.ofIntegers())).apply(ParDo.of(new DoFn<KV<String, Integer>, String>() {
            @ProcessElement
            public void processElement(@Element KV<String, Integer> element, OutputReceiver<String> out) {
                String outputLine = element.getKey()+" scored "+element.getValue();
                System.out.println(outputLine);
                out.output(outputLine);
            }
        }));

        pipeline.run().waitUntilFinish();
    }


    private static class ReadKey extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<KV<String, Integer>> out) {
            String[] split = word.split(" ");
            String name = split[0];
            int score = Integer.parseInt(split[1]);
            out.output(KV.of(name, score));
        }
    }
}
