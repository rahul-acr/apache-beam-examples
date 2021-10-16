package org.rahul.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class GroupingByKeyTest {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("readFromFile", TextIO.read().from("data/naruto_data2.txt"))
                .apply("keyRead", ParDo.of(new ReadKey()))
                .apply("group", GroupByKey.create())
                .apply("output", ParDo.of(new DoFn<KV<String, Iterable<Integer>>, String>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Iterable<Integer>> elem, OutputReceiver<String> out) {
                        System.out.println(elem);
                        out.output(elem.getKey() + " scored " +elem.getValue());
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
