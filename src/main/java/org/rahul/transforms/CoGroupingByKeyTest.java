package org.rahul.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class CoGroupingByKeyTest {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, String>> emailCollection = pipeline.apply(Create.of(KV.of("john", "john@abc.com"),
                KV.of("doe", "doe@abc.com"),
                KV.of("john", "john@gmail.com")));

        PCollection<KV<String, String>> phoneCollection = pipeline.apply(Create.of(KV.of("john", "11555888"),
                KV.of("doe", "4445555"),
                KV.of("doe", "778855")));

        // collections must be tagged
        TupleTag<String> emailTag = new TupleTag<>();
        TupleTag<String> phoneTag = new TupleTag<>();

        // KeyedPCollectionTuple class is for type safety
        PCollection<KV<String, CoGbkResult>> coCollection = KeyedPCollectionTuple.of(emailTag, emailCollection)
                .and(phoneTag, phoneCollection)
                .apply(CoGroupByKey.create());

        coCollection.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<String> out) {
                String name = element.getKey();
                Iterable<String> emails = element.getValue().getAll(emailTag);
                Iterable<String> phones = element.getValue().getAll(phoneTag);
                String res = name + emails + phones;
                System.out.println(res);
                out.output(res);
            }
        }));

        pipeline.run().waitUntilFinish();
    }

}
