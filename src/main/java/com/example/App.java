package com.example;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.JSONObject;

public class App {
    public interface Options extends DataflowPipelineOptions {
        @Description("The pipeline will process the top NumRepos of repos by stars.")
        @Default.Integer(1)
        int getNumRepos();

        void setNumRepos(int value);

        @Description("The folder to write output to.")
        @Default.String("/tmp/github-analysis")
        String getOutputFolder();

        void setOutputFolder(String value);
    }

    private static class TopNReposFn extends DoFn<String, String> {
        @ProcessElement
        public void ProcessElement(ProcessContext c, @Element String val, OutputReceiver<String> out) {
            String[] csvValues = val.split(",", 0);
            if (csvValues.length <= 7) {
                return;
            }
            String type = csvValues[1];
            if (!type.equals("top-100-stars")) {
                // Ignore non-top 100 repos.
                return;
            }
            int rank = Integer.parseInt(csvValues[0]);
            Options ops = c.getPipelineOptions().as(Options.class);
            if (rank > ops.getNumRepos()) {
                // Ignore non-top N repos
                return;
            }
            String repoUrl = csvValues[6];
            // We can take advantage of the fact that all urls are prefixed by https://github.com/ (this would not be a good practice outside a hackathon :) )
            repoUrl = repoUrl.substring("https://github.com/".length());
            out.output(repoUrl);
        }
    }

    public static class GitHubResponseException 
        extends RuntimeException {
            private int code;
            public GitHubResponseException(String errorMessage) {
                super(errorMessage);
            }

            public GitHubResponseException(String errorMessage, int responseCode) {
                super(errorMessage);
                code = responseCode;
            }

            public int GetResponseCode() {
                return code;
            }
        }

    private static Object GetGitHubContent(String targetUrl, boolean retryOnRateLimits) throws Exception {
        int status = 500;
        int backoffSeconds = 1;
        URL url = new URL(targetUrl);
        while (true) {
            try {
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("GET");
                con.setRequestProperty("Accept", "application/vnd.github.v3+json");
                status = con.getResponseCode();
                if (status >= 300) {
                    throw new GitHubResponseException("Failed to get content", status);
                }
                BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                con.disconnect();
                return JSONValue.parse(content.toString());

            } catch(GitHubResponseException ex) {
                if (!retryOnRateLimits || status != 403) {
                    throw ex;
                }
                try {
                    TimeUnit.SECONDS.sleep(backoffSeconds);
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception sleeping, continuing anyways");
                }
                System.out.println(String.format("Retrying call %s on status code %s", targetUrl, status));
                backoffSeconds = backoffSeconds*2;
            }
        }
    }

    private static Integer GetLargestPull(String repoName, boolean retryOnRateLimits) {
        int status = 500;
        int backoffSeconds = 1;
        JSONArray pullBlob;
        try {
            pullBlob = (JSONArray)GetGitHubContent(String.format("https://api.github.com/repos/%s/pulls", repoName), true);
        } catch (Exception ex) {
            // TODO - more robust error handling
            return -1;
        }
        JSONObject latestPull = (JSONObject)pullBlob.get(0);
        return Integer.parseInt(latestPull.get("number").toString());
    }

    private static class ProcessRepoActivity extends DoFn<String, KV<String, String>> {
        @GetInitialRestriction
        public OffsetRange getInitialRestriction(@Element String repoName) throws IOException {
            // Range represents pull numbers. 
            // TODO - flip this to the full range.
            // return new OffsetRange(1, 10000000);
            return new OffsetRange(45512, 45515);
        }

        // Providing the coder is only necessary if it can not be inferred at runtime.
        @GetRestrictionCoder
        public Coder<OffsetRange> getRestrictionCoder() {
            return OffsetRange.Coder.of();
        }

        // Process all new pulls from lowest to highest, advancing the watermark as we go (we can probably just use a monotonically increasing watermark).
        // If we reach a number in the restriction that doesn't have a pr associated with it, or we start to get rate limited, self checkpoint.
        @ProcessElement
        public ProcessContinuation ProcessElement(ProcessContext c, @Element String repoName, RestrictionTracker<OffsetRange, Long> tracker, OutputReceiver<KV<String, String>> out, ManualWatermarkEstimator<Instant> watermarkEstimator, BundleFinalizer bundleFinalizer) {
            long i = tracker.currentRestriction().getFrom();
            int retries = 0;
            while (true){
                try {
                    JSONObject pullBlob = (JSONObject)GetGitHubContent(String.format("https://api.github.com/repos/%s/issues/%s", repoName, i), true);
                    String createTime = pullBlob.get("created_at").toString();
                    Instant timestamp = new Instant(createTime);
                    if (tracker.tryClaim(i)) {
                        Object pullInfoBlob = pullBlob.get("pull_request");
                        if (pullInfoBlob != null) {
                            String body = pullBlob.get("body").toString();
                            String author = ((JSONObject)pullBlob.get("user")).get("login").toString();
                            out.outputWithTimestamp(KV.of("author:" + author, repoName), timestamp);
                            out.outputWithTimestamp(KV.of("body:" + repoName, body), timestamp);
                            watermarkEstimator.setWatermark(timestamp);
                            bundleFinalizer.afterBundleCommit(
                                Instant.now().plus(Duration.standardMinutes(500)),
                                () -> {
                                    System.out.println("Persisted results for another pr");
                                });
                        }
                        i++;
                    } else {
                        return ProcessContinuation.stop();
                    }
                    retries = 0;
                } catch (GitHubResponseException ex) {
                    int responseCode = ex.GetResponseCode();
                    if (responseCode == 404) {
                        System.out.println("No prs avaialable to process. Checkpointing and waiting for more.");
                        // Advance the watermark to the current time
                        watermarkEstimator.setWatermark(new Instant());
                        return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(120));
                    } else if (responseCode == 403) {
                        System.out.println("Getting throttled. Checkpointing and waiting for rate limits to cool down.");
                        return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1800));
                    } else {
                        if (retries >= 3) {
                            System.out.println(ex.toString());
                            System.out.println(ex.GetResponseCode());
                            // If we've already tried this one 3 times, give up and move on to the next element.
                            if (!tracker.tryClaim(i)) {
                                return ProcessContinuation.stop();
                            }
                            i++;
                            retries = 0;
                        } else {
                            retries++;
                        }
                    }
                    i++;
                } catch (Exception ex) {
                    // TODO - more robust error handling. For now, just try again
                    System.out.println(ex.toString());
                }
            }
        }

        @SplitRestriction
        public void splitRestriction(@Element String repoName, @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> splitReceiver) {
            // This might have been simplified with a growable offset range tracker
            // We don't want to split larger than the largest pull.
            int largestPull = GetLargestPull(repoName, true);
            long splitSize = 10;
            long i = restriction.getFrom();
            while (i < largestPull - splitSize) {
                // Since the ranges are inclusive, subtract 1
                long end = i + splitSize - 1;
                splitReceiver.output(new OffsetRange(i, end));
                i = end;
            }
            // Output the last range
            splitReceiver.output(new OffsetRange(i, restriction.getTo()));
        }

        @GetInitialWatermarkEstimatorState
        public Instant getInitialWatermarkEstimatorState() {
            return new Instant(0);
        }

        @NewWatermarkEstimator
        public WatermarkEstimator<Instant> newWatermarkEstimator(@WatermarkEstimatorState Instant watermarkEstimatorState) {
            return new Manual(watermarkEstimatorState);
        }
    }

    // TODO - remove extraneous terms used for testing.
    static String[] termsToCheck = new String[]{"beam", "apache", "streaming pipeline", "checklist", "crowdin"};

    private static class ExtractResultMetrics extends DoFn<KV<String, Iterable<String>>, KV<String, Integer>> {
        @ProcessElement
        public void ProcessElement(ProcessContext c, @Element KV<String, Iterable<String>> metricsMapping, OutputReceiver<KV<String, Integer>> out) {
            int numValues = ((Collection<String>)metricsMapping.getValue()).size();
            if (metricsMapping.getKey().startsWith("author:")) {
                out.output(KV.of(metricsMapping.getKey(), numValues));
            } else if (metricsMapping.getKey().startsWith("body:")) {
                String repoName = metricsMapping.getKey().split(":", 0)[1];
                out.output(KV.of("numPulls:" + repoName, numValues));
                out.output(KV.of("numPulls:all-repos", numValues));
                for (String term : termsToCheck) {
                    int occurrences = 0;
                    for (String body : (Collection<String>)metricsMapping.getValue()) {
                        if (body.toLowerCase().contains(term.toLowerCase())) {
                            occurrences++;
                        }
                    }
                    if (occurrences > 0) {
                        out.output(KV.of(String.format("mentions:%s:%s", term, repoName), occurrences));
                        out.output(KV.of(String.format("mentions:%s:all-repos", term), occurrences));
                    }
                }
            }
        }
    }

    private static class CondenseMetrics extends DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>> {
        @ProcessElement
        public void ProcessElement(ProcessContext c, @Element KV<String, Iterable<Integer>> metricsMapping, OutputReceiver<KV<String, Integer>> out) {
            int sum = 0;
            for (Integer value : (Collection<Integer>)metricsMapping.getValue()) {
                sum += value;
            }
            out.output(KV.of(metricsMapping.getKey(), sum));
        }
    }

    private static class FormatElements extends DoFn<KV<String, Integer>, String> {
        @ProcessElement
        public void ProcessElement(ProcessContext c, @Element KV<String, Integer> metricsMapping, OutputReceiver<String> out, BoundedWindow w) {
            System.out.println(w.toString());
            String key = metricsMapping.getKey();
            String identifier = key.substring(key.indexOf(":"));

            out.output(identifier + ": " + metricsMapping.getValue());
        }
    }

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);

        var pipeline = Pipeline.create(options);
        PCollectionList<KV<String, Integer>> results = pipeline.apply(TextIO.read().from("/Users/dannymccormick/Downloads/repos.csv"))
            .apply("Extract top repos", ParDo.of(new TopNReposFn()))
            .apply("Process repo activity", ParDo.of(new ProcessRepoActivity()))
            // Don't allow late data - it should be impossible given the structure of our transforms/watermarks.
            // TODO - consider a custom trigger (I don't think it buys much, but it would be an interesting exercise).
            .apply("Apply windowing strategy", Window.<KV<String, String>>into(SlidingWindows.of(Duration.standardSeconds(60000)).every(Duration.standardSeconds(15000))))
            .apply("Group by key", GroupByKey.create())
            .apply("Extract results from activity", ParDo.of(new ExtractResultMetrics()))
            .apply("Group by key", GroupByKey.create())
            .apply("Extract results from activity", ParDo.of(new CondenseMetrics()))
            .apply(Partition.of(3, new PartitionFn<KV<String, Integer>>() {
                public int partitionFor(KV<String, Integer> metricsMapping, int numPartitions) {
                    if (metricsMapping.getKey().startsWith("author:")) {
                        return 0;
                    } else if (metricsMapping.getKey().startsWith("numPulls:")) {
                        return 1;
                    }
                    return 2;
                }}));

        PCollection<KV<String, Integer>> authorResults = results.get(0);
        PCollection<KV<String, Integer>> pullResults = results.get(1);
        PCollection<KV<String, Integer>> mentionResults = results.get(2);
        String outputFolder = options.getOutputFolder();
        if (outputFolder.charAt(outputFolder.length()-1) != '/') {
            outputFolder += "/";
        }
        authorResults
            .apply("Format elements", ParDo.of(new FormatElements()))
            .apply(TextIO.write().withWindowedWrites().withNumShards(1).to(outputFolder + "author/authorResults.txt"));
        pullResults
            .apply("Format elements", ParDo.of(new FormatElements()))
            .apply(TextIO.write().withWindowedWrites().withNumShards(1).to(outputFolder + "pull/pullResults.txt"));
        mentionResults
            .apply("Format elements", ParDo.of(new FormatElements()))
            .apply(TextIO.write().withWindowedWrites().withNumShards(1).to(outputFolder + "mentions/mentionResults.txt"));

        // TODO - consider adding some custom metrics - https://beam.apache.org/documentation/programming-guide/#metrics
        // TODO - is there a way to utilize state/timers?

        pipeline.run();
    }
}