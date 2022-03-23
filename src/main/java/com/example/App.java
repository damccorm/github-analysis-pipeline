package com.example;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.JSONObject;

public class App {
    public interface Options extends StreamingOptions {
        @Description("The pipeline will process the top NumRepos of repos by stars.")
        @Default.Integer(1)
        int getNumRepos();

        void setNumRepos(int value);
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
            System.out.println(repoUrl);
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
            // return new OffsetRange(1, 10000000);
            return new OffsetRange(45512, 45520);
        }

        // Providing the coder is only necessary if it can not be inferred at runtime.
        @GetRestrictionCoder
        public Coder<OffsetRange> getRestrictionCoder() {
            return OffsetRange.Coder.of();
        }

        // Process all new pulls from lowest to highest, advancing the watermark as we go (we can probably just use a monotonically increasing watermark).
        // If we reach a number in the restriction that doesn't have a pr associated with it, or we start to get rate limited, self checkpoint.
        @ProcessElement
        public ProcessContinuation ProcessElement(ProcessContext c, @Element String repoName, RestrictionTracker<OffsetRange, Long> tracker, OutputReceiver<KV<String, String>> out) {
            // TODO - add in bundle finalization to just log that we've completed processing
            long i = tracker.currentRestriction().getFrom();
            int retries = 0;
            while (true){
                try {
                    JSONObject pullBlob = (JSONObject)GetGitHubContent(String.format("https://api.github.com/repos/%s/issues/%s", repoName, i), true);
                    System.out.println(pullBlob.toString());

                    if (tracker.tryClaim(i)) {
                        Object pullInfoBlob = pullBlob.get("pull_request");
                        if (pullInfoBlob != null) {
                            String body = pullBlob.get("body").toString();
                            String author = ((JSONObject)pullBlob.get("user")).get("login").toString();
                            // TODO - get timestamp and associate it with this element
                            // TODO - set up watermark estimator
                            out.output(KV.of("author:all-repos", author));
                            out.output(KV.of("body:all-repos:", body));
                            out.output(KV.of("author:" + repoName, author));
                            out.output(KV.of("body:" + repoName, body));
                            System.out.println(KV.of("author", author));
                            System.out.println(KV.of("body", body));
                        }
                        i++;
                    }
                    retries = 0;
                } catch (GitHubResponseException ex) {
                    int responseCode = ex.GetResponseCode();
                    if (responseCode == 404) {
                        // TODO - also, store that i-1 is the largest PR we can process at the moment in state. Then reference that cached value in splitRestriction.
                        System.out.println("No prs avaialable to process. Checkpointing and waiting for more.");
                        return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(120));
                    } else if (responseCode == 403) {
                        System.out.println("Getting throttled. Checkpointing and waiting for rate limits to cool down.");
                        return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1800));
                    } else {
                        if (retries >= 3) {
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

                    System.out.println(ex.toString());
                    System.out.println(ex.GetResponseCode());
                    i++;
                } catch (Exception ex) {
                    // TODO
                    System.out.println(ex.toString());
                }
            }
            // out.output(KV.of("test", repoToPullRangeMapping.getKey()));
            // System.out.println(KV.of("test", repoToPullRangeMapping.getKey()));
        }

        @SplitRestriction
        public void splitRestriction(@Element String repoName, @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> splitReceiver) {
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
    }

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);

        var pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.read().from("/Users/dannymccormick/Downloads/repos.csv"))
            .apply("Extract top repos", ParDo.of(new TopNReposFn()))
            .apply("Process repo activity", ParDo.of(new ProcessRepoActivity()));
        // TODO - window into
        // TODO - group by key
        // TODO - extract/format interesting results
        // TODO - write to a file somewhere

        pipeline.run();
    }
}