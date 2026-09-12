package com.example.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Comparator;

public class MovieTop10 {

    // =====================================================
    // MAPPER
    // =====================================================

    public static class MovieMapper
            extends Mapper<Object, Text, Text, Text> {

        private final Text outKey = new Text("TOP");
        private final Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header
            if (line.startsWith("MovieID")) {
                return;
            }

            String[] fields = line.split(",", -1);

            try {

                /*
                 * Dataset columns:
                 *
                 * 0  MovieID
                 * 1  Title
                 * 2  Genre
                 * 3  ReleaseYear
                 * 4  ReleaseDate
                 * 5  Country
                 * 6  BudgetUSD
                 * 7  US_BoxOfficeUSD
                 * 8  Global_BoxOfficeUSD
                 * 9  Opening_Day_SalesUSD
                 * 10 One_Week_SalesUSD
                 * 11 IMDbRating
                 * 12 RottenTomatoesScore
                 * 13 NumVotesIMDb
                 * 14 NumVotesRT
                 * 15 Director
                 * 16 LeadActor
                 */

                String movieTitle = fields[1].trim();

                double globalBoxOffice =
                        Double.parseDouble(fields[8].trim());

                /*
                 * Send every movie to the same key.
                 *
                 * This ensures that all movies go to
                 * the same reducer.
                 */

                outValue.set(
                        movieTitle + "\t" + globalBoxOffice
                );

                context.write(outKey, outValue);

            } catch (Exception e) {

                // Ignore malformed records
                System.err.println(
                        "Skipping invalid record: " + line
                );
            }
        }
    }


    // =====================================================
    // MOVIE RECORD FOR PRIORITY QUEUE
    // =====================================================

    public static class MovieRecord {

        String title;
        double revenue;

        public MovieRecord(String title, double revenue) {
            this.title = title;
            this.revenue = revenue;
        }
    }


    // =====================================================
    // REDUCER
    // =====================================================

    public static class MovieReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            /*
             * PriorityQueue works as a MIN-HEAP.
             *
             * The movie with the smallest revenue
             * stays at the top.
             */

            PriorityQueue<MovieRecord> top10 =
                    new PriorityQueue<>(
                            10,
                            Comparator.comparingDouble(
                                    movie -> movie.revenue
                            )
                    );

            // Read all movies
            for (Text value : values) {

                String[] parts =
                        value.toString().split("\t", 2);

                if (parts.length != 2) {
                    continue;
                }

                String title = parts[0];

                double revenue;

                try {
                    revenue = Double.parseDouble(parts[1]);
                } catch (NumberFormatException e) {
                    continue;
                }

                MovieRecord movie =
                        new MovieRecord(title, revenue);

                /*
                 * Add movie to Top 10
                 */
                top10.offer(movie);

                /*
                 * If we now have more than 10,
                 * remove the movie with the
                 * smallest revenue.
                 */
                if (top10.size() > 10) {
                    top10.poll();
                }
            }


            // =================================================
            // OUTPUT TOP 10 IN DESCENDING ORDER
            // =================================================

            MovieRecord[] results =
                    top10.toArray(
                            new MovieRecord[0]
                    );

            /*
             * Sort from highest revenue to lowest.
             */
            java.util.Arrays.sort(
                    results,
                    (a, b) ->
                            Double.compare(
                                    b.revenue,
                                    a.revenue
                            )
            );

            /*
             * Output the final Top 10.
             */
            for (MovieRecord movie : results) {

                context.write(
                        new Text(movie.title),
                        new Text(
                                String.format(
                                        "$%,.0f",
                                        movie.revenue
                                )
                        ));
            }
        }
    }


    // =====================================================
    // DRIVER
    // =====================================================

    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {

            System.err.println(
                    "Usage: MovieTop10 <input path> <output path>"
            );

            System.exit(2);
        }

        Configuration conf =
                new Configuration();

        Job job =
                Job.getInstance(
                        conf,
                        "Top 10 Highest Grossing Movies"
                );

        job.setJarByClass(
                MovieTop10.class
        );


        // Mapper
        job.setMapperClass(
                MovieMapper.class
        );


        // Reducer
        job.setReducerClass(
                MovieReducer.class
        );


        // Mapper output
        job.setMapOutputKeyClass(
                Text.class
        );

        job.setMapOutputValueClass(
                Text.class
        );


        // Final output
        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                Text.class
        );


        /*
         * IMPORTANT:
         *
         * We use ONE reducer because we need
         * one global Top 10.
         */

        job.setNumReduceTasks(1);


        // Input path
        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );


        // Output path
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );


        // Run Hadoop job
        System.exit(
                job.waitForCompletion(true)
                        ? 0
                        : 1
        );
    }
}