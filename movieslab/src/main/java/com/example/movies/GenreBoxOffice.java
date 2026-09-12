
package com.example.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GenreBoxOffice {

    // =========================
    // MAPPER
    // =========================
    public static class GenreMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final Text genre = new Text();
        private final DoubleWritable boxOffice = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header
            if (line.startsWith("MovieID")) {
                return;
            }

            // Split CSV
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

                String genreValue = fields[2].trim();

                double globalBoxOffice =
                        Double.parseDouble(fields[8].trim());

                // Mapper output:
                // Genre -> Global Box Office
                genre.set(genreValue);
                boxOffice.set(globalBoxOffice);

                context.write(genre, boxOffice);

            } catch (Exception e) {

                // Ignore malformed rows
                System.err.println(
                        "Skipping invalid record: " + line
                );
            }
        }
    }


    // =========================
    // REDUCER
    // =========================
    public static class GenreReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double total = 0.0;

            for (DoubleWritable value : values) {
                total += value.get();
            }

            // Format as currency with commas and 2 decimal places
            String formattedTotal = String.format("$%,.2f", total);

            context.write(key, new Text(formattedTotal));
        }
    }


    // =========================
    // DRIVER
    // =========================
    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {

            System.err.println(
                    "Usage: GenreBoxOffice <input path> <output path>"
            );

            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Total Global Box Office by Genre"
        );

        job.setJarByClass(GenreBoxOffice.class);

        // Mapper
        job.setMapperClass(GenreMapper.class);

        // Reducer
        job.setReducerClass(GenreReducer.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and output paths
        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        // Start Hadoop job
        System.exit(
                job.waitForCompletion(true)
                        ? 0
                        : 1
        );
    }
}

