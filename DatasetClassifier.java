/****************************************************************************************************************
 * DatasetClassifier.java
 * 
 * This Java program classify medical records based on specific columns in the CSV file (MedicalFiles.csv). The program processes
 * the data using YARN over Apache Hadoop's MapReduce framework. It first reads input from a file named MedicalFiles.csv,
 * extracts relevant information, and writes the classification summary to the specified
 * output directory.
 * 
 * The steps:
 * 
 * Step 1: Reading and parsing the CSV file:
 * - Read the contents of the input file MedicalFiles.csv, but not the header.
 * - The new CSV file header is defined to identify and extract specific columns.
 * 
 * Step 2: Mapper processing:
 * - The Mapper class parses each line of the CSV file except the header.
 * - For each line it extracts the NHS number and the specified column value (e.g., symptoms).
 * - It then emits a key-value pairs where the key is the extracted value and the value is the entire line.
 * 
 * Step 3: Reducer processing:
 * - The Reducer class receives key-value pairs from the Mapper.
 * - It aggregates occurrences of each key (e.g. symptom) and compiles patient details for each category.
 * - It generates classification summaries containing the counts and the corresponding patient details.
 * 
 * Step 4: Configuring and running job:
 * - The job configuration specifies both input and output paths, column name for classification, and other setting.
 * - The program uses the Apache Hadoop YARN framework for job scheduling and the resource management.
 * 
 * This program is designed to handle large datasets efficiently by distributing tasks across several nodes in
 * Hadoop cluster which ensures a scalable and parallel processing.
 * 
 * @authors: Raz Mohammad Yousufi
 * @version 1.0
 ****************************************************************************************************************/

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DatasetClassifier {

    // Define the new header for the CSV data
    private final static String CSV_HEADER = "NHS_Number,Name,Age,Gender,Admission_Date,Year,Record_Number,Symptoms,Illness_History,Chief_Complaint,Physical_Examination,Assessment_Plan,Region";

    // Mapper class to process CSV data
    public static class CSVProcessor extends Mapper<Object, Text, Text, Text> {
        // Text objects to hold key-value pairs
        private Text keyOutput = new Text();
        private Text valueOutput = new Text();
        // Flag to skip the header row
        private boolean isHeader = true;
        // Indexes for columns
        private int targetColumnIndex;
        private int nhsColumnIndex;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // Get column name from configuration
            Configuration conf = context.getConfiguration();
            String targetColumn = conf.get("targetColumn", "Region"); // Default to 'Region'
            targetColumnIndex = getColumnIndex(targetColumn);
            nhsColumnIndex = getColumnIndex("NHS_Number");
        }

        // Mapper function that processes each line of the input file
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (isHeader) {
                isHeader = false; // Skip the header row
            } else {
                // Split the line into columns
                String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                if (columns.length > targetColumnIndex && columns.length > nhsColumnIndex) {
                    String nhsNumber = columns[nhsColumnIndex].trim();
                    String targetValue = columns[targetColumnIndex].trim().replaceAll("^\"|\"$", ""); // Remove quotes inside column value
                   
                                   // Emit each value inside a column (i.e., 'Diabets type 2' in symptom) as a separate key
                    String[] symptoms = targetValue.split(",\\s*");
                    for (String symptom : symptoms) {
                        keyOutput.set(symptom);
                        valueOutput.set(line);  // Return the entire line as the key's value
                        context.write(keyOutput, valueOutput);
                    }
                }
            }
        }

        // Helper function for finding index of specified column
        private int getColumnIndex(String columnName) {
            String[] headers = CSV_HEADER.split(",");
            for (int i = 0; i < headers.length; i++) {
                if (headers[i].equalsIgnoreCase(columnName.trim())) {
                    return i;
                }
            }
            return -1; // Return -1, if the column is not found
        }
    }

    // Reducer class for aggregating and listing the column values based on their specifc headers (separted by comma)
    public static class ColumnReducer extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, Integer> reduceCounts = new HashMap<>();
        private String[] headers;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            String csvHeader = conf.get("CSV_HEADER");
            headers = csvHeader.split(",");
        }

        // Reducer function to process each key-value pairs
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder patientDetails = new StringBuilder();
            int count = 0;
            for (Text value : values) {
                if (count > 0) {
                    patientDetails.append("\n\n");
                }
                patientDetails.append(formatPatientDetails(value.toString()));
                count++;
            }
            // Output of the classification and their corresponding patient details
        context.write(
            new Text("---------------------------------------------------------------\n" + key.toString() + " (" + count + ") matched. The details are listed as follows:\n"),
            new Text(patientDetails.toString())
        );
        }

        // Helper function to format patient details from a CSV line
        private String formatPatientDetails(String line) {
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            StringBuilder details = new StringBuilder();
            for (int i = 0; i < headers.length; i++) {
                if (i > 0) {
                    details.append("\n");
                }
                details.append(headers[i]).append(": ").append(columns[i].trim().replaceAll("^\"|\"$", ""));
            }
            return details.toString();
        }

        // Write the symptom counts to output
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : reduceCounts.entrySet()) {
                String symptom = entry.getKey().replaceAll("^\"|\"$", ""); // Remove quotes inside a specific column vlaue
                context.write(new Text(symptom), new Text(entry.getValue().toString()));
            }
        }
    }

    // Main method to configure and run job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("CSV_HEADER", CSV_HEADER);
        Job job = Job.getInstance(conf, "dataset classifier");
        job.setJarByClass(DatasetClassifier.class);
        job.setMapperClass(CSVProcessor.class);
        job.setReducerClass(ColumnReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and output paths
        FileInputFormat.addInputPath(job, new Path("MedicalFiles.csv")); // Input file
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        // Pass the column name as a commandline argument
        if (args.length > 1) {
            job.getConfiguration().set("targetColumn", args[1]);
        }

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}