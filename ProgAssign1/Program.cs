using System;
using System.IO;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.VisualBasic.FileIO;

namespace Assignment1
{
    public class DirWalker
    {
        private int skippedRowsCount = 0;
        private int validRowsCount = 0;

        public void WalkAndProcess(string path, string outputFilePath)
        {
            try
            {

                string[] directories = Directory.GetDirectories(path);
                Parallel.ForEach(directories, dirpath =>
                {
                    WalkAndProcess(dirpath, outputFilePath);
                    Console.WriteLine("Dir: " + dirpath);
                });


                string[] fileList = Directory.GetFiles(path, "CustomerData*.csv");
                Parallel.ForEach(fileList, filepath =>
                {
                    Console.WriteLine("Processing File: " + filepath);
                    ProcessCSV(filepath, outputFilePath, path);
                });
            }
            catch (UnauthorizedAccessException e)
            {
                Console.WriteLine("Error: Access to directory denied - " + e.Message);
            }
            catch (DirectoryNotFoundException e)
            {
                Console.WriteLine("Error: Directory not found - " + e.Message);
            }
            catch (IOException e)
            {
                Console.WriteLine("I/O Error: " + e.Message);
            }
        }

        private void ProcessCSV(string filePath, string outputFilePath, string directoryPath)
        {
            string date = ExtractDateFromPath(directoryPath);
            List<string> validRows = new List<string>();

            try
            {
                using (TextFieldParser parser = new TextFieldParser(filePath))
                {
                    parser.TextFieldType = FieldType.Delimited;
                    parser.SetDelimiters(",");

                    bool headerSkipped = false;

                    while (!parser.EndOfData)
                    {
                        string[] fields = parser.ReadFields();


                        if (!headerSkipped)
                        {
                            headerSkipped = true;
                            continue;
                        }


                        if (fields.Length < 10 || Array.Exists(fields, string.IsNullOrWhiteSpace))
                        {
                            Interlocked.Increment(ref skippedRowsCount);
                            continue;
                        }


                        validRows.Add(string.Join(",", fields) + "," + date);
                        Interlocked.Increment(ref validRowsCount);
                    }
                }


                if (validRows.Count > 0)
                {
                    WriteToOutputCSV(outputFilePath, validRows);
                }
            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("Error: File not found - " + e.Message);
            }
            catch (UnauthorizedAccessException e)
            {
                Console.WriteLine("Error: Access denied - " + e.Message);
            }
            catch (IOException e)
            {
                Console.WriteLine("I/O Error: " + e.Message);
            }
        }

        private void WriteToOutputCSV(string outputFilePath, List<string> validRows)
        {
            try
            {
                File.AppendAllLines(outputFilePath, validRows);
            }
            catch (UnauthorizedAccessException e)
            {
                Console.WriteLine("Error: Access denied when writing to output file - " + e.Message);
            }
            catch (DirectoryNotFoundException e)
            {
                Console.WriteLine("Error: Output directory not found - " + e.Message);
            }
            catch (IOException e)
            {
                Console.WriteLine("I/O Error while writing to output file: " + e.Message);
            }
        }

        private string ExtractDateFromPath(string path)
        {
            try
            {
                string[] splitPath = path.Split(Path.DirectorySeparatorChar);
                return $"{splitPath[splitPath.Length - 3]}/{splitPath[splitPath.Length - 2]}/{splitPath[splitPath.Length - 1]}";
            }
            catch (IndexOutOfRangeException e)
            {
                Console.WriteLine("Error extracting date from path - " + e.Message);
                return "Unknown";
            }
        }

        private void LogSummary(string logFilePath, TimeSpan executionTime)
        {
            try
            {
                using (StreamWriter sw = new StreamWriter(logFilePath, append: true))
                {
                    sw.WriteLine("Log Entry: " + DateTime.Now);
                    sw.WriteLine("Total Execution Time: " + executionTime.TotalSeconds + " seconds");
                    sw.WriteLine("Total Valid Rows: " + validRowsCount);
                    sw.WriteLine("Total Skipped Rows: " + skippedRowsCount);
                    sw.WriteLine("--------------------------------------------");
                }
            }
            catch (UnauthorizedAccessException e)
            {
                Console.WriteLine("Error: Access denied when writing to log file - " + e.Message);
            }
            catch (DirectoryNotFoundException e)
            {
                Console.WriteLine("Error: Logs directory not found - " + e.Message);
            }
            catch (IOException e)
            {
                Console.WriteLine("I/O Error while writing to log file: " + e.Message);
            }
        }

        public static void Main(string[] args)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            DirWalker walker = new DirWalker();
            string rootDirectory = @"C:\Users\Library\source\repos\ProgAssign1\ProgAssign1\Sample Data\";
            string outputFilePath = @"C:\Users\Library\source\repos\ProgAssign1\ProgAssign1\Output\Output.csv";
            string logFilePath = @"C:\Users\Library\source\repos\ProgAssign1\ProgAssign1\Log\Log.txt";

            try
            {
                using (StreamWriter sw = new StreamWriter(outputFilePath))
                {
                    sw.WriteLine("FirstName,LastName,StreetNumber,Street,City,Province,PostalCode,Country,PhoneNumber,EmailAddress,Date");
                }

                walker.WalkAndProcess(rootDirectory, outputFilePath);

                stopwatch.Stop();
                walker.LogSummary(logFilePath, stopwatch.Elapsed);

                Console.WriteLine("Process completed.");
                Console.WriteLine("Total skipped rows: " + walker.skippedRowsCount);
                Console.WriteLine("Total valid rows: " + walker.validRowsCount);
            }
            catch (UnauthorizedAccessException e)
            {
                Console.WriteLine("Error: Access denied - " + e.Message);
            }
            catch (DirectoryNotFoundException e)
            {
                Console.WriteLine("Error: Directory not found - " + e.Message);
            }
            catch (IOException e)
            {
                Console.WriteLine("I/O Error: " + e.Message);
            }
        }
    }
}
