﻿using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using PRISM;

namespace MASIC_ReporterIonObsStatsUploader
{
    class Program
    {
        static int Main(string[] args)
        {
            var asmName = typeof(Program).GetTypeInfo().Assembly.GetName();
            var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);       // Alternatively: System.AppDomain.CurrentDomain.FriendlyName
            var version = StatsUploaderOptions.GetAppVersion();

            var parser = new CommandLineParser<StatsUploaderOptions>(asmName.Name, version)
            {
                ProgramInfo = "This program loads data from a reporter ion observation rate file created by MASIC, " +
                              "and pushes the information into DMS. " +
                              "An example input file name is DatasetName_RepIonObsRate.txt. " +
                              "The directory with the file must also have the MASIC parameter file, " +
                              "along with the JobParameters XML file (e.g. JobParameters_1816211.xml). " +
                              "As an alternative to including the JobParameters file, the input file can be " +
                              "in a directory with a DMS-generated name, e.g. SIC202007110835_Auto1816211",

                ContactInfo = "Program written by Matthew Monroe for PNNL (Richland, WA) in 2020" +
                              Environment.NewLine +
                              "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                              "Website: https://panomics.pnnl.gov/ or https://omics.pnl.gov",

                UsageExamples = {
                    exeName + " DatasetInfoFile.txt",
                    exeName + @" DatasetInfoFile.txt G:\Upload",
                    exeName + @" DatasetInfoFile.txt G:\Upload /Preview",
                    exeName + @" DatasetInfoFile.txt G:\Upload /ChecksumMode:MoTrPAC",
                    exeName + @" /I:DatasetInfoFile.txt /O:G:\Upload"
                }
            };

            parser.AddParamFileKey("Conf");

            var parseResults = parser.ParseArgs(args);
            var options = parseResults.ParsedResults;

            try
            {
                if (!parseResults.Success)
                {
                    // Error messages should have already been shown to the user
                    Thread.Sleep(1500);
                    return -1;
                }

                if (!options.ValidateArgs(out var errorMessage))
                {
                    parser.PrintHelp();

                    Console.WriteLine();
                    ConsoleMsgUtils.ShowWarning("Validation error:");
                    ConsoleMsgUtils.ShowWarning(errorMessage);

                    Thread.Sleep(1500);
                    return -1;
                }

                options.OutputSetOptions();
            }
            catch (Exception e)
            {
                Console.WriteLine();
                Console.Write($"Error parsing options for {exeName}");
                Console.WriteLine(e.Message);
                Console.WriteLine($"See help with {exeName} --help");
                return -1;
            }

            try
            {
                var processor = new ReporterIonStatsUploader(options);

                processor.DebugEvent += Processor_DebugEvent;
                processor.ErrorEvent += Processor_ErrorEvent;
                processor.StatusEvent += Processor_StatusEvent;
                processor.WarningEvent += Processor_WarningEvent;
                processor.SkipConsoleWriteIfNoProgressListener = true;

                bool success;
                if (options.ReporterIonObsRateFilePath.Contains('*') ||
                    options.ReporterIonObsRateFilePath.Contains('?'))
                {
                    success = processor.ProcessFilesWildcard(options.ReporterIonObsRateFilePath);
                }
                else
                {
                    success = processor.ProcessFile(options.ReporterIonObsRateFilePath);
                }

                if (success)
                {
                    return 0;
                }

                Thread.Sleep(1500);
                return -1;

            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error occurred in Program->Main", ex);
                Thread.Sleep(1500);
                return -1;
            }
        }

        #region "Event handlers"

        private static void Processor_DebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebugCustom(message, emptyLinesBeforeMessage: 0);
        }

        private static void Processor_ErrorEvent(string message, Exception ex)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void Processor_StatusEvent(string message)
        {
            // Skip this message, which is auto-generated by ProcessFilesWildcard
            if (message.Equals("100.0% complete"))
                return;

            Console.WriteLine(message);
        }

        private static void Processor_WarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
        }

        #endregion
    }
}
