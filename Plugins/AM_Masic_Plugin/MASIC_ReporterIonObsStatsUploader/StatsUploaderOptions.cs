using System;
using System.Reflection;
using PRISM;

namespace MASIC_ReporterIonObsStatsUploader
{
    public class StatsUploaderOptions
    {
        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "2024-08-08";

        /// <summary>
        /// Database connections string
        /// </summary>
        [Option("DatabaseConnectionString", "ConnectionString", "CN",
            HelpShowsDefault = true,
            HelpText = "Database connection string")]
        public string ConnectionString { get; set; } = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;";

        /// <summary>
        /// When true, load data, but do not contact the database
        /// </summary>
        [Option("Preview", HelpShowsDefault = false,
            HelpText = "Load observation rate data, but do not contact the database")]
        public bool PreviewMode { get; set; }

        /// <summary>
        /// When true, show more status messages
        /// </summary>
        [Option("VerboseMode", "Verbose", "V", HelpShowsDefault = false,
            HelpText = "When true, show more status messages")]
        public bool VerboseMode { get; set; }

        /// <summary>
        /// File with reporter ion observation rates to push into DMS
        /// </summary>
        [Option("ReporterIonObsRateFilePath", "ObsRateFile", "InputFile", "i", "input",
            ArgPosition = 1, HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Reporter ion observation rate file path")]
        public string ReporterIonObsRateFilePath { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public StatsUploaderOptions()
        {
            ReporterIonObsRateFilePath = string.Empty;
        }

        /// <summary>
        /// Get the program version
        /// </summary>
        public static string GetAppVersion()
        {
            var version = Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";

            return version;
        }

        /// <summary>
        /// Show the options at the console
        /// </summary>
        public void OutputSetOptions()
        {
            Console.WriteLine("Options:");

            Console.WriteLine(" {0,-25} {1}", "Input file:", PathUtils.CompactPathString(ReporterIonObsRateFilePath, 120));

            Console.WriteLine(" {0,-25} {1}", "Connection string", ConnectionString);

            Console.WriteLine();

            if (PreviewMode)
            {
                Console.WriteLine(" Preview Mode is enabled: Loading stats, but not contacting the database");
                Console.WriteLine();
            }
        }

        /// <summary>
        /// Validate the options
        /// </summary>
        public bool ValidateArgs(out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(ReporterIonObsRateFilePath))
            {
                errorMessage = "You must specify the MASIC input file, e.g. DatasetName_RepIonObsRate.txt";
                return false;
            }

            errorMessage = string.Empty;
            return true;
        }
    }
}
