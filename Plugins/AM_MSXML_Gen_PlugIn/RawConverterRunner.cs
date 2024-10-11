using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using PRISM;

namespace AnalysisManagerMsXmlGenPlugIn
{
    public class RawConverterRunner : EventNotifier
    {
        // Ignore Spelling: mgf

        public const string RAW_CONVERTER_FILENAME = "RawConverter.exe";

        /// <summary>
        /// 0 means no debugging, 1 for normal, 2 for verbose
        /// </summary>
        private readonly int mDebugLevel;

        public string RawConverterExePath { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public RawConverterRunner(string rawConverterDir, int debugLevel = 1)
        {
            RawConverterExePath = Path.Combine(rawConverterDir, RAW_CONVERTER_FILENAME);

            if (!File.Exists(RawConverterExePath))
            {
                throw new FileNotFoundException(RawConverterExePath);
            }

            mDebugLevel = debugLevel;
        }

        /// <summary>
        /// Create .mgf file using RawConverter
        /// this method is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public bool ConvertRawToMGF(string rawFilePath)
        {
            try
            {
                var sourceFile = new FileInfo(rawFilePath);

                if (sourceFile.Directory == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of the instrument file: " + rawFilePath);
                    return false;
                }

                if (mDebugLevel > 0)
                {
                    OnProgressUpdate("Creating .MGF file using RawConverter", 0);
                }

                var rawConverter = new FileInfo(RawConverterExePath);

                if (rawConverter.Directory == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of the converter exe: " + RawConverterExePath);
                    return false;
                }

                // Set up command
                var arguments =
                    " " + Global.PossiblyQuotePath(sourceFile.FullName) +
                    " --mgf";

                if (mDebugLevel > 0)
                {
                    OnProgressUpdate(rawConverter.FullName + " " + arguments, 0);
                }

                // Setup a program runner tool to make the spectra files
                // The working directory must be the directory that has RawConverter.exe
                // Otherwise, the program creates the .mgf file in C:\  (and will likely get Access Denied)

                var consoleOutputFilePath = Path.Combine(sourceFile.Directory.FullName, "RawConverter_ConsoleOutput.txt");

                var progRunner = new RunDosProgram(rawConverter.Directory.FullName, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = consoleOutputFilePath
                };
                progRunner.ErrorEvent += ProgRunner_ErrorEvent;

                if (!progRunner.RunProgram(rawConverter.FullName, arguments, "RawConverter", true))
                {
                    // .RunProgram returned false
                    OnErrorEvent("Error running " + Path.GetFileNameWithoutExtension(rawConverter.Name));
                    return false;
                }

                if (mDebugLevel >= 2)
                {
                    OnProgressUpdate(" ... MGF file created using RawConverter", 100);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in ConvertRawToMGF: " + ex.Message);
                return false;
            }
        }

        private void ProgRunner_ErrorEvent(string errMsg, Exception ex)
        {
            if (ex == null || errMsg.Contains(ex.Message))
            {
                OnErrorEvent("Exception running RawConverter: " + errMsg);
            }
            else
            {
                OnErrorEvent("Exception running RawConverter: " + errMsg + "; " + ex.Message);
            }
        }
    }
}
