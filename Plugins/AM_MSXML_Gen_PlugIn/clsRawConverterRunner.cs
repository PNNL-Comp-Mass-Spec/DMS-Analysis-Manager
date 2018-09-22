using System;
using System.IO;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerMsXmlGenPlugIn
{
    public class clsRawConverterRunner : EventNotifier
    {
        #region "Constants"

        public const string RAWCONVERTER_FILENAME = "RawConverter.exe";

        #endregion

        #region "Member variables"

        /// <summary>
        /// 0 means no debugging, 1 for normal, 2 for verbose
        /// </summary>
        private readonly int m_DebugLevel;

        #endregion

        #region "Properties"

        public string RawConverterExePath { get; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public clsRawConverterRunner(string rawConverterDir, int debugLevel = 1)
        {
            RawConverterExePath = Path.Combine(rawConverterDir, RAWCONVERTER_FILENAME);
            if (!File.Exists(RawConverterExePath))
            {
                throw new FileNotFoundException(RawConverterExePath);
            }

            m_DebugLevel = debugLevel;
        }

        /// <summary>
        /// Create .mgf file using RawConverter
        /// This function is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool ConvertRawToMGF(string rawFilePath)
        {
            try
            {
                var fiSourceFile = new FileInfo(rawFilePath);

                if (m_DebugLevel > 0)
                {
                    OnProgressUpdate("Creating .MGF file using RawConverter", 0);
                }

                var fiRawConverter = new FileInfo(RawConverterExePath);

                // Set up command
                var cmdStr = " " + clsGlobal.PossiblyQuotePath(fiSourceFile.FullName) + " --mgf";

                if (m_DebugLevel > 0)
                {
                    OnProgressUpdate(fiRawConverter.FullName + " " + cmdStr, 0);
                }

                // Setup a program runner tool to make the spectra files
                // The working directory must be the directory that has RawConverter.exe
                // Otherwise, the program creates the .mgf file in C:\  (and will likely get Access Denied)

                var consoleOutputFilePath = Path.Combine(fiSourceFile.Directory.FullName, "RawConverter_ConsoleOutput.txt");

                var progRunner = new clsRunDosProgram(fiRawConverter.Directory.FullName, m_DebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = consoleOutputFilePath
                };
                progRunner.ErrorEvent += ProgRunner_ErrorEvent;

                if (!progRunner.RunProgram(fiRawConverter.FullName, cmdStr, "RawConverter", true))
                {
                    // .RunProgram returned False
                    OnErrorEvent("Error running " + Path.GetFileNameWithoutExtension(fiRawConverter.Name));
                    return false;
                }

                if (m_DebugLevel >= 2)
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
