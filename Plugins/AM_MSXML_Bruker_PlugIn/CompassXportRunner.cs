//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/30/2011
//
// Uses CompassXport to create a .mzXML or .mzML file
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using PRISM;

namespace AnalysisManagerMsXmlBrukerPlugIn
{
    public class CompassXportRunner : EventNotifier
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Centroiding, CompassXport, csv, jcamp, mzdata

        // ReSharper restore CommentTypo

        public enum MSXMLOutputTypeConstants
        {
            Invalid = -1,
            mzXML = 0,
            mzData = 1,
            mzML = 2,
            JCAMP = 3,
            CSV = 4
        }

        private readonly string mWorkDir;
        private readonly string mCompassXportProgramPath;
        private readonly string mDatasetName;
        private MSXMLOutputTypeConstants mOutputType;
        private readonly bool mCentroidMSXML;

        public event ProgRunnerStartingEventHandler ProgRunnerStarting;

        public delegate void ProgRunnerStartingEventHandler(string commandLine);

        public event LoopWaitingEventHandler LoopWaiting;

        public delegate void LoopWaitingEventHandler();

        public string ErrorMessage { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public CompassXportRunner(string workDir, string compassXportProgramPath, string datasetName,
                                     MSXMLOutputTypeConstants outputType, bool centroidMSXML)
        {
            mWorkDir = workDir;
            mCompassXportProgramPath = compassXportProgramPath;
            mDatasetName = datasetName;
            mOutputType = outputType;
            mCentroidMSXML = centroidMSXML;

            ErrorMessage = string.Empty;
        }

        /// <summary>
        /// Generate the mzXML or mzML file
        /// </summary>
        /// <returns>True if success; false if a failure</returns>
        public bool CreateMSXMLFile()
        {
            int formatMode;

            ErrorMessage = string.Empty;

            // Resolve the output file format
            if (mOutputType == MSXMLOutputTypeConstants.Invalid)
            {
                mOutputType = MSXMLOutputTypeConstants.mzXML;

                formatMode = 0;
            }
            else
            {
                formatMode = (int)mOutputType;
            }

            var msXmlFormatName = GetMsXmlOutputTypeByID(mOutputType);

            // Define the input file path
            var sourceFolderPath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_D_EXTENSION);
            var inputFilePath = Path.Combine(sourceFolderPath, "analysis.baf");

            if (!File.Exists(inputFilePath))
            {
                // Analysis.baf not found; look for analysis.yep instead
                inputFilePath = Path.Combine(sourceFolderPath, "analysis.yep");

                if (!File.Exists(inputFilePath))
                {
                    ErrorMessage = "Could not find analysis.baf or analysis.yep in " + mDatasetName + AnalysisResources.DOT_D_EXTENSION;
                    return false;
                }
            }

            // Define the output file path
            var outputFilePath = Path.Combine(mWorkDir, mDatasetName + "." + msXmlFormatName);

            // Verify that program file exists
            if (!File.Exists(mCompassXportProgramPath))
            {
                ErrorMessage = "Cannot find CompassXport exe program file: " + mCompassXportProgramPath;
                return false;
            }

            var cmdRunner = new RunDosProgram(Path.GetDirectoryName(mCompassXportProgramPath));
            cmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            // Set up and execute a program runner to run CompassXport executable

            var arguments = " -mode " + formatMode +
                            " -a " + inputFilePath +
                            " -o " + outputFilePath;

            if (mCentroidMSXML)
            {
                // Centroiding is enabled
                arguments += " -raw 0";
            }
            else
            {
                arguments += " -raw 1";
            }

            ProgRunnerStarting?.Invoke(mCompassXportProgramPath + arguments);

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;
            cmdRunner.EchoOutputToConsole = true;

            cmdRunner.WriteConsoleOutputToFile = true;
            cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(mCompassXportProgramPath) + "_ConsoleOutput.txt");

            var success = cmdRunner.RunProgram(mCompassXportProgramPath, arguments, Path.GetFileNameWithoutExtension(mCompassXportProgramPath), true);

            if (!success)
            {
                if (cmdRunner.ExitCode != 0)
                {
                    ErrorMessage = Path.GetFileNameWithoutExtension(mCompassXportProgramPath) + " returned a non-zero exit code: " +
                                    cmdRunner.ExitCode;
                    success = false;
                }
                else
                {
                    ErrorMessage = "Call to " + Path.GetFileNameWithoutExtension(mCompassXportProgramPath) + " failed (but exit code is 0)";
                    success = true;
                }
            }

            return success;
        }

        public static string GetMsXmlOutputTypeByID(MSXMLOutputTypeConstants fileType)
        {
            return fileType switch
            {
                MSXMLOutputTypeConstants.mzXML => "mzXML",
                MSXMLOutputTypeConstants.mzData => "mzData",
                MSXMLOutputTypeConstants.mzML => "mzML",
                MSXMLOutputTypeConstants.JCAMP => "JCAMP",
                MSXMLOutputTypeConstants.CSV => "CSV",
                _ => ""  // Includes MSXMLOutputTypeConstants.Invalid
            };
        }

        public static MSXMLOutputTypeConstants GetMsXmlOutputTypeByName(string typeName)
        {
            return typeName.ToLower() switch
            {
                "mzxml" => MSXMLOutputTypeConstants.mzXML,
                "mzdata" => MSXMLOutputTypeConstants.mzData,
                "mzml" => MSXMLOutputTypeConstants.mzML,
                "jcamp" => MSXMLOutputTypeConstants.JCAMP,
                "csv" => MSXMLOutputTypeConstants.CSV,
                _ => MSXMLOutputTypeConstants.Invalid
            };
        }

        /// <summary>
        /// Event handler for event CmdRunner.ErrorEvent
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        private void CmdRunner_ErrorEvent(string message, Exception ex)
        {
            ErrorMessage = message ?? string.Empty;
            OnErrorEvent(message, ex);
        }

        /// <summary>
        /// Event handler for event CmdRunner.LoopWaiting
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            LoopWaiting?.Invoke();
        }
    }
}
