//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/30/2011
//
// Uses CompassXport to create a .mzXML or .mzML file
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerMsXmlBrukerPlugIn
{
    public class clsCompassXportRunner : clsEventNotifier
    {
        #region "Enums"

        public enum MSXMLOutputTypeConstants
        {
            Invalid = -1,
            mzXML = 0,
            mzData = 1,
            mzML = 2,
            JCAMP = 3,
            CSV = 4
        }

        #endregion

        #region "Module Variables"

        private readonly string mWorkDir;
        private readonly string mCompassXportProgramPath;
        private readonly string mDatasetName;
        private MSXMLOutputTypeConstants mOutputType;
        private readonly bool mCentroidMSXML;

        private string mErrorMessage = string.Empty;

        public event ProgRunnerStartingEventHandler ProgRunnerStarting;

        public delegate void ProgRunnerStartingEventHandler(string CommandLine);

        public event LoopWaitingEventHandler LoopWaiting;

        public delegate void LoopWaitingEventHandler();

        #endregion

        #region "Properties"

        public string ErrorMessage
        {
            get
            {
                if (mErrorMessage == null)
                {
                    return string.Empty;
                }
                else
                {
                    return mErrorMessage;
                }
            }
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Presently not used</remarks>
        public clsCompassXportRunner(string WorkDir, string CompassXportProgramPath, string DatasetName, MSXMLOutputTypeConstants eOutputType,
            bool CentroidMSXML)
        {
            mWorkDir = WorkDir;
            mCompassXportProgramPath = CompassXportProgramPath;
            mDatasetName = DatasetName;
            mOutputType = eOutputType;
            mCentroidMSXML = CentroidMSXML;

            mErrorMessage = string.Empty;
        }

        /// <summary>
        /// Generate the mzXML or mzML file
        /// </summary>
        /// <returns>True if success; false if a failure</returns>
        /// <remarks></remarks>
        public bool CreateMSXMLFile()
        {
            string CmdStr = null;

            int intFormatMode = 0;

            string strSourceFolderPath = null;
            string strInputFilePath = null;
            string strOutputFilePath = null;

            bool blnSuccess = false;

            mErrorMessage = string.Empty;

            // Resolve the output file format
            if (mOutputType == MSXMLOutputTypeConstants.Invalid)
            {
                mOutputType = MSXMLOutputTypeConstants.mzXML;
                intFormatMode = 0;
            }
            else
            {
                intFormatMode = (int)mOutputType;
            }

            var strMSXmlFormatName = GetMsXmlOutputTypeByID(mOutputType);

            // Define the input file path
            strSourceFolderPath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_D_EXTENSION);
            strInputFilePath = Path.Combine(strSourceFolderPath, "analysis.baf");

            if (!File.Exists(strInputFilePath))
            {
                // Analysis.baf not found; look for analysis.yep instead
                strInputFilePath = Path.Combine(strSourceFolderPath, "analysis.yep");

                if (!File.Exists(strInputFilePath))
                {
                    mErrorMessage = "Could not find analysis.baf or analysis.yep in " + mDatasetName + clsAnalysisResources.DOT_D_EXTENSION;
                    return false;
                }
            }

            // Define the output file path
            strOutputFilePath = Path.Combine(mWorkDir, mDatasetName + "." + strMSXmlFormatName);

            // Verify that program file exists
            if (!File.Exists(mCompassXportProgramPath))
            {
                mErrorMessage = "Cannot find CompassXport exe program file: " + mCompassXportProgramPath;
                return false;
            }

            var cmdRunner = new clsRunDosProgram(Path.GetDirectoryName(mCompassXportProgramPath));
            cmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            //Set up and execute a program runner to run CompassXport executable

            CmdStr = " -mode " + intFormatMode.ToString() + " -a " + strInputFilePath + " -o " + strOutputFilePath;

            if (mCentroidMSXML)
            {
                // Centroiding is enabled
                CmdStr += " -raw 0";
            }
            else
            {
                CmdStr += " -raw 1";
            }

            if (ProgRunnerStarting != null)
            {
                ProgRunnerStarting(mCompassXportProgramPath + CmdStr);
            }

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;
            cmdRunner.EchoOutputToConsole = true;

            cmdRunner.WriteConsoleOutputToFile = true;
            cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(mCompassXportProgramPath) + "_ConsoleOutput.txt");

            blnSuccess = cmdRunner.RunProgram(mCompassXportProgramPath, CmdStr, Path.GetFileNameWithoutExtension(mCompassXportProgramPath), true);

            if (!blnSuccess)
            {
                if (cmdRunner.ExitCode != 0)
                {
                    mErrorMessage = Path.GetFileNameWithoutExtension(mCompassXportProgramPath) + " returned a non-zero exit code: " +
                                    cmdRunner.ExitCode.ToString();
                    blnSuccess = false;
                }
                else
                {
                    mErrorMessage = "Call to " + Path.GetFileNameWithoutExtension(mCompassXportProgramPath) + " failed (but exit code is 0)";
                    blnSuccess = true;
                }
            }

            return blnSuccess;
        }

        public static string GetMsXmlOutputTypeByID(MSXMLOutputTypeConstants eType)
        {
            switch (eType)
            {
                case MSXMLOutputTypeConstants.mzXML:
                    return "mzXML";
                case MSXMLOutputTypeConstants.mzData:
                    return "mzData";
                case MSXMLOutputTypeConstants.mzML:
                    return "mzML";
                case MSXMLOutputTypeConstants.JCAMP:
                    return "JCAMP";
                case MSXMLOutputTypeConstants.CSV:
                    return "CSV";
                default:
                    // Includes MSXMLOutputTypeConstants.Invalid
                    return "";
            }
        }

        public static MSXMLOutputTypeConstants GetMsXmlOutputTypeByName(string strName)
        {
            switch (strName.ToLower())
            {
                case "mzxml":
                    return MSXMLOutputTypeConstants.mzXML;
                case "mzdata":
                    return MSXMLOutputTypeConstants.mzData;
                case "mzml":
                    return MSXMLOutputTypeConstants.mzML;
                case "jcamp":
                    return MSXMLOutputTypeConstants.JCAMP;
                case "csv":
                    return MSXMLOutputTypeConstants.CSV;
                default:
                    return MSXMLOutputTypeConstants.Invalid;
            }
        }

        /// <summary>
        /// Event handler for event CmdRunner.ErrorEvent
        /// </summary>
        /// <param name="strMessage"></param>
        /// <param name="ex"></param>
        private void CmdRunner_ErrorEvent(string strMessage, Exception ex)
        {
            mErrorMessage = strMessage;
            OnErrorEvent(strMessage, ex);
        }

        /// <summary>
        /// Event handler for event CmdRunner.LoopWaiting
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            if (LoopWaiting != null)
            {
                LoopWaiting();
            }
        }

        #endregion
    }
}
