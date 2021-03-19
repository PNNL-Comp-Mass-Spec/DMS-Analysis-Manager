//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMasicPlugin
{
    /// <summary>
    /// Derived class for performing MASIC analysis on Finnigan datasets
    /// </summary>
    public class AnalysisToolRunnerMASICFinnigan : AnalysisToolRunnerMASICBase
    {
        // Ignore Spelling: parm

        #region "Module Variables"

        private AnalysisManagerMsXmlGenPlugIn.MSXMLCreator mMSXmlCreator;

        #endregion

        protected override CloseOutType RunMASIC()
        {
            string parameterFilePath;

            var parameterFileName = mJobParams.GetParam("parmFileName");

            if (parameterFileName != null && !string.Equals(parameterFileName.Trim(), "na", StringComparison.OrdinalIgnoreCase))
            {
                parameterFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("parmFileName"));
            }
            else
            {
                parameterFilePath = string.Empty;
            }

            // Determine the path to the .Raw file
            var rawFileName = mDatasetName + ".raw";
            var inputFilePath = AnalysisResources.ResolveStoragePath(mWorkDir, rawFileName);

            if (string.IsNullOrWhiteSpace(inputFilePath))
            {
                // Unable to resolve the file path
                mErrorMessage = "Could not find " + rawFileName + " or " + rawFileName + AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX +
                                 " in the working folder; unable to run MASIC";
                LogError(mErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Examine the size of the .Raw file
            var fiInputFile = new FileInfo(inputFilePath);
            if (!fiInputFile.Exists)
            {
                // Unable to resolve the file path
                mErrorMessage = "Could not find " + fiInputFile.FullName + "; unable to run MASIC";
                LogError(mErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!string.IsNullOrEmpty(parameterFilePath))
            {
                // Make sure the parameter file has IncludeHeaders defined and set to True
                ValidateParameterFile(parameterFilePath);
            }

            var eCloseout = StartMASICAndWait(inputFilePath, mWorkDir, parameterFilePath);

            return eCloseout;
        }

        /// <summary>
        /// Converts the .Raw file specified by thermoRawFile to a .mzXML file
        /// </summary>
        /// <param name="thermoRawFile"></param>
        /// <returns>Path to the newly created .mzXML file</returns>
#pragma warning disable IDE0051 // Remove unused private members
#pragma warning disable RCS1213 // Remove unused member declaration.
        private string ConvertRawToMzXML(FileInfo thermoRawFile)
#pragma warning restore RCS1213 // Remove unused member declaration.
#pragma warning restore IDE0051 // Remove unused private members
        {
            var msXmlGeneratorAppPath = GetMSXmlGeneratorAppPath();

            mMSXmlCreator = new AnalysisManagerMsXmlGenPlugIn.MSXMLCreator(msXmlGeneratorAppPath, mWorkDir, mDatasetName, mDebugLevel, mJobParams);
            RegisterEvents(mMSXmlCreator);
            mMSXmlCreator.LoopWaiting += MSXmlCreator_LoopWaiting;

            var success = mMSXmlCreator.CreateMZXMLFile();

            if (!success && string.IsNullOrEmpty(mMessage))
            {
                mMessage = mMSXmlCreator.ErrorMessage;
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Unknown error creating the mzXML file for dataset " + mDatasetName;
                }
                else if (!mMessage.Contains(mDatasetName))
                {
                    mMessage += "; dataset " + mDatasetName;
                }
            }

            if (!success)
                return string.Empty;

            var mzXMLFilePath = Path.ChangeExtension(thermoRawFile.FullName, "mzXML");
            if (!File.Exists(mzXMLFilePath))
            {
                mMessage = "MSXmlCreator did not create the .mzXML file";
                return string.Empty;
            }

            return mzXMLFilePath;
        }

        /// <summary>
        /// Deletes the .raw file from the working directory
        /// </summary>
        protected override CloseOutType DeleteDataFile()
        {
            try
            {
                var FoundFiles = Directory.GetFiles(mWorkDir, "*.raw");
                foreach (var MyFile in FoundFiles)
                {
                    DeleteFileWithRetries(MyFile);
                }
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error finding .raw files to delete", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        #region "Event Handlers"

        private void MSXmlCreator_LoopWaiting()
        {
            UpdateStatusFile();

            LogProgress("MSXmlCreator");
        }

        #endregion
    }
}
