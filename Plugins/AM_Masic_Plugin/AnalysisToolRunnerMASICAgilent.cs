//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMasicPlugin
{
    // ReSharper disable once UnusedMember.Global

    /// <summary>
    /// Derived class for performing MASIC analysis on Agilent datasets
    /// </summary>
    public class AnalysisToolRunnerMASICAgilent : AnalysisToolRunnerMASICBase
    {
        // Ignore Spelling: MASIC, na

        protected override CloseOutType RunMASIC()
        {
            string parameterFilePath;

            var parameterFileName = mJobParams.GetParam("ParamFileName");

            if (parameterFileName != null && !string.Equals(parameterFileName.Trim(), "na", StringComparison.OrdinalIgnoreCase))
            {
                parameterFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("ParamFileName"));
            }
            else
            {
                parameterFilePath = string.Empty;
            }

            // Determine the path to the .Raw file
            var mgfFileName = mDatasetName + ".mgf";
            var inputFilePath = AnalysisResources.ResolveStoragePath(mWorkDir, mgfFileName);

            if (string.IsNullOrEmpty(inputFilePath))
            {
                // Unable to resolve the file path
                mErrorMessage = "Could not find " + mgfFileName + " or " + mgfFileName + AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX +
                                 " in the working folder; unable to run MASIC";
                LogError(mErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return StartMASICAndWait(inputFilePath, mWorkDir, parameterFilePath);
        }

        protected override CloseOutType DeleteDataFile()
        {
            // Deletes the .cdf and .mgf files from the working directory
            string[] foundFiles;

            // Delete the .cdf file
            try
            {
                foundFiles = Directory.GetFiles(mWorkDir, "*.cdf");

                foreach (var targetFile in foundFiles)
                {
                    DeleteFileWithRetries(targetFile);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting .cdf file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Delete the .mgf file
            try
            {
                foundFiles = Directory.GetFiles(mWorkDir, "*.mgf");

                foreach (var targetFile in foundFiles)
                {
                    DeleteFileWithRetries(targetFile);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting .mgf file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
