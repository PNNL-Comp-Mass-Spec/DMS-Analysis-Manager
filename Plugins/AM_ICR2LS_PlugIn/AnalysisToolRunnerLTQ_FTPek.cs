using AnalysisManagerBase;
using PRISM.Logging;
using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerICR2LSPlugIn
{
    /// <summary>
    /// Performs PEK analysis using ICR-2LS on LTQ-FT MS data
    /// </summary>
    public class AnalysisToolRunnerLTQ_FTPek : AnalysisToolRunnerICRBase
    {
        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            // Start with base class method to get settings information
            var resultCode = base.RunTool();
            if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
                return resultCode;

            // Store the ICR2LS version info in the database
            if (!StoreToolVersionInfo())
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "Aborting since StoreToolVersionInfo returned false");
                mMessage = "Error determining ICR2LS version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Verify a param file has been specified
            var paramFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("parmFileName"));
            if (!File.Exists(paramFilePath))
            {
                // Param file wasn't specified, but is required for ICR-2LS analysis
                mMessage = "ICR-2LS Param file not found";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage + ": " + paramFilePath);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Add handling of settings file info here if it becomes necessary in the future

            // Get scan settings from settings file
            var minScan = mJobParams.GetJobParameter("scanStart", 0);
            var maxScan = mJobParams.GetJobParameter("ScanStop", 0);

            // Determine whether or not we should be processing MS2 spectra
            var skipMS2 = !mJobParams.GetJobParameter("ProcessMS2", false);

            // ReSharper disable once ArrangeRedundantParentheses
            var useAllScans = (minScan == 0 && maxScan == 0) || minScan > maxScan || maxScan > 500000;

            // Assemble the data file name and path
            var datasetNamePath = Path.Combine(mWorkDir, mDatasetName + ".raw");
            if (!File.Exists(datasetNamePath))
            {
                mMessage = "Raw file not found: " + datasetNamePath;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Assemble the output file name and path
            var outFileNamePath = Path.Combine(mWorkDir, mDatasetName + ".pek");

            var success = StartICR2LS(datasetNamePath, paramFilePath, outFileNamePath, ICR2LSProcessingModeConstants.LTQFTPEK, useAllScans, skipMS2,
                minScan, maxScan);

            if (success)
            {
                if (!VerifyPEKFileExists(mWorkDir, mDatasetName))
                {
                    mMessage = "ICR-2LS successfully finished but did not make a .Pek file; if all spectra are MS/MS use settings file LTQ_FTPEK_ProcessMS2.txt";
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Error running ICR-2LS on file " + datasetNamePath);

                // If a .PEK file exists, call PerfPostAnalysisTasks() to move the .Pek file into the results folder, which we'll then archive in the Failed Results folder
                if (VerifyPEKFileExists(mWorkDir, mDatasetName))
                {
                    mMessage = "ICR-2LS returned false (see .PEK file in Failed results folder)";
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                        ".Pek file was found, so will save results to the failed results archive folder");

                    PerfPostAnalysisTasks(false);

                    // Try to save whatever files were moved into the results directory
                    var analysisResults = new AnalysisResults(mMgrParams, mJobParams);
                    analysisResults.CopyFailedResultsToArchiveDirectory(Path.Combine(mWorkDir, mResultsDirectoryName));
                }
                else
                {
                    mMessage = "Error running ICR-2LS (.Pek file not found in " + mWorkDir + ")";
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Run the cleanup routine from the base class
            if (PerfPostAnalysisTasks(true) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Error performing post analysis tasks";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected override CloseOutType DeleteDataFile()
        {
            // Deletes the .raw file from the working directory

            // Delete the .raw file
            try
            {
                // Allow extra time for ICR2LS to release file locks
                Global.IdleLoop(5);
                var foundFiles = Directory.GetFiles(mWorkDir, "*.raw");
                foreach (var targetFile in foundFiles)
                {
                    // Add the file to .FilesToDelete just in case the deletion fails
                    mJobParams.AddResultFileToSkip(targetFile);
                    DeleteFileWithRetries(targetFile);
                }
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.ERROR,
                    "Error deleting .raw file, job " + mJob + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
