//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/30/2011
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMsXmlBrukerPlugIn
{
    /// <summary>
    /// Class for running MSXML Bruker
    /// </summary>
    public class AnalysisToolRunnerMSXMLBruker : AnalysisToolRunnerBase
    {
        // Ignore Spelling: Bruker, CompassXport, gzip

        private const int PROGRESS_PCT_MSXML_GEN_RUNNING = 5;

        private const string COMPASS_XPORT = "CompassXport.exe";

        private DirectoryInfo mMSXmlCacheFolder;

        private CompassXportRunner mCompassXportRunner;

        private const int MAX_CSV_FILES = 50;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Presently not used</remarks>
        // ReSharper disable once EmptyConstructor
        public AnalysisToolRunnerMSXMLBruker()
        {
            // Empty constructor
        }

        /// <summary>
        /// Runs ReadW tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the CompassXport version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");

                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Error determining CompassXport version";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var msXMLCacheFolderPath = mMgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
            mMSXmlCacheFolder = new DirectoryInfo(msXMLCacheFolderPath);

            if (!mMSXmlCacheFolder.Exists)
            {
                LogError("MSXmlCache folder not found: " + msXMLCacheFolderPath);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var processingErrorMessage = string.Empty;

            var result = CreateMSXmlFile(out var resultsFile);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Something went wrong
                // In order to help diagnose things, move the output files into the results directory,
                // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Error running CompassXport";
                }
                processingErrorMessage = mMessage;
            }
            else
            {
                // Gzip the .mzML or .mzXML file then copy to the server cache
                PostProcessMsXmlFile(resultsFile);
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            // Delete the raw data files
            if (mDebugLevel > 3)
            {
                LogDebug("AnalysisToolRunnerMSXMLBruker.RunTool(), Deleting raw data file");
            }

            var deleteSuccess = DeleteRawDataFiles();

            if (!deleteSuccess)
            {
                LogError("AnalysisToolRunnerMSXMLBruker.RunTool(), Problem deleting raw data files: " + mMessage);

                if (!string.IsNullOrEmpty(processingErrorMessage))
                {
                    mMessage = processingErrorMessage;
                }
                else
                {
                    // Don't treat this as a critical error; leave eReturnCode unchanged
                    mMessage = "Error deleting raw data files";
                }
            }

            // Update the job summary file
            if (mDebugLevel > 3)
            {
                LogDebug("AnalysisToolRunnerMSXMLBruker.RunTool(), Updating summary file");
            }

            UpdateSummaryFile();

            var success = CopyResultsToTransferDirectory();

            if (success && result == CloseOutType.CLOSEOUT_SUCCESS)
                return CloseOutType.CLOSEOUT_SUCCESS;

            return CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Generate the mzXML or mzML file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType CreateMSXmlFile(out FileInfo resultsFile)
        {
            if (mDebugLevel > 4)
            {
                LogDebug("AnalysisToolRunnerMSXMLGen.CreateMSXmlFile(): Enter");
            }

            var msXmlGenerator = mJobParams.GetParam("MSXMLGenerator");
            // Typically CompassXport.exe

            var msXmlFormat = mJobParams.GetParam("MSXMLOutputType");

            // Typically mzXML or mzML
            var centroidMSXML = bool.Parse(mJobParams.GetParam("CentroidMSXML"));

            string compassXportProgramPath;

            // Initialize the Results File output parameter to a dummy name for now
            resultsFile = new FileInfo(Path.Combine(mWorkDir, "NonExistent_Placeholder_File.tmp"));

            if (string.Equals(msXmlGenerator, COMPASS_XPORT, StringComparison.OrdinalIgnoreCase))
            {
                compassXportProgramPath = mMgrParams.GetParam("CompassXportLoc");

                if (string.IsNullOrEmpty(compassXportProgramPath))
                {
                    mMessage = "Manager parameter CompassXportLoc is not defined in the Manager Control DB";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!File.Exists(compassXportProgramPath))
                {
                    mMessage = "CompassXport program not found";
                    LogError(mMessage + " at " + compassXportProgramPath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                mMessage = "Invalid value for MSXMLGenerator: " + msXmlGenerator;
                LogError(mMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var eOutputType = CompassXportRunner.GetMsXmlOutputTypeByName(msXmlFormat);

            if (eOutputType == CompassXportRunner.MSXMLOutputTypeConstants.Invalid)
            {
                LogWarning("msXmlFormat string is not recognized (" + msXmlFormat + "); it is typically mzXML, mzML, or CSV; will default to mzXML");
                eOutputType = CompassXportRunner.MSXMLOutputTypeConstants.mzXML;
            }

            resultsFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "." + CompassXportRunner.GetMsXmlOutputTypeByID(eOutputType)));

            // Instantiate the processing class
            mCompassXportRunner = new CompassXportRunner(mWorkDir, compassXportProgramPath, mDatasetName, eOutputType, centroidMSXML);
            RegisterEvents(mCompassXportRunner);

            mCompassXportRunner.LoopWaiting += CompassXportRunner_LoopWaiting;
            mCompassXportRunner.ProgRunnerStarting += CompassXportRunner_ProgRunnerStarting;

            // Create the file
            var success = mCompassXportRunner.CreateMSXMLFile();

            if (!success)
            {
                LogWarning(mCompassXportRunner.ErrorMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mCompassXportRunner.ErrorMessage.Length > 0)
            {
                LogWarning(mCompassXportRunner.ErrorMessage);
            }

            if (eOutputType != CompassXportRunner.MSXMLOutputTypeConstants.CSV)
            {
                if (resultsFile.Exists)
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                mMessage = "MSXml results file not found: " + resultsFile.FullName;
                LogError(mMessage);
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // CompassXport created one CSV file for each spectrum in the dataset
            // Confirm that fewer than 100 CSV files were created

            var workDir = new DirectoryInfo(mWorkDir);
            var csvFiles = workDir.GetFiles("*.csv").ToList();

            if (csvFiles.Count < MAX_CSV_FILES)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            mMessage = "CompassXport created " + csvFiles.Count +
                        " CSV files. The CSV conversion mode is only appropriate for datasets with fewer than " + MAX_CSV_FILES +
                        " spectra; create a mzXML file instead (e.g., settings file mzXML_Bruker.xml)";
            LogError(mMessage);

            foreach (var csvFile in csvFiles)
            {
                try
                {
                    csvFile.Delete();
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }

            return CloseOutType.CLOSEOUT_FAILED;
        }

        private void PostProcessMsXmlFile(FileInfo resultsFile)
        {
            // Compress the file using GZip
            LogMessage("GZipping " + resultsFile.Name);
            resultsFile = GZipFile(resultsFile);

            if (resultsFile == null)
            {
                return;
            }

            // Copy the .mzXML.gz or .mzML.gz file to the MSXML cache
            var remoteCacheFilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, resultsFile.FullName, purgeOldFilesIfNeeded: true);

            if (string.IsNullOrEmpty(remoteCacheFilePath))
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    LogError("CopyFileToServerCache returned false for " + resultsFile.Name);
                }

                return;
            }

            // Create the _CacheInfo.txt file
            var cacheInfoFilePath = resultsFile.FullName + "_CacheInfo.txt";
            using (var writer = new StreamWriter(new FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                writer.WriteLine(remoteCacheFilePath);
            }

            mJobParams.AddResultFileToSkip(resultsFile.Name);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>();

            var msXmlGenerator = mJobParams.GetParam("MSXMLGenerator");

            // Typically CompassXport.exe
            if (string.Equals(msXmlGenerator, COMPASS_XPORT, StringComparison.OrdinalIgnoreCase))
            {
                var compassXportPath = mMgrParams.GetParam("CompassXportLoc");

                if (string.IsNullOrEmpty(compassXportPath))
                {
                    mMessage = "Path defined by manager param CompassXportLoc is empty";
                    LogError(mMessage);
                    return false;
                }

                try
                {
                    toolFiles.Add(new FileInfo(compassXportPath));
                }
                catch (Exception ex)
                {
                    mMessage = "Path defined by manager param CompassXportLoc is invalid: " + compassXportPath;
                    LogError(mMessage + "; " + ex.Message);
                    return false;
                }
            }
            else
            {
                if (string.IsNullOrEmpty(msXmlGenerator))
                {
                    mMessage = "Job Parameter MSXMLGenerator is not defined";
                }
                else
                {
                    mMessage = "Invalid value for MSXMLGenerator, should be " + COMPASS_XPORT + ", not " + msXmlGenerator;
                }

                LogError(mMessage);
                return false;
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Event handler for CompassXportRunner.LoopWaiting event
        /// </summary>
        private void CompassXportRunner_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_MSXML_GEN_RUNNING);

            LogProgress("CompassXport");
        }

        /// <summary>
        /// Event handler for mCompassXportRunner.ProgRunnerStarting event
        /// </summary>
        /// <param name="commandLine">The command being executed (program path plus command line arguments)</param>
        private void CompassXportRunner_ProgRunnerStarting(string commandLine)
        {
            LogDebug(commandLine);
        }
    }
}
