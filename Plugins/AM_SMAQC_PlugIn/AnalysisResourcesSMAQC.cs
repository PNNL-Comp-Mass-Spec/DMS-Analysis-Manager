using PHRPReader;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerSMAQCPlugIn
{
    /// <summary>
    /// Retrieve resources for the SMAQC plugin
    /// </summary>
    public class AnalysisResourcesSMAQC : AnalysisResources
    {
        // Ignore Spelling: Parm

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve the parameter file
            var paramFileName = mJobParams.GetParam("ParamFileName");
            var paramFileStoragePath = mJobParams.GetParam("ParamFileStoragePath");

            if (!FileSearchTool.RetrieveFile(paramFileName, paramFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            // Retrieve the PHRP files (for the X!Tandem, SEQUEST, or MS-GF+ source job)
            if (!RetrievePHRPFiles())
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // ReSharper disable once CommentTypo
            // Retrieve the MASIC ScanStats.txt, ScanStatsEx.txt, and _SICstats.txt files
            if (!RetrieveMASICFiles())
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // In use from June 2013 through November 12, 2015
            // Retrieve the LLRC .RData files
            // if (RetrieveLLRCFiles())
            // {
            //    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            // }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        [Obsolete("No longer used", true)]
        private bool RetrieveLLRCFiles()
        {
            if (!AnalysisToolRunnerSMAQC.LLRC_ENABLED)
                throw new Exception("LLRC is disabled -- do not call this method");

#pragma warning disable CS0162
#if false
            var llrcRunnerProgLoc = mMgrParams.GetParam("LLRCRunnerProgLoc", @"\\gigasax\DMS_Programs\LLRCRunner");
            var filesToCopy = new List<string> {
                LLRC.LLRCWrapper.RDATA_FILE_ALLDATA,
                LLRC.LLRCWrapper.RDATA_FILE_MODELS};

            foreach (var fileName in filesToCopy)
            {
                var sourceFile = new FileInfo(Path.Combine(llrcRunnerProgLoc, fileName));

                if (!sourceFile.Exists)
                {
                    mMessage = "LLRC RData file not found: " + sourceFile.FullName;
                    LogError(mMessage);
                    return false;
                }
                else
                {
                    sourceFile.CopyTo(Path.Combine(mWorkDir, sourceFile.Name));
                    mJobParams.AddResultFileToSkip(sourceFile.Name);
                }
            }

            return true;
#endif
#pragma warning restore CS0162

        }

        private bool RetrieveMASICFiles()
        {
            const bool createStoragePathInfoFile = false;

            var masicResultsDirectoryName = mJobParams.GetParam("MASIC_Results_Folder_Name");

            mJobParams.AddResultFileExtensionToSkip(SCAN_STATS_FILE_SUFFIX);        // _ScanStats.txt
            mJobParams.AddResultFileExtensionToSkip(SCAN_STATS_EX_FILE_SUFFIX);     // _ScanStatsEx.txt
            mJobParams.AddResultFileExtensionToSkip("_SICStats.txt");
            mJobParams.AddResultFileExtensionToSkip(REPORTERIONS_FILE_SUFFIX);

            var nonCriticalFileSuffixes = new List<string>
            {
                SCAN_STATS_EX_FILE_SUFFIX,
                REPORTERIONS_FILE_SUFFIX
            };

            if (string.IsNullOrEmpty(masicResultsDirectoryName))
            {
                if (mDebugLevel >= 2)
                {
                    LogDebug("Retrieving the MASIC files by searching for any valid MASIC folder");
                }

                return FileSearchTool.RetrieveScanAndSICStatsFiles(
                    retrieveSICStatsFile: true,
                    createStoragePathInfoOnly: createStoragePathInfoFile,
                    retrieveScanStatsFile: true,
                    retrieveScanStatsExFile: true,
                    retrieveReporterIonsFile: true,
                    nonCriticalFileSuffixes: nonCriticalFileSuffixes);
            }

            if (mDebugLevel >= 2)
            {
                LogDebug("Retrieving the MASIC files from " + masicResultsDirectoryName);
            }

            var serverPath = DirectorySearchTool.FindValidDirectory(DatasetName, "", masicResultsDirectoryName, 2);

            if (string.IsNullOrEmpty(serverPath))
            {
                mMessage = "Dataset directory path not defined";
            }
            else
            {
                if (serverPath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    var bestMasicResultsDirectoryPath = Path.Combine(MYEMSL_PATH_FLAG, masicResultsDirectoryName);

                    return FileSearchTool.RetrieveScanAndSICStatsFiles(
                        bestMasicResultsDirectoryPath,
                        retrieveSICStatsFile: true,
                        createStoragePathInfoOnly: createStoragePathInfoFile,
                        retrieveScanStatsFile: true,
                        retrieveScanStatsExFile: true,
                        retrieveReporterIonsFile: true,
                        nonCriticalFileSuffixes: nonCriticalFileSuffixes
                    );
                }

                var datasetDirectory = new DirectoryInfo(serverPath);

                if (!datasetDirectory.Exists)
                {
                    mMessage = "Dataset directory not found: " + datasetDirectory.FullName;
                }
                else
                {
                    // See if the ServerPath folder actually contains a subdirectory named masicResultsDirectoryName
                    var masicResultsDirectory = new DirectoryInfo(Path.Combine(datasetDirectory.FullName, masicResultsDirectoryName));

                    if (!masicResultsDirectory.Exists)
                    {
                        mMessage = "Unable to find MASIC results folder " + masicResultsDirectoryName;
                    }
                    else
                    {
                        return FileSearchTool.RetrieveScanAndSICStatsFiles(
                            masicResultsDirectory.FullName,
                            retrieveSICStatsFile: true,
                            createStoragePathInfoOnly: createStoragePathInfoFile,
                            retrieveScanStatsFile: true,
                            retrieveScanStatsExFile: true,
                            retrieveReporterIonsFile: true,
                            nonCriticalFileSuffixes: nonCriticalFileSuffixes);
                    }
                }
            }

            return false;
        }

        private bool RetrievePHRPFiles()
        {
            var fileNamesToGet = new List<string>();
            PeptideHitResultTypes resultType;

            // The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
            // For example, for dataset QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45 using Special_Processing of
            //   SourceJob:Auto{Tool = "XTandem" AND Settings_File = "IonTrapDefSettings.xml" AND [Param File] = "xtandem_Rnd1PartTryp.xml"}
            // leads to the input folder being XTM201009211859_Auto625059

            var inputFolder = mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "InputFolderName");

            if (string.IsNullOrEmpty(inputFolder))
            {
                mMessage = "InputFolder step parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            if (inputFolder.StartsWith("XTM", StringComparison.InvariantCultureIgnoreCase))
            {
                resultType = PeptideHitResultTypes.XTandem;
            }
            else if (inputFolder.StartsWith("SEQ", StringComparison.InvariantCultureIgnoreCase))
            {
                resultType = PeptideHitResultTypes.Sequest;
            }
            else if (inputFolder.StartsWith("MSG", StringComparison.InvariantCultureIgnoreCase))
            {
                resultType = PeptideHitResultTypes.MSGFPlus;
            }
            else
            {
                mMessage = "InputFolder is not an X!Tandem, SEQUEST, or MS-GF+ folder: " + inputFolder +
                    "; it should start with XTM, Seq, or MSG and is auto-determined by the SourceJob SpecialProcessing text";
                LogError(mMessage);
                return false;
            }

            if (mDebugLevel >= 2)
            {
                LogDebug("Retrieving the PHRP files");
            }

            var msgfplusSynopsisFile = ReaderFactory.GetPHRPSynopsisFileName(resultType, DatasetName);
            var synFileToFind = msgfplusSynopsisFile;

            var success = FileSearchTool.FindAndRetrievePHRPDataFile(ref synFileToFind, "", addToResultFileSkipList: true, logFileNotFound: true, logRemoteFilePath: true);

            if (!success)
            {
                // Errors were reported in method call, so just return
                return false;
            }

            // Check whether we are loading data where the filenames are _msgfdb.txt instead of _msgfplus.txt
            var autoSwitchFilename = !string.Equals(synFileToFind, msgfplusSynopsisFile);

            if (autoSwitchFilename)
            {
                msgfplusSynopsisFile = synFileToFind;
            }

            fileNamesToGet.Add(ReaderFactory.GetPHRPResultToSeqMapFileName(resultType, DatasetName));
            fileNamesToGet.Add(ReaderFactory.GetPHRPSeqInfoFileName(resultType, DatasetName));
            fileNamesToGet.Add(ReaderFactory.GetPHRPSeqToProteinMapFileName(resultType, DatasetName));
            fileNamesToGet.Add(ReaderFactory.GetPHRPModSummaryFileName(resultType, DatasetName));
            fileNamesToGet.Add(ReaderFactory.GetMSGFFileName(msgfplusSynopsisFile));

            foreach (var phrpFile in fileNamesToGet)
            {
                string fileToGet;

                if (autoSwitchFilename)
                {
                    fileToGet = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(phrpFile, msgfplusSynopsisFile);
                }
                else
                {
                    fileToGet = phrpFile;
                }

                success = FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, unzip: false);

                if (!success)
                {
                    // Errors were reported in method call, so just return
                    return false;
                }

                mJobParams.AddResultFileToSkip(fileToGet);
            }

            return true;
        }
    }
}