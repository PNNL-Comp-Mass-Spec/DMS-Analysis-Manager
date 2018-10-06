using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using PHRPReader;

namespace AnalysisManagerSMAQCPlugIn
{

    /// <summary>
    /// Retrieve resources for the SMAQC plugin
    /// </summary>
    public class clsAnalysisResourcesSMAQC : clsAnalysisResources
    {

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
            var strParamFileName = mJobParams.GetParam("ParmFileName");
            var strParamFileStoragePath = mJobParams.GetParam("ParmFileStoragePath");

            if (!FileSearch.RetrieveFile(strParamFileName, strParamFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            // Retrieve the PHRP files (for the X!Tandem, Sequest, or MSGF+ source job)
            if (!RetrievePHRPFiles())
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

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

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        [Obsolete("No longer used")]
        private bool RetrieveLLRCFiles()
        {
            if (!clsAnalysisToolRunnerSMAQC.LLRC_ENABLED)
                throw new Exception("LLRC is disabled -- do not call this function");

#pragma warning disable 162

            var strLLRCRunnerProgLoc = mMgrParams.GetParam("LLRCRunnerProgLoc", @"\\gigasax\DMS_Programs\LLRCRunner");
            var lstFilesToCopy = new List<string> {
                LLRC.LLRCWrapper.RDATA_FILE_ALLDATA,
                LLRC.LLRCWrapper.RDATA_FILE_MODELS};


            foreach (var strFileName in lstFilesToCopy)
            {
                var fiSourceFile = new FileInfo(Path.Combine(strLLRCRunnerProgLoc, strFileName));

                if (!fiSourceFile.Exists)
                {
                    mMessage = "LLRC RData file not found: " + fiSourceFile.FullName;
                    LogError(mMessage);
                    return false;
                }
                else
                {
                    fiSourceFile.CopyTo(Path.Combine(mWorkDir, fiSourceFile.Name));
                    mJobParams.AddResultFileToSkip(fiSourceFile.Name);
                }
            }

            return true;

#pragma warning restore 162

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

                return FileSearch.RetrieveScanAndSICStatsFiles(
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

            var serverPath = FolderSearch.FindValidFolder(DatasetName, "", masicResultsDirectoryName, 2);

            if (string.IsNullOrEmpty(serverPath))
            {
                mMessage = "Dataset directory path not defined";
            }
            else
            {
                if (serverPath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    var bestMasicResultsDirectoryPath = Path.Combine(MYEMSL_PATH_FLAG, masicResultsDirectoryName);

                    return FileSearch.RetrieveScanAndSICStatsFiles(
                        bestMasicResultsDirectoryPath,
                        retrieveSICStatsFile: true,
                        createStoragePathInfoOnly: createStoragePathInfoFile,
                        retrieveScanStatsFile: true,
                        retrieveScanStatsExFile: true,
                        retrieveReporterIonsFile: true,
                        nonCriticalFileSuffixes: nonCriticalFileSuffixes
                    );
                }

                var diFolderInfo = new DirectoryInfo(serverPath);

                if (!diFolderInfo.Exists)
                {
                    mMessage = "Dataset directory not found: " + diFolderInfo.FullName;
                }
                else
                {
                    // See if the ServerPath folder actually contains a subfolder named strMASICResultsFolderName
                    var masicResultsDirectory = new DirectoryInfo(Path.Combine(diFolderInfo.FullName, masicResultsDirectoryName));

                    if (!masicResultsDirectory.Exists)
                    {
                        mMessage = "Unable to find MASIC results folder " + masicResultsDirectoryName;
                    }
                    else
                    {
                        return FileSearch.RetrieveScanAndSICStatsFiles(
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
            var lstFileNamesToGet = new List<string>();
            clsPHRPReader.ePeptideHitResultType ePeptideHitResultType;

            // The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
            // For example, for dataset QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45 using Special_Processing of
            //   SourceJob:Auto{Tool = "XTandem" AND Settings_File = "IonTrapDefSettings.xml" AND [Parm File] = "xtandem_Rnd1PartTryp.xml"}
            // leads to the input folder being XTM201009211859_Auto625059

            var strInputFolder = mJobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "InputFolderName");

            if (string.IsNullOrEmpty(strInputFolder))
            {
                mMessage = "InputFolder step parameter not found; this is unexpected";
                LogError(mMessage);
                return false;
            }

            if (strInputFolder.StartsWith("XTM", StringComparison.InvariantCultureIgnoreCase))
            {
                ePeptideHitResultType = clsPHRPReader.ePeptideHitResultType.XTandem;
            }
            else if (strInputFolder.StartsWith("SEQ", StringComparison.InvariantCultureIgnoreCase))
            {
                ePeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Sequest;
            }
            else if (strInputFolder.StartsWith("MSG", StringComparison.InvariantCultureIgnoreCase))
            {
                ePeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB;
            }
            else
            {
                mMessage = "InputFolder is not an X!Tandem, Sequest, or MSGF+ folder: " + strInputFolder +
                    "; it should start with XTM, Seq, or MSG and is auto-determined by the SourceJob SpecialProcessing text";
                LogError(mMessage);
                return false;
            }

            if (mDebugLevel >= 2)
            {
                LogDebug("Retrieving the PHRP files");
            }

            var msgfplusSynopsisFile = clsPHRPReader.GetPHRPSynopsisFileName(ePeptideHitResultType, DatasetName);
            var synFileToFind = string.Copy(msgfplusSynopsisFile);

            var success = FileSearch.FindAndRetrievePHRPDataFile(ref synFileToFind, "", addToResultFileSkipList: true);
            if (!success)
            {
                // Errors were reported in function call, so just return
                return false;
            }

            // Check whether we are loading data where the filenames are _msgfdb.txt instead of _msgfplus.txt
            var autoSwitchFilename = !string.Equals(synFileToFind, msgfplusSynopsisFile);
            if (autoSwitchFilename)
            {
                msgfplusSynopsisFile = synFileToFind;
            }

            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPResultToSeqMapFileName(ePeptideHitResultType, DatasetName));
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqInfoFileName(ePeptideHitResultType, DatasetName));
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqToProteinMapFileName(ePeptideHitResultType, DatasetName));
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPModSummaryFileName(ePeptideHitResultType, DatasetName));
            lstFileNamesToGet.Add(clsPHRPReader.GetMSGFFileName(msgfplusSynopsisFile));

            foreach (var phrpFile in lstFileNamesToGet)
            {
                string fileToGet;
                if (autoSwitchFilename)
                {
                    fileToGet = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(phrpFile, msgfplusSynopsisFile);

                }
                else
                {
                    fileToGet = phrpFile;
                }

                success = FileSearch.FindAndRetrieveMiscFiles(fileToGet, unzip: false);

                if (!success)
                {
                    // Errors were reported in function call, so just return
                    return false;
                }

                mJobParams.AddResultFileToSkip(fileToGet);
            }

            return true;
        }
    }
}