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
            var strParamFileName = m_jobParams.GetParam("ParmFileName");
            var strParamFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");

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

            if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
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

            var strLLRCRunnerProgLoc = m_mgrParams.GetParam("LLRCRunnerProgLoc", @"\\gigasax\DMS_Programs\LLRCRunner");
            var lstFilesToCopy = new List<string> {
                LLRC.LLRCWrapper.RDATA_FILE_ALLDATA,
                LLRC.LLRCWrapper.RDATA_FILE_MODELS};


            foreach (var strFileName in lstFilesToCopy)
            {
                var fiSourceFile = new FileInfo(Path.Combine(strLLRCRunnerProgLoc, strFileName));

                if (!fiSourceFile.Exists)
                {
                    m_message = "LLRC RData file not found: " + fiSourceFile.FullName;
                    LogError(m_message);
                    return false;
                }
                else
                {
                    fiSourceFile.CopyTo(Path.Combine(m_WorkingDir, fiSourceFile.Name));
                    m_jobParams.AddResultFileToSkip(fiSourceFile.Name);
                }
            }

            return true;

#pragma warning restore 162

        }

        private bool RetrieveMASICFiles()
        {
            const bool createStoragePathInfoFile = false;

            var strMASICResultsFolderName = m_jobParams.GetParam("MASIC_Results_Folder_Name");

            m_jobParams.AddResultFileExtensionToSkip(SCAN_STATS_FILE_SUFFIX);        // _ScanStats.txt
            m_jobParams.AddResultFileExtensionToSkip(SCAN_STATS_EX_FILE_SUFFIX);     // _ScanStatsEx.txt
            m_jobParams.AddResultFileExtensionToSkip("_SICstats.txt");
            m_jobParams.AddResultFileExtensionToSkip(REPORTERIONS_FILE_SUFFIX);

            var lstNonCriticalFileSuffixes = new List<string>
            {
                SCAN_STATS_EX_FILE_SUFFIX,
                REPORTERIONS_FILE_SUFFIX
            };

            if (string.IsNullOrEmpty(strMASICResultsFolderName))
            {
                if (m_DebugLevel >= 2)
                {
                    LogDebug("Retrieving the MASIC files by searching for any valid MASIC folder");
                }

                return FileSearch.RetrieveScanAndSICStatsFiles(
                    retrieveSICStatsFile: true,
                    createStoragePathInfoOnly: createStoragePathInfoFile,
                    retrieveScanStatsFile: true,
                    retrieveScanStatsExFile: true,
                    retrieveReporterIonsFile: true,
                    lstNonCriticalFileSuffixes: lstNonCriticalFileSuffixes);
            }

            if (m_DebugLevel >= 2)
            {
                LogDebug("Retrieving the MASIC files from " + strMASICResultsFolderName);
            }

            var serverPath = FolderSearch.FindValidFolder(DatasetName, "", strMASICResultsFolderName, 2);

            if (string.IsNullOrEmpty(serverPath))
            {
                m_message = "Dataset folder path not defined";
            }
            else
            {
                if (serverPath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    var bestSICFolderPath = Path.Combine(MYEMSL_PATH_FLAG, strMASICResultsFolderName);

                    return FileSearch.RetrieveScanAndSICStatsFiles(
                        bestSICFolderPath,
                        retrieveSICStatsFile: true,
                        createStoragePathInfoOnly: createStoragePathInfoFile,
                        retrieveScanStatsFile: true,
                        retrieveScanStatsExFile: true,
                        retrieveReporterIonsFile: true,
                        lstNonCriticalFileSuffixes: lstNonCriticalFileSuffixes
                    );
                }

                var diFolderInfo = new DirectoryInfo(serverPath);

                if (!diFolderInfo.Exists)
                {
                    m_message = "Dataset folder not found: " + diFolderInfo.FullName;
                }
                else
                {
                    // See if the ServerPath folder actually contains a subfolder named strMASICResultsFolderName
                    var diMASICFolderInfo = new DirectoryInfo(Path.Combine(diFolderInfo.FullName, strMASICResultsFolderName));

                    if (!diMASICFolderInfo.Exists)
                    {
                        m_message = "Unable to find MASIC results folder " + strMASICResultsFolderName;
                    }
                    else
                    {
                        return FileSearch.RetrieveScanAndSICStatsFiles(
                            diMASICFolderInfo.FullName,
                            retrieveSICStatsFile: true,
                            createStoragePathInfoOnly: createStoragePathInfoFile,
                            retrieveScanStatsFile: true,
                            retrieveScanStatsExFile: true,
                            retrieveReporterIonsFile: true,
                            lstNonCriticalFileSuffixes: lstNonCriticalFileSuffixes);
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

            var strInputFolder = m_jobParams.GetParam("StepParameters", "InputFolderName");

            if (string.IsNullOrEmpty(strInputFolder))
            {
                m_message = "InputFolder step parameter not found; this is unexpected";
                LogError(m_message);
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
                m_message = "InputFolder is not an X!Tandem, Sequest, or MSGF+ folder: " + strInputFolder +
                    "; it should start with XTM, Seq, or MSG and is auto-determined by the SourceJob SpecialProcessing text";
                LogError(m_message);
                return false;
            }

            if (m_DebugLevel >= 2)
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

                m_jobParams.AddResultFileToSkip(fileToGet);
            }

            return true;
        }
    }
}