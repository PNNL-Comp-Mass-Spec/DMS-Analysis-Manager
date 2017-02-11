using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using PHRPReader;

namespace AnalysisManagerSMAQCPlugIn
{
    public class clsAnalysisResourcesSMAQC : clsAnalysisResources
    {
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve the parameter file
            string strParamFileName = m_jobParams.GetParam("ParmFileName");
            string strParamFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");

            if (!RetrieveFile(strParamFileName, strParamFileStoragePath))
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
            //if (RetrieveLLRCFiles())
            //{
            //    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            //}

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
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

            string strLLRCRunnerProgLoc = m_mgrParams.GetParam("LLRCRunnerProgLoc", @"\\gigasax\DMS_Programs\LLRCRunner");
            var lstFilesToCopy = new List<string>();

            lstFilesToCopy.Add(LLRC.LLRCWrapper.RDATA_FILE_ALLDATA);
            lstFilesToCopy.Add(LLRC.LLRCWrapper.RDATA_FILE_MODELS);

            foreach (string strFileName in lstFilesToCopy)
            {
                var fiSourceFile = new FileInfo(Path.Combine(strLLRCRunnerProgLoc, strFileName));

                if (!fiSourceFile.Exists)
                {
                    m_message = "LLRC RData file not found: " + fiSourceFile.FullName;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return false;
                }
                else
                {
                    fiSourceFile.CopyTo(Path.Combine(m_WorkingDir, fiSourceFile.Name));
                    m_jobParams.AddResultFileToSkip(fiSourceFile.Name);
                }
            }

            return true;
        }

        private bool RetrieveMASICFiles()
        {
            var createStoragePathInfoFile = false;

            string strMASICResultsFolderName = m_jobParams.GetParam("MASIC_Results_Folder_Name");

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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Retrieving the MASIC files by searching for any valid MASIC folder");
                }

                return RetrieveScanAndSICStatsFiles(retrieveSICStatsFile: true, createStoragePathInfoOnly: createStoragePathInfoFile,
                    retrieveScanStatsFile: true, retrieveScanStatsExFile: true, retrieveReporterIonsFile: true,
                    lstNonCriticalFileSuffixes: lstNonCriticalFileSuffixes);
            }
            else
            {
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Retrieving the MASIC files from " + strMASICResultsFolderName);
                }

                string ServerPath = null;
                ServerPath = FindValidFolder(m_DatasetName, "", strMASICResultsFolderName, 2);

                if (string.IsNullOrEmpty(ServerPath))
                {
                    m_message = "Dataset folder path not defined";
                }
                else
                {
                    if (ServerPath.StartsWith(MYEMSL_PATH_FLAG))
                    {
                        var bestSICFolderPath = Path.Combine(MYEMSL_PATH_FLAG, strMASICResultsFolderName);

                        return RetrieveScanAndSICStatsFiles(bestSICFolderPath, retrieveSICStatsFile: true,
                            createStoragePathInfoOnly: createStoragePathInfoFile, retrieveScanStatsFile: true, retrieveScanStatsExFile: true,
                            retrieveReporterIonsFile: true, lstNonCriticalFileSuffixes: lstNonCriticalFileSuffixes);
                    }

                    var diFolderInfo = new DirectoryInfo(ServerPath);

                    if (!diFolderInfo.Exists)
                    {
                        m_message = "Dataset folder not found: " + diFolderInfo.FullName;
                    }
                    else
                    {
                        //See if the ServerPath folder actually contains a subfolder named strMASICResultsFolderName
                        var diMASICFolderInfo = new DirectoryInfo(Path.Combine(diFolderInfo.FullName, strMASICResultsFolderName));

                        if (!diMASICFolderInfo.Exists)
                        {
                            m_message = "Unable to find MASIC results folder " + strMASICResultsFolderName;
                        }
                        else
                        {
                            return RetrieveScanAndSICStatsFiles(diMASICFolderInfo.FullName, retrieveSICStatsFile: true,
                                createStoragePathInfoOnly: createStoragePathInfoFile, retrieveScanStatsFile: true, retrieveScanStatsExFile: true,
                                retrieveReporterIonsFile: true, lstNonCriticalFileSuffixes: lstNonCriticalFileSuffixes);
                        }
                    }
                }
            }

            return false;
        }

        private bool RetrievePHRPFiles()
        {
            List<string> lstFileNamesToGet = new List<string>();
            clsPHRPReader.ePeptideHitResultType ePeptideHitResultType;

            // The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
            // For example, for dataset QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45 using Special_Processing of
            //   SourceJob:Auto{Tool = "XTandem" AND Settings_File = "IonTrapDefSettings.xml" AND [Parm File] = "xtandem_Rnd1PartTryp.xml"}
            // leads to the input folder being XTM201009211859_Auto625059

            string strInputFolder = null;
            strInputFolder = m_jobParams.GetParam("StepParameters", "InputFolderName");

            if (string.IsNullOrEmpty(strInputFolder))
            {
                m_message = "InputFolder step parameter not found; this is unexpected";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                return false;
            }

            if (strInputFolder.ToUpper().StartsWith("XTM"))
            {
                ePeptideHitResultType = clsPHRPReader.ePeptideHitResultType.XTandem;
            }
            else if (strInputFolder.ToUpper().StartsWith("SEQ"))
            {
                ePeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Sequest;
            }
            else if (strInputFolder.ToUpper().StartsWith("MSG"))
            {
                ePeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB;
            }
            else
            {
                m_message = "InputFolder is not an X!Tandem, Sequest, or MSGF+ folder; it should start with XTM, Seq, or MSG and is auto-determined by the SourceJob SpecialProcessing text";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                return false;
            }

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the PHRP files");
            }

            string strSynopsisFileName = null;

            strSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(ePeptideHitResultType, m_DatasetName);
            lstFileNamesToGet.Add(strSynopsisFileName);

            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPResultToSeqMapFileName(ePeptideHitResultType, m_DatasetName));
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqInfoFileName(ePeptideHitResultType, m_DatasetName));
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqToProteinMapFileName(ePeptideHitResultType, m_DatasetName));
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPModSummaryFileName(ePeptideHitResultType, m_DatasetName));
            lstFileNamesToGet.Add(clsPHRPReader.GetMSGFFileName(strSynopsisFileName));

            foreach (string FileToGet in lstFileNamesToGet)
            {
                if (!FindAndRetrieveMiscFiles(FileToGet, false))
                {
                    //Errors were reported in function call, so just return
                    return false;
                }
                m_jobParams.AddResultFileToSkip(FileToGet);
            }

            return true;
        }
    }
}