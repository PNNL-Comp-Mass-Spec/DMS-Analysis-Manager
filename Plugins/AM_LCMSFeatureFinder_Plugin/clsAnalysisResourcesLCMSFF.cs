//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.IO;
using System.Threading;
using AnalysisManagerBase;
using MyEMSLReader;
using PRISM;

namespace AnalysisManagerLCMSFeatureFinderPlugIn
{
    public class clsAnalysisResourcesLCMSFF : clsAnalysisResources
    {
        public const string SCANS_FILE_SUFFIX = "_scans.csv";
        public const string ISOS_FILE_SUFFIX = "_isos.csv";

        public override CloseOutType GetResources()
        {
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting required files");

            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve Decon2LS _scans.csv file for this dataset
            // The LCMSFeature Finder doesn't actually use the _scans.csv file, but we want to be sure it's present in the results folder
            var strFileToGet = DatasetName + SCANS_FILE_SUFFIX;
            if (!FileSearch.FindAndRetrieveMiscFiles(strFileToGet, false))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Retrieve Decon2LS _isos.csv files for this dataset
            strFileToGet = DatasetName + ISOS_FILE_SUFFIX;
            if (!FileSearch.FindAndRetrieveMiscFiles(strFileToGet, false))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            m_jobParams.AddResultFileToSkip(strFileToGet);

            // Retrieve the LCMSFeatureFinder .Ini file specified for this job
            var strLCMSFFIniFileName = m_jobParams.GetParam("LCMSFeatureFinderIniFile");
            if (strLCMSFFIniFileName == null || strLCMSFFIniFileName.Length == 0)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "LCMSFeatureFinderIniFile not defined in the settings for this job; unable to continue");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "LCMSFeatureFinder";
            var strFFIniFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName);
            if (strFFIniFileStoragePath == null || strFFIniFileStoragePath.Length == 0)
            {
                strFFIniFileStoragePath = "\\\\gigasax\\DMS_Parameter_Files\\LCMSFeatureFinder";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                    "Parameter '" + strParamFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + strFFIniFileStoragePath);
            }

            if (!CopyFileToWorkDir(strLCMSFFIniFileName, strFFIniFileStoragePath, m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            string strRawDataType = null;
            strRawDataType = m_jobParams.GetParam("RawDataType");

            if (strRawDataType.ToLower() == RAW_DATA_TYPE_DOT_UIMF_FILES)
            {
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving .UIMF file");
                }

                // IMS data; need to get the .UIMF file
                if (!FileSearch.RetrieveSpectra(strRawDataType))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    if (m_DebugLevel >= 1)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieved .UIMF file");
                    }
                }

                m_jobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION);
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Could add an extension of a file to delete, like this:
            // m_JobParams.AddResultFileExtensionToSkip(".dta")  'DTA files

            // Customize the LCMSFeatureFinder .Ini file to include the input file path and output folder path
            var success = UpdateFeatureFinderIniFile(strLCMSFFIniFileName);
            if (!success)
            {
                string Msg = "clsAnalysisResourcesLCMSFF.GetResources(), failed customizing .Ini file " + strLCMSFFIniFileName;
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = Msg;
                }
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected string GetValue(string strLine)
        {
            int intEqualsIndex = 0;
            string strValue = string.Empty;

            if (!string.IsNullOrEmpty(strLine))
            {
                intEqualsIndex = strLine.IndexOf('=');
                if (intEqualsIndex > 0 && intEqualsIndex < strLine.Length - 1)
                {
                    strValue = strLine.Substring(intEqualsIndex + 1);
                }
            }

            return strValue;
        }

        protected bool UpdateFeatureFinderIniFile(string strLCMSFFIniFileName)
        {
            const string INPUT_FILENAME_KEY = "InputFileName";
            const string OUTPUT_DIRECTORY_KEY = "OutputDirectory";
            const string FILTER_FILE_NAME_KEY = "DeconToolsFilterFileName";

            // Read the source .Ini file and update the settings for InputFileName and OutputDirectory
            // In addition, look for an entry for DeconToolsFilterFileName;
            //  if present, verify that the file exists and copy it locally (so that it will be included in the results folder)

            string SrcFilePath = Path.Combine(m_WorkingDir, strLCMSFFIniFileName);
            string TargetFilePath = Path.Combine(m_WorkingDir, strLCMSFFIniFileName + "_new");
            string IsosFilePath = Path.Combine(m_WorkingDir, DatasetName + ISOS_FILE_SUFFIX);

            string strLineIn = null;
            string strLineInLCase = null;

            bool blnInputFileDefined = false;
            bool blnOutputDirectoryDefined = false;

            var result = true;

            try
            {
                // Create the output file (temporary name ending in "_new"; we'll swap the files later)
                using (var swOutFile = new StreamWriter(new FileStream(TargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    try
                    {
                        // Open the input file
                        using (var srInFile = new StreamReader(new FileStream(SrcFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            while (!srInFile.EndOfStream)
                            {
                                strLineIn = srInFile.ReadLine();

                                if (strLineIn == null)
                                    continue;

                                strLineInLCase = strLineIn.ToLower().Trim();

                                if (strLineInLCase.StartsWith(INPUT_FILENAME_KEY.ToLower()))
                                {
                                    // Customize the input file name
                                    strLineIn = INPUT_FILENAME_KEY + "=" + IsosFilePath;
                                    blnInputFileDefined = true;
                                }

                                if (strLineInLCase.StartsWith(OUTPUT_DIRECTORY_KEY.ToLower()))
                                {
                                    // Customize the output directory name
                                    strLineIn = OUTPUT_DIRECTORY_KEY + "=" + m_WorkingDir;
                                    blnOutputDirectoryDefined = true;
                                }

                                if (strLineInLCase.StartsWith(FILTER_FILE_NAME_KEY.ToLower()))
                                {
                                    // Copy the file defined by DeconToolsFilterFileName= to the working directory

                                    var strValue = GetValue(strLineIn);

                                    if (!string.IsNullOrEmpty(strValue))
                                    {
                                        var fiFileInfo = new FileInfo(strValue);
                                        if (!fiFileInfo.Exists)
                                        {
                                            m_message = "Entry for " + FILTER_FILE_NAME_KEY + " in " + strLCMSFFIniFileName +
                                                        " points to an invalid file: " + strValue;
                                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                                            result = false;
                                            break;
                                        }
                                        else
                                        {
                                            // Copy the file locally
                                            string strTargetFilePath = Path.Combine(m_WorkingDir, fiFileInfo.Name);
                                            fiFileInfo.CopyTo(strTargetFilePath);
                                        }
                                    }
                                }

                                swOutFile.WriteLine(strLineIn);
                            }
                        }

                        if (!blnInputFileDefined)
                        {
                            swOutFile.WriteLine(INPUT_FILENAME_KEY + "=" + IsosFilePath);
                        }

                        if (!blnOutputDirectoryDefined)
                        {
                            swOutFile.WriteLine(OUTPUT_DIRECTORY_KEY + "=" + m_WorkingDir);
                        }
                    }
                    catch (Exception ex)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            "clsAnalysisResourcesLCMSFF.UpdateFeatureFinderIniFile, Error opening the .Ini file to customize (" + strLCMSFFIniFileName +
                            "): " + ex.Message);
                        result = false;
                    }
                }

                // Wait 250 millseconds, then replace the original .Ini file with the new one
                Thread.Sleep(250);
                clsProgRunner.GarbageCollectNow();

                // Delete the input file
                File.Delete(SrcFilePath);

                // Wait another 250 milliseconds before renaming the output file
                Thread.Sleep(50);
                clsProgRunner.GarbageCollectNow();

                // Rename the newly created output file to have the name of the input file
                File.Move(TargetFilePath, SrcFilePath);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "clsAnalysisResourcesLCMSFF.UpdateFeatureFinderIniFile, Error opening the .Ini file to customize (" + strLCMSFFIniFileName + "): " +
                    ex.Message);
                result = false;
            }

            return result;
        }

    }
}
