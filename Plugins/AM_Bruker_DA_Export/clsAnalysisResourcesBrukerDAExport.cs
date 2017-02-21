/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 04/29/2015                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;

namespace AnalysisManagerBrukerDAExportPlugin
{
    public class clsAnalysisResourcesBrukerDAExport : clsAnalysisResources
    {

        public override CloseOutType GetResources()
        {

            var currentTask = "Initializing";

            try
            {
                currentTask = "Retrieve shared resources";
                
                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS) {
                    return result;
                }

                // Retrieve the export script
                currentTask = "Get parameter BrukerSpectraExportScriptFile";
                var exportScriptName = m_jobParams.GetJobParameter("BrukerSpectraExportScriptFile", string.Empty);
                if (string.IsNullOrEmpty(exportScriptName))
                {
                    LogError("BrukerSpectraExportScriptFile parameter is empty");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the script file
                currentTask = "Retrieve the export script file: " + exportScriptName;

                const string paramFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "Bruker_DA_Export";

                var exportScriptStoragePath = m_mgrParams.GetParam(paramFileStoragePathKeyName);
                if (string.IsNullOrWhiteSpace(exportScriptStoragePath))
                {
                    exportScriptStoragePath = @"F:\My Documents\Gigasax_Data\DMS_Parameter_Files\Bruker_Data_Analysis";
                    LogWarning("Parameter '" + paramFileStoragePathKeyName + "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + exportScriptStoragePath);
                }

                if (!FileSearch.RetrieveFile(exportScriptName, exportScriptStoragePath))
                {
                    // Errors should have already been logged
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Get the instrument data
                var strRawDataType = m_jobParams.GetParam("RawDataType");

                var retrievalAttempts = 0;

                while (retrievalAttempts < 2)
                {
                    retrievalAttempts += 1;
                    switch (strRawDataType.ToLower())
                    {
                        case RAW_DATA_TYPE_DOT_D_FOLDERS:
                        case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                        case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                            currentTask = "Retrieve spectra: " + strRawDataType;

                            if (!FileSearch.RetrieveSpectra(strRawDataType))
                            {
                                LogError("AnalysisManagerBrukerDAExportPlugin.GetResources: Error occurred retrieving spectra.");
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                            break;
                        default:
                            m_message = "Dataset type " + strRawDataType + " is not supported";
                            LogWarning(
                                "AnalysisManagerBrukerDAExportPlugin.GetResources: " + m_message + "; must be " +
                                RAW_DATA_TYPE_DOT_D_FOLDERS + " or " + RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER);
                            return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (m_MyEMSLUtilities.FilesToDownload.Count == 0)
                    {
                        break;
                    }

                    currentTask = "Process the MyEMSL download queue";
                    if (ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                    {
                        break;
                    }

                    // Look for this file on the Samba share
                    base.DisableMyEMSLSearch();
                }

                // Delete any _1.mcf or _1.mcf_idx files (or similar)
                // The presence of those files can cause the Bruker DataAnalysis application to crash
                // especially if those files were created with a newer build of DataAnalysis
                // We observed this in January 2017 with Build 378 on Pub-88 vs. Build 384 on the instrument
                var searchSpecList = new List<string>
                {
                    "*_1.mcf",
                    "*_2.mcf",
                    "*_3.mcf",
                    "*_4.mcf",
                    "*_1.mcf_idx",
                    "*_2.mcf_idx",
                    "*_3.mcf_idx",
                    "*_4.mcf_idx",
                    "*.mcf_idx-journal",
                    "LockInfo",
                    "SyncHelper",
                    "ProjectCreationHelper"
                    // Possibly also add "Storage.mcf_idx"
                };

                currentTask = "Delete extra files";

                var workDirFolder = new DirectoryInfo(m_WorkingDir);
                var deleteAttemptCount = 0;
                var deleteSuccessCount = 0;

                foreach (var searchSpec in searchSpecList)
                {
                    var fileList = workDirFolder.GetFiles(searchSpec, SearchOption.AllDirectories);
                    foreach (var file in fileList)
                    {
                        deleteAttemptCount++;
                        try
                        {
                            file.Delete();
                            deleteSuccessCount++;
                        }
                        catch (Exception ex2)
                        {
                            LogError("Exception deleting file " + file.FullName, ex2);
                            m_message = string.Empty;
                        }
                        
                    }
                }

                if (deleteAttemptCount <= 0)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                if (deleteSuccessCount == deleteAttemptCount)
                {
                    if (deleteSuccessCount == 1)
                        LogDebugMessage("Deleted 1 extra file in the working directory");
                    else
                        LogDebugMessage($"Deleted {deleteSuccessCount} extra files in the working directory");
                }
                else
                {
                    LogDebugMessage(
                        $"Deleted extra files in the working directory: {deleteSuccessCount} of {deleteAttemptCount} successfully deleted");
                }

                return CloseOutType.CLOSEOUT_SUCCESS;

            }
            catch (Exception ex)
            {
                LogError("Exception in GetResources (task = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

    }
}
