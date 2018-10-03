//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System;
using AnalysisManagerBase;


namespace AnalysisManagerTopFDPlugIn
{
    /// <summary>
    /// Retrieve resources for the TopFD plugin
    /// </summary>
    public class clsAnalysisResourcesTopFD : clsAnalysisResources
    {

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                currentTask = "Retrieve shared resources";

                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                var topFdParamFile = mJobParams.GetParam("TopFD_ParamFile");
                if (string.IsNullOrWhiteSpace(topFdParamFile))
                {
                    LogError("TopFD parameter file not defined in the job settings (param name TopFD_ParamFile)");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }


                // Retrieve the TopFD parameter file
                currentTask = "Retrieve the TopFD parameter file " + topFdParamFile;

                const string paramFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "TopFD";

                var topFdParmFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);
                if (string.IsNullOrWhiteSpace(topFdParmFileStoragePath))
                {
                    topFdParmFileStoragePath = @"\\gigasax\dms_parameter_Files\TopFD";
                    LogWarning("Parameter '" + paramFileStoragePathKeyName + "' is not defined " +
                               "(obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); " +
                               "will assume: " + topFdParmFileStoragePath);
                }

                if (!FileSearch.RetrieveFile(topFdParamFile, topFdParmFileStoragePath))
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                currentTask = "Get Input file";

                var eResult = GetMzMLFile();
                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    mMessage = "";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Make sure we don't move the .mzML file into the results folder
                mJobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION);

                if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in GetResources: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }
    }
}
