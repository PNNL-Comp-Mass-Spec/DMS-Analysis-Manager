//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.IO;

namespace AnalysisManagerMultiAlignPlugIn
{
    /// <summary>
    /// Retrieve resources for the MultiAlign plugin
    /// </summary>
    public class clsAnalysisResourcesMultiAlign : clsAnalysisResources
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

            LogMessage("Getting required files");

            var strInputFileExtension = string.Empty;

            var splitString = mJobParams.GetParam("TargetJobFileList").Split(',');

            foreach (var row in splitString)
            {
                var fileNameExt = row.Split(':');
                if (fileNameExt.Length < 3)
                {
                    throw new InvalidOperationException("Malformed target job specification; must have three columns separated by two colons: " + row);
                }
                if (fileNameExt[2] == "nocopy")
                {
                    mJobParams.AddResultFileExtensionToSkip(fileNameExt[1]);
                }
                strInputFileExtension = fileNameExt[1];
            }

            // Retrieve FeatureFinder _LCMSFeatures.txt or Decon2ls isos file for this dataset
            var fileToGet = DatasetName + strInputFileExtension;
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the MultiAlign Parameter .xml file specified for this job
            var multialignParamFileName = mJobParams.GetParam("ParmFileName");
            if (string.IsNullOrEmpty(multialignParamFileName))
            {
                LogError("MultiAlign ParmFileName not defined in the settings for this job; unable to continue");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var paramFileStoragePathKeyName = clsGlobal.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "MultiAlign";
            var multialignParameterFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);
            if (string.IsNullOrEmpty(multialignParameterFileStoragePath))
            {
                multialignParameterFileStoragePath = @"\\gigasax\DMS_Parameter_Files\MultiAlign";
                LogWarning(
                    "Parameter '" + paramFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " +
                    multialignParameterFileStoragePath);
            }

            if (!CopyFileToWorkDir(multialignParamFileName, multialignParameterFileStoragePath, mWorkDir))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Build the MultiAlign input text file
            var success = BuildMultiAlignInputTextFile(strInputFileExtension);

            if (!success)
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected bool BuildMultiAlignInputTextFile(string strInputFileExtension)
        {
            const string INPUT_FILENAME = "input.txt";

            var TargetFilePath = Path.Combine(mWorkDir, INPUT_FILENAME);
            var DatasetFilePath = Path.Combine(mWorkDir, DatasetName + strInputFileExtension);

            var blnSuccess = true;

            // Create the MA input file
            try
            {
                using var writer = new StreamWriter(new FileStream(TargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("[Files]");

                //..\SARC_MS_Final\663878_Sarc_MS_13_24Aug10_Cheetah_10-08-02_0000_LCMSFeatures.txt
                writer.WriteLine(DatasetFilePath);

                writer.WriteLine("[Database]");

                // Database = MT_Human_Sarcopenia_MixedLC_P692
                writer.WriteLine("Database = " + mJobParams.GetParam("AMTDB"));

                // Server = elmer
                writer.WriteLine("Server = " + mJobParams.GetParam("AMTDBServer"));
            }
            catch (Exception ex)
            {
                LogError("clsAnalysisResourcesMultiAlign.BuildMultiAlignInputTextFile, Error buliding the input .txt file " +
                         "(" + INPUT_FILENAME + "): " + ex.Message);
                blnSuccess = false;
            }

            return blnSuccess;
        }
    }
}
