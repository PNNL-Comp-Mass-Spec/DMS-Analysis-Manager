//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMultiAlignPlugIn
{
    /// <summary>
    /// Retrieve resources for the MultiAlign plugin
    /// </summary>
    public class AnalysisResourcesMultiAlign : AnalysisResources
    {
        // Ignore Spelling: isos, nocopy

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

            var inputFileExtension = string.Empty;

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
                inputFileExtension = fileNameExt[1];
            }

            // Retrieve FeatureFinder _LCMSFeatures.txt or Decon2ls isos file for this dataset
            var fileToGet = DatasetName + inputFileExtension;
            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the MultiAlign Parameter .xml file specified for this job
            var multialignParamFileName = mJobParams.GetParam("ParamFileName");
            if (string.IsNullOrEmpty(multialignParamFileName))
            {
                LogError("MultiAlign ParamFileName not defined in the settings for this job; unable to continue");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "MultiAlign";
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
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Build the MultiAlign input text file
            var success = BuildMultiAlignInputTextFile(inputFileExtension);

            if (!success)
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool BuildMultiAlignInputTextFile(string inputFileExtension)
        {
            const string INPUT_FILENAME = "input.txt";

            var targetFilePath = Path.Combine(mWorkDir, INPUT_FILENAME);
            var datasetFilePath = Path.Combine(mWorkDir, DatasetName + inputFileExtension);

            var success = true;

            // Create the MA input file
            try
            {
                using var writer = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("[Files]");

                //..\SARC_MS_Final\663878_Sarc_MS_13_24Aug10_Cheetah_10-08-02_0000_LCMSFeatures.txt
                writer.WriteLine(datasetFilePath);

                writer.WriteLine("[Database]");

                // Database = MT_Human_Sarcopenia_MixedLC_P692
                writer.WriteLine("Database = " + mJobParams.GetParam("AMTDB"));

                // Server = elmer
                writer.WriteLine("Server = " + mJobParams.GetParam("AMTDBServer"));
            }
            catch (Exception ex)
            {
                LogError("AnalysisResourcesMultiAlign.BuildMultiAlignInputTextFile, Error creating the input .txt file " +
                         "(" + INPUT_FILENAME + "): " + ex.Message);
                success = false;
            }

            return success;
        }
    }
}
