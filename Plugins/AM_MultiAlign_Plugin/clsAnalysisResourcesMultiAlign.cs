//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerMultiAlignPlugIn
{
    public class clsAnalysisResourcesMultiAlign : clsAnalysisResources
    {
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            LogMessage("Getting required files");

            string strInputFileExtension = string.Empty;

            var splitString = m_jobParams.GetParam("TargetJobFileList").Split(',');

            foreach (string row in splitString)
            {
                var fileNameExt = row.Split(':');
                if (fileNameExt.Length < 3)
                {
                    throw new InvalidOperationException("Malformed target job specification; must have three columns separated by two colons: " + row);
                }
                if (fileNameExt[2] == "nocopy")
                {
                    m_jobParams.AddResultFileExtensionToSkip(fileNameExt[1]);
                }
                strInputFileExtension = fileNameExt[1];
            }

            // Retrieve FeatureFinder _LCMSFeatures.txt or Decon2ls isos file for this dataset
            var fileToGet = DatasetName + strInputFileExtension;
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the MultiAlign Parameter .xml file specified for this job
            var multialignParamFileName = m_jobParams.GetParam("ParmFileName");
            if (string.IsNullOrEmpty(multialignParamFileName))
            {
                LogError(
                    "MultiAlign ParmFileName not defined in the settings for this job; unable to continue");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var paramFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "MultiAlign";
            var multialignParameterFileStoragePath = m_mgrParams.GetParam(paramFileStoragePathKeyName);
            if (string.IsNullOrEmpty(multialignParameterFileStoragePath))
            {
                multialignParameterFileStoragePath = @"\\gigasax\DMS_Parameter_Files\MultiAlign";
                LogWarning(
                    "Parameter '" + paramFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " +
                    multialignParameterFileStoragePath);
            }

            if (!CopyFileToWorkDir(multialignParamFileName, multialignParameterFileStoragePath, m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Build the MultiAlign input text file
            var success = BuildMultiAlignInputTextFile(strInputFileExtension);

            if (!success)
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected bool BuildMultiAlignInputTextFile(string strInputFileExtension)
        {
            const string INPUT_FILENAME = "input.txt";

            string TargetFilePath = Path.Combine(m_WorkingDir, INPUT_FILENAME);
            string DatasetFilePath = Path.Combine(m_WorkingDir, DatasetName + strInputFileExtension);

            var blnSuccess = true;

            // Create the MA input file
            try
            {
                using (var swOutFile = new StreamWriter(new FileStream(TargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swOutFile.WriteLine("[Files]");
                    swOutFile.WriteLine(DatasetFilePath);
                    //..\SARC_MS_Final\663878_Sarc_MS_13_24Aug10_Cheetah_10-08-02_0000_LCMSFeatures.txt

                    swOutFile.WriteLine("[Database]");

                    swOutFile.WriteLine("Database = " + m_jobParams.GetParam("AMTDB"));
                    swOutFile.WriteLine("Server = " + m_jobParams.GetParam("AMTDBServer"));
                    //Database = MT_Human_Sarcopenia_MixedLC_P692
                    //Server = elmer
                }
            }
            catch (Exception ex)
            {
                LogError(
                    "clsAnalysisResourcesMultiAlign.BuildMultiAlignInputTextFile, Error buliding the input .txt file (" + INPUT_FILENAME + "): " +
                    ex.Message);
                blnSuccess = false;
            }

            return blnSuccess;
        }
    }
}
