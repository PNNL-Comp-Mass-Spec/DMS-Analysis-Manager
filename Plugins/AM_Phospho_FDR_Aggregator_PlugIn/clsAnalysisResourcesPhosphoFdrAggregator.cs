using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;
using PRISM.Logging;

namespace AnalysisManagerPhospho_FDR_AggregatorPlugIn
{
    /// <summary>
    /// Retrieve resources for the Phospho FDR Aggregator plugin
    /// </summary>
    public class clsAnalysisResourcesPhosphoFdrAggregator : clsAnalysisResources
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

            // Lookup the file processing options, for example:
            // sequest:_syn.txt:nocopy,sequest:_fht.txt:nocopy,sequest:_dta.zip:nocopy,masic_finnigan:_ScanStatsEx.txt:nocopy
            // MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_fht.txt,MSGFPlus:_dta.zip,masic_finnigan:_ScanStatsEx.txt

            var fileSpecList = m_jobParams.GetParam("TargetJobFileList").Split(',').ToList();

            foreach (var fileSpec in fileSpecList.ToList())
            {
                var fileSpecTerms = fileSpec.Split(':').ToList();
                if (fileSpecTerms.Count <= 2 || fileSpecTerms[2].ToLower().Trim() != "copy")
                {
                    m_jobParams.AddResultFileExtensionToSkip(fileSpecTerms[1]);
                }
            }

            LogMessage("Getting param file");

            var paramFilesCopied = 0;

            if (!RetrieveAScoreParamfile("AScoreCIDParamFile", ref paramFilesCopied))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!RetrieveAScoreParamfile("AScoreETDParamFile", ref paramFilesCopied))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!RetrieveAScoreParamfile("AScoreHCDParamFile", ref paramFilesCopied))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (paramFilesCopied == 0)
            {
                m_message = "One more more of these job parameters must define a valid AScore parameter file name: AScoreCIDParamFile, AScoreETDParamFile, or AScoreHCDParamFile";
                LogError(m_message);
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            LogMessage("Retrieving input files");

            // Retrieve the files for the jobs in the data package associated with this job
            if (!RetrieveAggregateFiles(fileSpecList, DataPackageFileRetrievalModeConstants.Ascore, out var dctDataPackageJobs))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Cache the data package info
            if (!CacheDataPackageInfo(dctDataPackageJobs))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected bool CacheDataPackageInfo(Dictionary<int, clsDataPackageJobInfo> dctDataPackageJobs)
        {
            try
            {
                var diWorkingFolder = new DirectoryInfo(m_WorkingDir);

                var jobToDatasetMap = new Dictionary<string, string>();
                var jobToSettingsFileMap = new Dictionary<string, string>();
                var jobToolMap = new Dictionary<string, string>();

                // Find the Job* folders

                foreach (var subFolder in diWorkingFolder.GetDirectories("Job*"))
                {
                    var jobNumber = int.Parse(subFolder.Name.Substring(3));
                    var udtJobInfo = dctDataPackageJobs[jobNumber];

                    jobToDatasetMap.Add(jobNumber.ToString(), udtJobInfo.Dataset);
                    jobToSettingsFileMap.Add(jobNumber.ToString(), udtJobInfo.SettingsFileName);
                    jobToolMap.Add(jobNumber.ToString(), udtJobInfo.Tool);
                }

                // Store the packed job parameters
                StorePackedJobParameterDictionary(jobToDatasetMap, JOB_PARAM_DICTIONARY_JOB_DATASET_MAP);
                StorePackedJobParameterDictionary(jobToSettingsFileMap, JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP);
                StorePackedJobParameterDictionary(jobToolMap, JOB_PARAM_DICTIONARY_JOB_TOOL_MAP);
            }
            catch (Exception ex)
            {
                m_message = "Error in CacheDataPackageInfo";
                LogError("Error in CacheDataPackageInfo: " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Retrieves the AScore parameter file stored in the given parameter name
        /// </summary>
        /// <param name="parameterName">AScoreCIDParamFile or AScoreETDParamFile or AScoreHCDParamFile</param>
        /// <param name="paramFilesCopied">Incremented if the parameter file is found and copied</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool RetrieveAScoreParamfile(string parameterName, ref int paramFilesCopied)
        {
            var paramFileName = m_jobParams.GetJobParameter(parameterName, string.Empty);
            if (string.IsNullOrWhiteSpace(paramFileName))
            {
                return true;
            }

            if (paramFileName.ToLower().StartsWith("xxx_ascore_") || paramFileName.ToLower().StartsWith("xxx_undefined"))
            {
                // Dummy parameter file; ignore it
                // Update the job parameter to be an empty string so that this parameter is ignored in BuildInputFile
                m_jobParams.SetParam(parameterName, string.Empty);
                return true;
            }

            var success = FileSearch.RetrieveFile(paramFileName, m_jobParams.GetParam("transferFolderPath"), 2, BaseLogger.LogLevels.DEBUG);

            if (!success)
            {
                // File not found in the transfer folder
                // Look in the AScore parameter folder on Gigasax, \\gigasax\DMS_Parameter_Files\AScore

                var paramFileFolder = m_jobParams.GetJobParameter("ParamFileStoragePath", @"\\gigasax\DMS_Parameter_Files\AScore");
                success = FileSearch.RetrieveFile(paramFileName, paramFileFolder, 2);
            }

            if (success)
            {
                paramFilesCopied += 1;
            }

            return success;
        }
    }
}
