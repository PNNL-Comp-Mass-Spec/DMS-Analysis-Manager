/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 04/02/2026                                           **
**                                                              **
*****************************************************************/

using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using PRISM.AppSettings;

namespace AnalysisManagerNOMAnnotationPlugin
{
    /// <summary>
    /// Retrieve resources for the NOM Annotation plugin
    /// </summary>
    public class AnalysisResourcesNOMAnnotation : AnalysisResources
    {
        /// <summary>
        /// Job parameter used to track the reference mass file defined in the parameter file
        /// </summary>
        public const string JOB_PARAM_REFERENCE_MASS_FILE = "ReferenceMassFile";

        /// <summary>
        /// Retrieves files necessary for annotating natural organic matter features
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
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

                // Retrieve the parameter file
                currentTask = "Retrieve the parameter file";
                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                var paramFileStoragePath = mJobParams.GetParam(JOB_PARAM_PARAM_FILE_STORAGE_PATH);

                if (!FileSearchTool.RetrieveFile(paramFileName, paramFileStoragePath))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Examine the parameter file to determine the reference mass file to use
                var refMassFileResult = RetrieveReferenceMassFile(paramFileStoragePath, paramFileName);

                if (refMassFileResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                var datasetFileRetriever = new DatasetFileRetriever(this);
                RegisterEvents(datasetFileRetriever);

                const int DATA_PACKAGE_ID = 0;
                const bool RETRIEVE_MSXML_FILES = false;

                var datasetCopyResult = datasetFileRetriever.RetrieveInstrumentFilesForJobDatasets(
                    DATA_PACKAGE_ID,
                    RETRIEVE_MSXML_FILES,
                    AnalysisToolRunnerNOMAnnotation.PROGRESS_PCT_INITIALIZING,
                    false,
                    out var dataPackageInfo,
                    out var dataPackageDatasets);

                if (datasetCopyResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (!string.IsNullOrWhiteSpace(datasetFileRetriever.ErrorMessage))
                    {
                        mMessage = datasetFileRetriever.ErrorMessage;
                    }

                    return datasetCopyResult;
                }

                // Store information about the datasets in several packed job parameters
                dataPackageInfo.StorePackedDictionaries(this);

                if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in GetResources: " + ex.Message;
                LogErrorNoMessageUpdate(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType RetrieveReferenceMassFile(string paramFileStoragePath, string paramFileName)
        {
            try
            {
                var paramFile = new FileInfo(Path.Combine(paramFileStoragePath, paramFileName));

                var paramFileReader = new KeyValueParamFileReader("NOM Annotation", paramFile.FullName);
                RegisterEvents(paramFileReader);

                var paramFileSuccess = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries);

                if (!paramFileSuccess)
                {
                    LogError(
                        string.Format("Error reading NOM Annotation parameter file in RetrieveReferenceMassFile: {0}", paramFileReader.ErrorMessage),
                        paramFileReader.ErrorMessage);

                    return paramFileReader.ParamFileNotFound ? CloseOutType.CLOSEOUT_NO_PARAM_FILE : CloseOutType.CLOSEOUT_FAILED;
                }

                var referenceMassFileName = string.Empty;

                foreach (var entry in paramFileEntries)
                {
                    if (entry.Key.Equals("ReferenceMassFile", StringComparison.OrdinalIgnoreCase))
                    {
                        referenceMassFileName = entry.Value.Trim();
                    }
                }

                if (string.IsNullOrWhiteSpace(referenceMassFileName))
                {
                    LogMessage("Parameter file {0} does not have a reference mass file defined; will only compute NOM metrics", paramFile.Name);
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var refMassFilesDirectory = new DirectoryInfo(Path.Combine(paramFileStoragePath, "ReferenceMassFiles"));

                if (!refMassFilesDirectory.Exists)
                {
                    LogError("Calibration files directory not found: " + refMassFilesDirectory.FullName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var referenceMassFile = new FileInfo(Path.Combine(refMassFilesDirectory.FullName, referenceMassFileName));

                if (!referenceMassFile.Exists)
                {
                    LogError("Reference mass file not found: " + referenceMassFile.FullName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var localRefMassFilePath = Path.Combine(mWorkDir, referenceMassFile.Name);
                referenceMassFile.CopyTo(localRefMassFilePath, true);

                mJobParams.AddResultFileToSkip(referenceMassFile.Name);

                mJobParams.AddAdditionalParameter(
                    AnalysisJob.STEP_PARAMETERS_SECTION,
                    JOB_PARAM_REFERENCE_MASS_FILE,
                    referenceMassFile.Name);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveReferenceMassFile: " + ex.Message;
                LogErrorNoMessageUpdate(mMessage + "; " + Global.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
