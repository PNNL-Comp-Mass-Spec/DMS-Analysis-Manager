using System;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMsXmlGenPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSXMLGen plugin
    /// </summary>
    public class AnalysisResourcesMSXMLGen : AnalysisResources
    {
        /// <summary>
        /// Retrieves files necessary for creating the .mzML or .mzXML file
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

                currentTask = "Examine processing options";
                var msXmlGenerator = mJobParams.GetParam("MSXMLGenerator"); // ReAdW.exe or MSConvert.exe
                var msXmlFormat = mJobParams.GetParam("MSXMLOutputType");   // Typically mzXML or mzML

                if (string.IsNullOrWhiteSpace(msXmlGenerator) || string.IsNullOrWhiteSpace(msXmlFormat))
                {
                    LogError("Job parameters are invalid: MSXMLGenerator and MSXMLOutputType must be defined for this step tool");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // The ToolName job parameter holds the name of the job script we are executing
                var scriptName = mJobParams.GetParam("ToolName");
                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                if (Global.IsMatch(scriptName, "MaxQuant_DataPkg"))
                {
                    var createMzML = mJobParams.GetJobParameter("CreateMzMLFiles", false);

                    if (dataPackageID > 0 && !createMzML)
                    {
                        EvalMessage = string.Format("Skipping MSXMLGen since script is {0} and job parameter CreateMzMLFiles is false", scriptName);
                        LogMessage(EvalMessage);
                        return CloseOutType.CLOSEOUT_SKIPPED_MSXML_GEN;
                    }
                }

                if (Global.IsMatch(msXmlGenerator, "skip"))
                {
                    EvalMessage = "Skipping MSXMLGen since job parameter MSXMLGenerator is 'skip'";
                    LogMessage(EvalMessage);
                    return CloseOutType.CLOSEOUT_SKIPPED_MSXML_GEN;
                }

                var proMexBruker = scriptName.StartsWith("ProMex_Bruker", StringComparison.OrdinalIgnoreCase);

                if (proMexBruker)
                {
                    // Make sure the settings file has MSXMLOutputType=mzML, not mzXML

                    if (string.IsNullOrWhiteSpace(msXmlFormat))
                    {
                        LogError("Job parameter MSXMLOutputType must be defined in the settings file");
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }

                    if (msXmlFormat.IndexOf(MSXmlGen.MZML_FILE_FORMAT, StringComparison.OrdinalIgnoreCase) < 0)
                    {
                        LogError("ProMex_Bruker jobs require mzML files, not " + msXmlFormat + " files");
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }
                }

                const bool retrieveMsXmlFiles = false;

                var datasetFileRetriever = new DatasetFileRetriever(this);
                RegisterEvents(datasetFileRetriever);

                // If processing datasets in a data package, if an existing .mzML (or .mzXML) file is found,
                // do not retrieve the .raw file (or .d directory) and thus do not re-create the .mzML or .mzXML file
                var skipDatasetsWithExistingMzML = dataPackageID > 0;

                var datasetCopyResult = datasetFileRetriever.RetrieveInstrumentFilesForJobDatasets(
                    dataPackageID,
                    retrieveMsXmlFiles,
                    1,
                    skipDatasetsWithExistingMzML,
                    out var dataPackageInfo,
                    out _);

                if (datasetCopyResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (!string.IsNullOrWhiteSpace(datasetFileRetriever.ErrorMessage))
                    {
                        mMessage = datasetFileRetriever.ErrorMessage;
                    }

                    return datasetCopyResult;
                }

                if (skipDatasetsWithExistingMzML && dataPackageInfo.DatasetFiles.Count == 0)
                {
                    var msXMLOutputType = mJobParams.GetJobParameter("MSXMLOutputType", "mzML");

                    // Example message: Skipping MSXMLGen since all 12 datasets in data package 4117 already have a mzML file

                    EvalMessage = string.Format(
                        "Skipping MSXMLGen since all {0} datasets in data package {1} already have a {2} file",
                        dataPackageInfo.Datasets.Count,
                        dataPackageID,
                        msXMLOutputType);

                    LogMessage(EvalMessage);
                    return CloseOutType.CLOSEOUT_SKIPPED_MSXML_GEN;
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
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
