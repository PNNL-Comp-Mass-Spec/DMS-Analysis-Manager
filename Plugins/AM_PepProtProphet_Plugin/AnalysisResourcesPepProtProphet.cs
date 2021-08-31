//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/19/2019
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerPepProtProphetPlugIn
{
    /// <summary>
    /// Retrieve resources for the PepProtProphet plugin
    /// </summary>
    /// <remarks>
    /// This plugin is used to post-process MSFragger results
    /// </remarks>
    public class AnalysisResourcesPepProtProphet : AnalysisResources
    {
        // Ignore Spelling: resourcer

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                currentTask = "Get DataPackageID";

                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                var paramFilePath = Path.Combine(mWorkDir, paramFileName);

                var datasetCount = GetDatasetCount();

                var optionsLoaded = LoadMSFraggerOptions(paramFilePath, datasetCount, out var options);
                if (!optionsLoaded)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (options.OpenSearch)
                {
                    // Make sure the machine has enough free memory to run Crystal-C
                    currentTask = "Validate free memory for Crystal-C";

                    if (!ValidateFreeMemorySizeGB("Crystal-C", AnalysisToolRunnerPepProtProphet.CRYSTALC_MEMORY_SIZE_GB))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                if (options.RunIonQuant)
                {
                    // Make sure the machine has enough free memory to run IonQuant

                    currentTask = "Validate free memory for IonQuant";

                    if (!ValidateFreeMemorySizeGB("IonQuant", AnalysisToolRunnerPepProtProphet.ION_QUANT_MEMORY_SIZE_GB))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                if (options.ReporterIonMode != ReporterIonModes.Disabled && options.DatasetCount > 1)
                {
                    currentTask = "Validate free memory for TMT-Integrator";

                    if (!ValidateFreeMemorySizeGB("TMT-Integrator", AnalysisToolRunnerPepProtProphet.TMT_INTEGRATOR_MEMORY_SIZE_GB))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Require that the input files be mzML files (since PeptideProphet prefers them and TmtIntegrator requires them)
                // In contrast, MaxQuant can work with either .raw files or .mzML files
                const bool retrieveMzML = true;

                currentTask = "RetrieveParamFile " + paramFileName;

                // Retrieve param file
                if (!FileSearchTool.RetrieveFile(paramFileName, mJobParams.GetParam("ParmFileStoragePath")))
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

                // Retrieve FASTA file
                var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;
                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                    return resultCode;

                var datasetFileRetriever = new DatasetFileRetriever(this);
                RegisterEvents(datasetFileRetriever);

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                var datasetCopyResult = datasetFileRetriever.RetrieveInstrumentFilesForJobDatasets(
                    dataPackageID,
                    retrieveMzML,
                    AnalysisToolRunnerPepProtProphet.PROGRESS_PCT_INITIALIZING,
                    false,
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

                if (dataPackageID > 0)
                {
                    // Look for a file named AliasNames.txt in the data package directory
                    // If found, copy it locally
                    // If not found, it will be auto-generated during the LabelQuant step

                    // ToDo: base.Retrieve
                }

                currentTask = "GetPepXMLFiles";

                var pepXmlResultCode = GetPepXMLFiles(dataPackageInfo);

                if (pepXmlResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return pepXmlResultCode;
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
                LogError("Exception in GetResources (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private int GetDatasetCount()
        {
            var dataPackageDefined = LoadDataPackageDatasetInfo(out var dataPackageDatasets);

            return dataPackageDefined ? dataPackageDatasets.Count : 1;
        }

        /// <summary>
        /// Retrieve the zipped pepXML files
        /// </summary>
        /// <remarks>
        /// Each _pepXML.zip file will also have a .pin file
        /// </remarks>
        /// <param name="dataPackageInfo"></param>
        private CloseOutType GetPepXMLFiles(DataPackageInfo dataPackageInfo)
        {
            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = mJobParams.GetParam("ToolName");

            if (!scriptName.StartsWith("MSFragger", StringComparison.OrdinalIgnoreCase))
            {
                LogError("The PepProtProphet step tool is not compatible with pipeline script " + scriptName);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            foreach (var item in dataPackageInfo.Datasets)
            {
                var datasetName = item.Value;

                var fileToRetrieve = datasetName + "_pepXML.zip";
                const bool unzipRequired = true;

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToRetrieve, unzipRequired))
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Assure that the first column of the .pin file includes the dataset name, scan number, and charge
                // To reduce file size, AnalysisToolRunnerPepProtProphet removes this information prior to creating the updated _pepXML.zip file
                var pinFile = new FileInfo(Path.Combine(mWorkDir, datasetName + AnalysisToolRunnerPepProtProphet.PIN_EXTENSION));

                if (!ValidatePINFile(datasetName, pinFile))
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }
            }

            mJobParams.AddResultFileExtensionToSkip(AnalysisToolRunnerPepProtProphet.PEPXML_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(AnalysisToolRunnerPepProtProphet.PIN_EXTENSION);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool ValidatePINFile(string datasetName, FileInfo sourcePinFile)
        {
            const string TRASH_EXTENSION = ".trash1";

            mJobParams.AddResultFileExtensionToSkip(TRASH_EXTENSION);

            try
            {
                if (!sourcePinFile.Exists)
                {
                    LogError("File not found: " + sourcePinFile.FullName);
                    return false;
                }

                var updatedPinFile = new FileInfo(sourcePinFile.FullName + ".updated");

                var scanNumberIndex = -1;

                // Keys in this dictionary are column index values, Values are charge state
                var columnIndexToChargeMap = new Dictionary<int, int>();

                var headerNames = new List<string>
                {
                    "SpecId",
                    "Label",
                    "ScanNr",
                    "ExpMass",
                    "retentiontime",
                    "rank",
                    "mass_diff_score",
                    "log10_evalue",
                    "hyperscore",
                    "delta_hyperscore",
                    "matched_ion_num",
                    "peptide_length",
                    "ntt",
                    "nmc",
                    "charge_1",
                    "charge_2",
                    "charge_3",
                    "charge_4",
                    "charge_5",
                    "charge_6",
                    "charge_7",
                    "Peptide",
                    "Proteins"
                };

                var fileUpdated = false;

                using (var reader = new StreamReader(new FileStream(sourcePinFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(updatedPinFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        if (scanNumberIndex < 0)
                        {
                            // This is the header line
                            writer.WriteLine(dataLine);

                            var columnMap = Global.ParseHeaderLine(dataLine, headerNames);

                            scanNumberIndex = columnMap["ScanNr"];
                            if (scanNumberIndex < 0)
                            {
                                LogError(string.Format("{0} column not found in {1}", "columnMap", sourcePinFile.FullName));
                                return false;
                            }

                            for (var chargeState = 1; chargeState <= 7; chargeState++)
                            {
                                var chargeColumn = string.Format("charge_{0}", chargeState);

                                var columnIndex = columnMap[chargeColumn];
                                if (columnIndex >= 0)
                                {
                                    columnIndexToChargeMap.Add(columnIndex, chargeState);
                                }
                            }

                            continue;
                        }

                        var lineParts = dataLine.Split('\t');
                        if (lineParts.Length > 1 && string.IsNullOrWhiteSpace(lineParts[0]))
                        {
                            var scanNumber = lineParts[scanNumberIndex];

                            // Examine the charge columns to find the first column with a 1
                            var chargeState = 0;

                            foreach (var item in columnIndexToChargeMap)
                            {
                                var chargeFlag = lineParts[item.Key];
                                if (!int.TryParse(chargeFlag, out var value) || value <= 0)
                                {
                                    continue;
                                }

                                chargeState = item.Value;
                                break;
                            }

                            lineParts[0] = string.Format("{0}.{1}.{1}.{2}_1", datasetName, scanNumber, chargeState);

                            fileUpdated = true;
                        }

                        writer.WriteLine(string.Join("\t", lineParts));
                    }
                }

                if (!fileUpdated)
                    return true;

                var finalPath = sourcePinFile.FullName;
                sourcePinFile.MoveTo(sourcePinFile.FullName + TRASH_EXTENSION);

                updatedPinFile.MoveTo(finalPath);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ValidatePINFile for " + sourcePinFile.Name, ex);
                return false;
            }
        }

        /// <summary>
        /// Parse the MSFragger parameter file to determine certain processing options
        /// </summary>
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <param name="paramFilePath"></param>
        /// <param name="datasetCount"></param>
        /// <param name="options">Output: instance of the MSFragger options class</param>
        /// <returns>True if success, false if an error</returns>
        private bool LoadMSFraggerOptions(string paramFilePath, int datasetCount, out FragPipeOptions options)
        {
            options = new FragPipeOptions(mJobParams, null, datasetCount);
            RegisterEvents(options);

            try
            {
                options.LoadMSFraggerOptions(paramFilePath);
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in LoadMSFraggerOptions", ex);
                return false;
            }
        }

        private bool ValidateFreeMemorySizeGB(string programName, int memoryRequiredGB)
        {
            if (ValidateFreeMemorySize(memoryRequiredGB * 1024, StepToolName, false))
            {
                return true;
            }

            mMessage = "Not enough free memory to run " + programName;
            return false;
        }
    }
}
