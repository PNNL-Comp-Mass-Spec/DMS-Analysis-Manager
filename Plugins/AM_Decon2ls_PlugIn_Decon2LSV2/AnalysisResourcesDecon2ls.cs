using System;
using System.IO;
using System.Linq;
using System.Xml;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using ThermoRawFileReader;
using UIMFLibrary;

namespace AnalysisManagerDecon2lsV2PlugIn
{
    /// <summary>
    /// Retrieve resources for the Decon2LS plugin
    /// </summary>
    public class AnalysisResourcesDecon2ls : AnalysisResources
    {
        // Ignore Spelling: Decon, Formularity, msms, mzml, mzxml

        /// <summary>
        /// Job parameter to track that MS/MS processing has been auto-enabled
        /// </summary>
        public const string JOB_PARAM_PROCESS_MSMS_AUTO_ENABLED = "DeconTools_ProcessMsMs_Auto_Enabled";

        /// <summary>
        /// Job parameter to track the DeconTools parameter file name when DeconTools is not the primary tool for the pipeline script
        /// </summary>
        public const string JOB_PARAM_DECON_TOOLS_PARAMETER_FILE_NAME = "DeconTools_ParameterFileName";

        /// <summary>
        /// Retrieves files necessary for performance of Decon2LS analysis
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var rawDataTypeName = mJobParams.GetParam("RawDataType");

            var msXmlOutputType = mJobParams.GetParam("MSXMLOutputType");

            if (!string.IsNullOrWhiteSpace(msXmlOutputType))
            {
                CloseOutType eResult;

                switch (msXmlOutputType.ToLower())
                {
                    case "mzxml":
                        eResult = GetMzXMLFile();
                        break;
                    case "mzml":
                        eResult = GetMzMLFile();
                        break;
                    default:
                        mMessage = "Unsupported value for MSXMLOutputType: " + msXmlOutputType;
                        eResult = CloseOutType.CLOSEOUT_FAILED;
                        break;
                }

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return eResult;
                }
            }
            else
            {
                // Get input data file
                if (!FileSearchTool.RetrieveSpectra(rawDataTypeName))
                {
                    LogError("AnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            mJobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(DOT_WIFF_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION);

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = mJobParams.GetParam("ToolName");

            string paramFileNameOverride;

            if (scriptName.StartsWith("Formularity", StringComparison.OrdinalIgnoreCase))
            {
                paramFileNameOverride = mJobParams.GetParam("DeconToolsParameterFile");

                if (!string.IsNullOrWhiteSpace(paramFileNameOverride))
                {
                    mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION,
                                                      JOB_PARAM_DECON_TOOLS_PARAMETER_FILE_NAME,
                                                      paramFileNameOverride);
                }
            }
            else
            {
                paramFileNameOverride = "";
            }

            string paramFileName;

            if (string.IsNullOrWhiteSpace(paramFileNameOverride))
            {
                paramFileName = mJobParams.GetParam("ParamFileName");
            }
            else
            {
                paramFileName = paramFileNameOverride;
            }

            // Prior to July 2022, ParmFileStoragePath had the parameter file storage path for the primary step tool of the job script
            // That field was renamed in July 22 to ParamFileStoragePath

            // When the analysis manager retrieves the parameters for the current job step, the database customizes
            // the value of ParamFileStoragePath to be the storage path for the current step tool
            var paramFileStoragePath = mJobParams.GetParam("ParamFileStoragePath");

            if (!FileSearchTool.RetrieveFile(paramFileName, paramFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ValidateDeconProcessingOptions(paramFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Read the setting for ProcessMSMS from the DeconTools parameter file
        /// </summary>
        /// <param name="paramFile"></param>
        /// <param name="processMSMS">Output: true if ProcessMSMS is true in the parameter file</param>
        /// <returns>True if success, false if an error</returns>
        private bool IsMSMSProcessingEnabled(FileSystemInfo paramFile, out bool processMSMS)
        {
            processMSMS = false;

            try
            {
                using var paramFileReader = new FileStream(paramFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read);

                // Open the file and parse the XML
                var paramFileXml = new XmlDocument();
                paramFileXml.Load(paramFileReader);

                // Look for the XML: <ProcessMSMS></ProcessMSMS>
                var node = paramFileXml.SelectSingleNode("//parameters/Miscellaneous/ProcessMSMS");

                // ReSharper disable once MergeIntoNegatedPattern
                if (node == null || !node.HasChildNodes)
                {
                    return true;
                }

                // Match found; read the value
                if (bool.TryParse(node.ChildNodes[0].Value, out processMSMS))
                {
                    return true;
                }

                // Parameter file formatting error
                LogError("Invalid entry for ProcessMSMS in the parameter file; should be true or false");
                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in IsMSMSProcessingEnabled", ex);
            }

            return false;
        }

        private bool ValidateDeconProcessingOptions(string paramFileName)
        {
            var paramFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));

            if (!paramFile.Exists)
            {
                // Parameter file not found
                const string errMsg = "Decon2LS param file not found by ValidateDeconProcessingOptions";
                LogError(errMsg, errMsg + ": " + paramFileName);

                return false;
            }

            if (!IsMSMSProcessingEnabled(paramFile, out var processMSMS))
            {
                return false;
            }

            if (processMSMS)
            {
                // No need to perform any further validation
                return true;
            }

            // Open the instrument data file and determine whether it only contains MS/MS spectra
            // If that is the case, update the parameter file to have ProcessMSMS=True

            if (!ExamineDatasetScanTypes(out var countMS1, out var countMSn))
            {
                return false;
            }

            if (countMS1 == 0 && countMSn > 0)
            {
                if (!EnableMSMSProcessingInParamFile(paramFile))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Determine the number of MS1 and MS2 (or higher) spectra in a dataset
        /// </summary>
        /// <remarks>At present only supports Thermo .Raw files and .UIMF files</remarks>
        /// <param name="countMs1"></param>
        /// <param name="countMSn"></param>
        private bool ExamineDatasetScanTypes(out int countMs1, out int countMSn)
        {
            countMs1 = 0;
            countMSn = 0;

            try
            {
                var rawDataTypeName = GetRawDataTypeName();

                if (string.IsNullOrWhiteSpace(rawDataTypeName))
                {
                    return false;
                }

                // Gets the Decon2LS file type based on the input data type
                var rawDataType = GetRawDataType(rawDataTypeName);

                var datasetFilePath = AnalysisToolRunnerDecon2ls.GetInputFilePath(mWorkDir, DatasetName, rawDataType);

                switch (rawDataType)
                {
                    case RawDataTypeConstants.ThermoRawFile:
                        LogMessage("Examining the scan types in the .raw file");
                        return ExamineScanTypesInRawFile(datasetFilePath, out countMs1, out countMSn);

                    case RawDataTypeConstants.UIMF:
                        LogMessage("Examining the scan types in the .UIMF file");
                        return ExamineScanTypesInUIMFFile(datasetFilePath, out countMs1, out countMSn);

                    default:
                        // Ignore datasets that are not .raw file or .uimf files
                        return true;
                }
            }
            catch (Exception ex)
            {
                LogError("Error in ExamineDatasetScanTypes", ex);
            }

            return false;
        }

        private bool ExamineScanTypesInRawFile(string datasetFilePath, out int countMs1, out int countMSn)
        {
            countMs1 = 0;
            countMSn = 0;

            try
            {
                using var rawFileReader = new XRawFileIO();
                RegisterEvents(rawFileReader);

                if (!rawFileReader.OpenRawFile(datasetFilePath))
                {
                    LogError("Error opening Thermo raw file " + Path.GetFileName(datasetFilePath));
                    return false;
                }

                var lastProgress = DateTime.UtcNow;
                var lastProgressLog = DateTime.UtcNow;
                var scanCount = rawFileReader.FileInfo.ScanEnd - rawFileReader.FileInfo.ScanStart + 1;

                for (var scanNumber = rawFileReader.FileInfo.ScanStart; scanNumber <= rawFileReader.FileInfo.ScanEnd; scanNumber++)
                {
                    var msLevel = rawFileReader.GetMSLevel(scanNumber);

                    if (msLevel == 1)
                    {
                        countMs1++;
                    }
                    else if (msLevel > 1)
                    {
                        countMSn++;
                    }

                    var logMessage = string.Format("Examining scan levels in .raw file, scan {0} / {1}", scanNumber, scanCount);

                    if (DateTime.UtcNow.Subtract(lastProgress).TotalSeconds >= 5)
                    {
                        // Show progress at the console, but do not write to the log file
                        LogDebug(logMessage, 10);
                        lastProgress = DateTime.UtcNow;
                    }

                    if (DateTime.UtcNow.Subtract(lastProgressLog).TotalSeconds >= 60)
                    {
                        lastProgressLog = DateTime.UtcNow;
                        LogDebug(logMessage, 1);

                        // Note: do not multiply by 100 since we want to call UpdateAndWrite with a number between 0 and 1 (meaning 0 to 1%)
                        var percentComplete = scanNumber / (float)scanCount;
                        mStatusTools.UpdateAndWrite(percentComplete);
                    }
                }

                rawFileReader.CloseRawFile();

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ExamineScanTypesInRawFile", ex);
                return false;
            }
        }

        private bool ExamineScanTypesInUIMFFile(string datasetFilePath, out int countMs1, out int countMSn)
        {
            countMs1 = 0;
            countMSn = 0;

            try
            {
                // ReSharper disable once RedundantNameQualifier
                using var reader = new UIMFLibrary.DataReader(datasetFilePath);

                var frameList = reader.GetMasterFrameList();

                var query = from item in frameList where item.Value == UIMFData.FrameType.MS1 select item;
                countMs1 = query.Count();

                countMSn = frameList.Count - countMs1;

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ExamineScanTypesInUIMFFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Update the parameter file to have ProcessMSMS set to true
        /// </summary>
        /// <param name="paramFile"></param>
        private bool EnableMSMSProcessingInParamFile(FileInfo paramFile)
        {
            try
            {
                var deconParamFilePath = paramFile.FullName;

                // Rename the existing parameter file
                var newParamFilePath = Path.Combine(mWorkDir, paramFile.Name + ".old");
                mJobParams.AddResultFileToSkip(newParamFilePath);

                paramFile.MoveTo(newParamFilePath);

                // Open the file and parse the XML
                var updatedXmlDoc = new XmlDocument();
                updatedXmlDoc.Load(new FileStream(paramFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));

                // Look for the XML: <ProcessMSMS></ProcessMSMS> in the Miscellaneous section
                // Set its value to "True" (the setting is added if missing)
                WriteTempParamFileUpdateElementValue(updatedXmlDoc, "//parameters/Miscellaneous", "ProcessMSMS", "True");

                try
                {
                    // Now write out the XML to paramFileTemp
                    using var updatedParamFileWriter = new StreamWriter(new FileStream(deconParamFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                    var formattedXmlWriter = new XmlTextWriter(updatedParamFileWriter)
                    {
                        Indentation = 1,
                        IndentChar = '\t',
                        Formatting = Formatting.Indented
                    };

                    updatedXmlDoc.WriteContentTo(formattedXmlWriter);

                    mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_PROCESS_MSMS_AUTO_ENABLED, true);
                    return true;
                }
                catch (Exception ex)
                {
                    LogError("Error writing new param file in EnableMSMSProcessingInParamFile", ex);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("Error reading existing param file in EnableMSMSProcessingInParamFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Looks for the section specified by parameter XPathForSection.  If found, updates its value to NewElementValue.  If not found, tries to add a new node with name ElementName
        /// </summary>
        /// <param name="xmlDoc">XML Document object</param>
        /// <param name="xpathForSection">XPath specifying the section that contains the desired element.  For example: "//parameters/Miscellaneous"</param>
        /// <param name="elementName">Element name to find (or add)</param>
        /// <param name="newElementValue">New value for this element</param>
        private void WriteTempParamFileUpdateElementValue(XmlDocument xmlDoc, string xpathForSection, string elementName, string newElementValue)
        {
            var node = xmlDoc.SelectSingleNode(xpathForSection + "/" + elementName);

            if (node != null)
            {
                if (node.HasChildNodes)
                {
                    // Match found; update the value
                    node.ChildNodes[0].Value = newElementValue;
                }
            }
            else
            {
                node = xmlDoc.SelectSingleNode(xpathForSection);

                if (node != null)
                {
                    var newChild = (XmlElement)xmlDoc.CreateNode(XmlNodeType.Element, elementName, string.Empty);
                    newChild.InnerXml = newElementValue;

                    node.AppendChild(newChild);
                }
            }
        }
    }
}
