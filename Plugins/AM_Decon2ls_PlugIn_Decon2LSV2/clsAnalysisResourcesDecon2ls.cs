using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml;
using AnalysisManagerBase;
using ThermoRawFileReader;
using UIMFLibrary;

namespace AnalysisManagerDecon2lsV2PlugIn
{
    public class clsAnalysisResourcesDecon2ls : clsAnalysisResources
    {
        public const string JOB_PARAM_PROCESSMSMS_AUTO_ENABLED = "DeconTools_ProcessMsMs_Auto_Enabled";

        #region "Methods"

        /// <summary>
        /// Retrieves files necessary for performance of Decon2ls analysis
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var strRawDataType = m_jobParams.GetParam("RawDataType");

            var msXmlOutputType = m_jobParams.GetParam("MSXMLOutputType");

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
                        m_message = "Unsupported value for MSXMLOutputType: " + msXmlOutputType;
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
                if (!FileSearch.RetrieveSpectra(strRawDataType))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            m_jobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(DOT_WIFF_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION);

            if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the parameter file
            var paramFileName = m_jobParams.GetParam("ParmFileName");
            var paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");

            if (!FileSearch.RetrieveFile(paramFileName, paramFileStoragePath))
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
        /// <param name="fiParamFile"></param>
        /// <param name="processMSMS">Output parameter: true if ProcessMSMS is True in the parameter file</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool IsMSMSProcessingEnabled(FileInfo fiParamFile, out bool processMSMS)
        {
            processMSMS = false;

            try
            {
                using (var srParamFile = new FileStream(fiParamFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    // Open the file and parse the XML
                    var objParamFile = new XmlDocument();
                    objParamFile.Load(srParamFile);

                    // Look for the XML: <ProcessMSMS></ProcessMSMS>
                    var objNode = objParamFile.SelectSingleNode("//parameters/Miscellaneous/ProcessMSMS");

                    if ((objNode != null) && objNode.HasChildNodes)
                    {
                        // Match found; read the value
                        if (!bool.TryParse(objNode.ChildNodes[0].Value, out processMSMS))
                        {
                            // Parameter file formatting error
                            LogError("Invalid entry for ProcessMSMS in the parameter file; should be True or False");
                            return false;
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in IsMSMSProcessingEnabled", ex);
            }

            return false;
        }

        private bool ValidateDeconProcessingOptions(string paramFileName)
        {
            var fiParamFile = new FileInfo(Path.Combine(m_WorkingDir, paramFileName));

            if (!fiParamFile.Exists)
            {
                // Parameter file not found
                var errMsg = "Decon2LS param file not found by ValidateDeconProcessingOptions";
                LogError(errMsg, errMsg + ": " + paramFileName);

                return false;
            }

            bool processMSMS;
            if (!IsMSMSProcessingEnabled(fiParamFile, out processMSMS))
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

            int countMS1;
            int countMSn;

            if (!ExamineDatasetScanTypes(out countMS1, out countMSn))
            {
                return false;
            }

            if (countMS1 == 0 & countMSn > 0)
            {
                if (!EnableMSMSProcessingInParamFile(fiParamFile))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Determine the number of MS1 and MS2 (or higher) spectra in a dataset
        /// </summary>
        /// <param name="countMs1"></param>
        /// <param name="countMSn"></param>
        /// <returns></returns>
        /// <remarks>At present only supports Thermo .Raw files and .UIMF files</remarks>
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
                var eRawDataType = GetRawDataType(rawDataTypeName);

                var datasetFilePath = clsAnalysisToolRunnerDecon2ls.GetInputFilePath(m_WorkingDir, DatasetName, eRawDataType);
                bool success;

                switch (eRawDataType)
                {
                    case eRawDataTypeConstants.ThermoRawFile:
                        success = ExamineScanTypesInRawFile(datasetFilePath, out countMs1, out countMSn);

                        break;
                    case eRawDataTypeConstants.UIMF:
                        //
                        success = ExamineScanTypesInUIMFFile(datasetFilePath, out countMs1, out countMSn);

                        break;
                    default:
                        // Ignore datasets that are not .raw file or .uimf files
                        success = true;
                        break;
                }

                return success;
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
                using (var rawFileReader = new XRawFileIO())
                {
                    if (!rawFileReader.OpenRawFile(datasetFilePath))
                    {
                        LogError("Error opening Thermo raw file " + Path.GetFileName(datasetFilePath));
                        return false;
                    }

                    for (var scanNumber = rawFileReader.FileInfo.ScanStart; scanNumber <= rawFileReader.FileInfo.ScanEnd; scanNumber++)
                    {
                        clsScanInfo scanInfo;
                        if (rawFileReader.GetScanInfo(scanNumber, out scanInfo))
                        {
                            if (scanInfo.MSLevel == 1)
                            {
                                countMs1 += 1;
                            }
                            else
                            {
                                countMSn += 1;
                            }
                        }
                    }

                    rawFileReader.CloseRawFile();
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ExamineScansTypesInRawFile", ex);
                return false;
            }
        }

        private bool ExamineScanTypesInUIMFFile(string datasetFilePath, out int countMs1, out int countMSn)
        {
            countMs1 = 0;
            countMSn = 0;

            try
            {
                using (var reader = new UIMFLibrary.DataReader(datasetFilePath))
                {
                    var frameList = reader.GetMasterFrameList();

                    var query = from item in frameList where item.Value == DataReader.FrameType.MS1 select item;
                    countMs1 = query.Count();

                    countMSn = frameList.Count - countMs1;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ExamineScansTypesInRawFile", ex);
                return false;
            }
        }

        /// <summary>
        ///Update the parameter file to have ProcessMSMS set to True
        /// </summary>
        /// <param name="fiParamFile"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool EnableMSMSProcessingInParamFile(FileInfo fiParamFile)
        {
            try
            {
                var deconParamFilePath = string.Copy(fiParamFile.FullName);

                // Rename the existing parameter file
                var newParamFilePath = Path.Combine(m_WorkingDir, fiParamFile.Name + ".old");
                m_jobParams.AddResultFileToSkip(newParamFilePath);

                fiParamFile.MoveTo(newParamFilePath);
                Thread.Sleep(250);

                // Open the file and parse the XML
                var updatedXmlDoc = new XmlDocument();
                updatedXmlDoc.Load(new FileStream(fiParamFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));

                // Look for the XML: <ProcessMSMS></ProcessMSMS> in the Miscellaneous section
                // Set its value to "True" (the setting is added if missing)
                WriteTempParamFileUpdateElementValue(updatedXmlDoc, "//parameters/Miscellaneous", "ProcessMSMS", "True");

                try
                {
                    // Now write out the XML to strParamFileTemp
                    using (var updatedParamFileWriter = new StreamWriter(new FileStream(deconParamFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        var formattedXmlWriter = new XmlTextWriter(updatedParamFileWriter)
                        {
                            Indentation = 1,
                            IndentChar = '\t',
                            Formatting = Formatting.Indented
                        };

                        updatedXmlDoc.WriteContentTo(formattedXmlWriter);
                    }

                    m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAM_PROCESSMSMS_AUTO_ENABLED, true);
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
        /// <remarks></remarks>
        private void WriteTempParamFileUpdateElementValue(XmlDocument xmlDoc, string xpathForSection, string elementName, string newElementValue)
        {
            var objNode = xmlDoc.SelectSingleNode(xpathForSection + "/" + elementName);

            if ((objNode != null))
            {
                if (objNode.HasChildNodes)
                {
                    // Match found; update the value
                    objNode.ChildNodes[0].Value = newElementValue;
                }
            }
            else
            {
                objNode = xmlDoc.SelectSingleNode(xpathForSection);

                if ((objNode != null))
                {
                    var objNewChild = (XmlElement) xmlDoc.CreateNode(XmlNodeType.Element, elementName, string.Empty);
                    objNewChild.InnerXml = newElementValue;

                    objNode.AppendChild(objNewChild);
                }
            }
        }

        #endregion
    }
}
