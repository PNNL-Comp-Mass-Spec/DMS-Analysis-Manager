//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 02/06/2009
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerMsXmlGenPlugIn
{
    /// <summary>
    /// Class for running MS XML generator to generate mzXML or .mzML files
    /// </summary>
    public class clsAnalysisToolRunnerMSXMLGen : clsAnalysisToolRunnerBase
    {

        #region "Module Variables"

        private const float PROGRESS_PCT_MSXML_GEN_RUNNING = 5;

        private string mMSXmlGeneratorAppPath = string.Empty;

        private clsAnalysisResources.MSXMLOutputTypeConstants mMSXmlOutputFileType;

        private DirectoryInfo mMSXmlCacheFolder;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs ReAdW or MSConvert
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            var result = CloseOutType.CLOSEOUT_SUCCESS;

            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the ReAdW or MSConvert version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                LogError("Error determining MSXMLGen version");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var storeInCache = m_jobParams.GetJobParameter("StoreMSXmlInCache", true);
            if (storeInCache)
            {
                var msXMLCacheFolderPath = m_mgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
                mMSXmlCacheFolder = new DirectoryInfo(msXMLCacheFolderPath);

                if (!mMSXmlCacheFolder.Exists)
                {
                    LogError("MSXmlCache folder not found: " + msXMLCacheFolderPath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            if (CreateMSXMLFile() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            if (!PostProcessMSXmlFile())
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Stop the job timer
            m_StopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Generate the mzXML or mzML file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType CreateMSXMLFile()
        {
            try
            {
                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerMSXMLGen.CreateMSXMLFile(): Enter");
                }

                var msXmlGenerator = m_jobParams.GetParam("MSXMLGenerator");          // ReAdW.exe or MSConvert.exe
                var msXmlFormat = m_jobParams.GetParam("MSXMLOutputType");            // Typically mzXML or mzML

                // Determine the output type
                switch (msXmlFormat.ToLower())
                {
                    case "mzxml":
                        mMSXmlOutputFileType = clsAnalysisResources.MSXMLOutputTypeConstants.mzXML;
                        break;
                    case "mzml":
                        mMSXmlOutputFileType = clsAnalysisResources.MSXMLOutputTypeConstants.mzML;
                        break;
                    default:
                        LogWarning("msXmlFormat string is not mzXML or mzML (" + msXmlFormat + "); will default to mzXML");
                        mMSXmlOutputFileType = clsAnalysisResources.MSXMLOutputTypeConstants.mzXML;
                        break;
                }

                // Lookup Centroid Settings
                var centroidMSXML = m_jobParams.GetJobParameter("CentroidMSXML", false);
                var centroidMS1 = m_jobParams.GetJobParameter("CentroidMS1", false);
                var centroidMS2 = m_jobParams.GetJobParameter("CentroidMS2", false);

                if (centroidMSXML)
                {
                    centroidMS1 = true;
                    centroidMS2 = true;
                }

                // Look for parameter CentroidPeakCountToRetain in the MSXMLGenerator section
                // If the value is -1, will retain all data points
                var centroidPeakCountToRetain = m_jobParams.GetJobParameter("MSXMLGenerator", "CentroidPeakCountToRetain", 0);

                if (centroidPeakCountToRetain == 0)
                {
                    // Look for parameter CentroidPeakCountToRetain in any section
                    centroidPeakCountToRetain = m_jobParams.GetJobParameter("CentroidPeakCountToRetain",
                        clsMSXmlGenMSConvert.DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN);
                }

                // Look for custom processing arguments
                var customMSConvertArguments = m_jobParams.GetJobParameter("MSXMLGenerator", "CustomMSConvertArguments", "");

                if (string.IsNullOrEmpty(mMSXmlGeneratorAppPath))
                {
                    LogWarning("mMSXmlGeneratorAppPath is empty; this is unexpected");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var rawDataType = m_jobParams.GetParam("RawDataType");
                var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);

                clsMSXmlGen msXmlGen;

                // Determine the program path and Instantiate the processing class
                if (msXmlGenerator.ToLower().Contains("readw"))
                {
                    // ReAdW
                    // mMSXmlGeneratorAppPath should have been populated during the call to StoreToolVersionInfo()

                    msXmlGen = new clsMSXMLGenReadW(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType, mMSXmlOutputFileType,
                        centroidMS1 | centroidMS2);

                    if (rawDataType != clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES)
                    {
                        LogError("ReAdW can only be used with .Raw files, not with " + rawDataType);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else if (msXmlGenerator.ToLower().Contains("msconvert"))
                {
                    // MSConvert

                    if (string.IsNullOrWhiteSpace(customMSConvertArguments))
                    {
                        msXmlGen = new clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType, mMSXmlOutputFileType,
                            centroidMS1, centroidMS2, centroidPeakCountToRetain);
                    }
                    else
                    {
                        msXmlGen = new clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType, mMSXmlOutputFileType,
                            customMSConvertArguments);
                    }
                }
                else
                {
                    LogError("Unsupported XmlGenerator: " + msXmlGenerator);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Attach events to msXmlGen
                RegisterEvents(msXmlGen);

                msXmlGen.LoopWaiting += MSXmlGen_LoopWaiting;
                msXmlGen.ProgRunnerStarting += MSXmlGen_ProgRunnerStarting;

                msXmlGen.DebugLevel = m_DebugLevel;

                if (!File.Exists(mMSXmlGeneratorAppPath))
                {
                    LogError("MsXmlGenerator not found: " + mMSXmlGeneratorAppPath);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Create the file
                var success = msXmlGen.CreateMSXMLFile();

                if (!success)
                {
                    LogError(msXmlGen.ErrorMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (msXmlGen.ErrorMessage.Length > 0)
                {
                    LogError(msXmlGen.ErrorMessage);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMSXMLFile", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Get the path to the .Exe to use for recalculating precursor ion m/z and charge values
        /// </summary>
        /// <param name="recalculatePrecursorsTool"></param>
        /// <returns>Path to the exe, or an empty string if an error</returns>
        private string GetRecalculatePrecursorsToolProgLoc(out string recalculatePrecursorsTool)
        {
            recalculatePrecursorsTool = m_jobParams.GetJobParameter("RecalculatePrecursorsTool", string.Empty);
            if (string.IsNullOrWhiteSpace(recalculatePrecursorsTool))
            {
                LogError("Job parameter RecalculatePrecursorsTool is not defined in the settings file; cannot determine tool to use");
                return string.Empty;
            }

            if (string.Equals(recalculatePrecursorsTool, clsRawConverterRunner.RAWCONVERTER_FILENAME, StringComparison.InvariantCultureIgnoreCase))
            {
                var rawConverterDir = m_mgrParams.GetParam("RawConverterProgLoc");
                if (string.IsNullOrWhiteSpace(rawConverterDir))
                {
                    LogError("Manager parameter RawConverterProgLoc is not defined; cannot find the folder for " +
                             clsRawConverterRunner.RAWCONVERTER_FILENAME);
                    return string.Empty;
                }

                return Path.Combine(rawConverterDir, clsRawConverterRunner.RAWCONVERTER_FILENAME);
            }

            return string.Empty;
        }

        private bool PostProcessMSXmlFile()
        {
            try
            {
                string resultFileExtension;

                switch (mMSXmlOutputFileType)
                {
                    case clsAnalysisResources.MSXMLOutputTypeConstants.mzML:
                        resultFileExtension = clsAnalysisResources.DOT_MZML_EXTENSION;
                        break;
                    case clsAnalysisResources.MSXMLOutputTypeConstants.mzXML:
                        resultFileExtension = clsAnalysisResources.DOT_MZXML_EXTENSION;
                        break;
                    default:
                        throw new Exception("Unrecognized MSXMLOutputType value");
                }

                var msXmlFilePath = Path.Combine(m_WorkDir, m_Dataset + resultFileExtension);
                var fiMSXmlFile = new FileInfo(msXmlFilePath);

                if (!fiMSXmlFile.Exists)
                {
                    LogError(resultFileExtension + " file not found: " + Path.GetFileName(msXmlFilePath));
                    return false;
                }

                // Possibly update the file using results from RawConverter

                var recalculatePrecursors = m_jobParams.GetJobParameter("RecalculatePrecursors", false);
                if (recalculatePrecursors)
                {
                    var success = RecalculatePrecursorIons(fiMSXmlFile);
                    if (!success)
                    {
                        return false;
                    }
                }

                // Compress the file using GZip
                LogMessage("GZipping " + fiMSXmlFile.Name);

                // Note that if this process turns out to be slow, we can have MSConvert do this for us using --gzip
                // However, that will not work if RecalculatePrecursors is true
                fiMSXmlFile = GZipFile(fiMSXmlFile);
                if (fiMSXmlFile == null)
                {
                    return false;
                }

                var storeInDataset = m_jobParams.GetJobParameter("StoreMSXmlInDataset", false);
                var storeInCache = m_jobParams.GetJobParameter("StoreMSXmlInCache", true);

                if (!storeInDataset && !storeInCache)
                    storeInCache = true;

                if (!storeInDataset)
                {
                    // Do not move the .mzXML or .mzML file to the result folder
                    m_jobParams.AddResultFileExtensionToSkip(resultFileExtension);
                    m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_GZ_EXTENSION);
                }

                if (storeInCache)
                {
                    // Copy the .mzXML or .mzML file to the MSXML cache
                    var remoteCachefilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, fiMSXmlFile.FullName, purgeOldFilesIfNeeded: true);

                    if (string.IsNullOrEmpty(remoteCachefilePath))
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            LogError("CopyFileToServerCache returned false for " + fiMSXmlFile.Name);
                        }
                        return false;
                    }

                    // Create the _CacheInfo.txt file
                    var cacheInfoFilePath = msXmlFilePath + "_CacheInfo.txt";
                    using (var swOutFile = new StreamWriter(new FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        swOutFile.WriteLine(remoteCachefilePath);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in PostProcessMSXmlFile", ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Recalculate the precursor ions in a MzML file
        /// The only supported option at present is RawConverter
        /// </summary>
        /// <param name="sourceMsXmlFile">MzML file to read</param>
        /// <returns>True if success, false if an error</returns>
        private bool RecalculatePrecursorIons(FileInfo sourceMsXmlFile)
        {
            if (mMSXmlOutputFileType != clsAnalysisResources.MSXMLOutputTypeConstants.mzML)
            {
                LogError("Unsupported file extension for RecalculatePrecursors=True; must be mzML, not " + mMSXmlOutputFileType);
                return false;
            }

            var rawDataType = m_jobParams.GetParam("RawDataType");
            var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);
            string rawFilePath;

            if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
            {
                rawFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
            }
            else
            {
                LogError("Unsupported dataset type for RecalculatePrecursors=True; must be .Raw, not " + eRawDataType);
                return false;
            }

            string recalculatePrecursorsTool;
            var recalculatePrecursorsToolProgLoc = GetRecalculatePrecursorsToolProgLoc(out recalculatePrecursorsTool);
            if (string.IsNullOrWhiteSpace(recalculatePrecursorsToolProgLoc))
            {
                return false;
            }

            if (string.Equals(recalculatePrecursorsTool, clsRawConverterRunner.RAWCONVERTER_FILENAME, StringComparison.InvariantCultureIgnoreCase))
            {
                // Using RawConverter.exe
                FileInfo mgfFile;
                var rawConverterExe = new FileInfo(recalculatePrecursorsToolProgLoc);

                var rawConverterSuccess = RecalculatePrecursorIonsCreateMGF(rawConverterExe.Directory.FullName, rawFilePath, out mgfFile);
                if (!rawConverterSuccess)
                    return false;

                var mzMLUpdated = RecalculatePrecursorIonsUpdateMzML(sourceMsXmlFile, mgfFile);
                return mzMLUpdated;
            }

            LogError("Unsupported tool for precursursor recalculation: " + recalculatePrecursorsTool);
            return false;
        }

        /// <summary>
        /// Use RawConverter to process the Thermo .Raw file and recalculate the precursor ion information, writing the results to a .MGF file
        /// </summary>
        /// <param name="rawConverterDir"></param>
        /// <param name="rawFilePath"></param>
        /// <param name="mgfFile"></param>
        /// <returns></returns>
        private bool RecalculatePrecursorIonsCreateMGF(string rawConverterDir, string rawFilePath, out FileInfo mgfFile)
        {
            try
            {
                if (m_message == null)
                    m_message = string.Empty;
                var messageAtStart = string.Copy(m_message);

                var converter = new clsRawConverterRunner(rawConverterDir, m_DebugLevel);
                RegisterEvents(converter);

                var success = converter.ConvertRawToMGF(rawFilePath);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(m_message) || string.Equals(messageAtStart, m_message))
                    {
                        LogError("Unknown RawConverter error");
                    }
                    mgfFile = null;
                    return false;
                }

                // Confirm that RawConverter created a .mgf file

                mgfFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MGF_EXTENSION));
                if (!mgfFile.Exists)
                {
                    LogError("RawConverter did not create file " + mgfFile.Name);
                    return false;
                }

                m_jobParams.AddResultFileToSkip(mgfFile.Name);

                return true;
            }
            catch (Exception ex)
            {
                LogError("RawConverter error", ex);
                m_message = "Exception running RawConverter";
                mgfFile = null;
                return false;
            }
        }

        private bool RecalculatePrecursorIonsUpdateMzML(FileInfo sourceMsXmlFile, FileInfo mgfFile)
        {
            try
            {
                var messageAtStart = string.Copy(m_message);

                var updater = new clsParentIonUpdater();
                RegisterEvents(updater);

                var updatedMzMLPath = updater.UpdateMzMLParentIonInfoUsingMGF(sourceMsXmlFile.FullName, mgfFile.FullName, false);

                if (string.IsNullOrEmpty(updatedMzMLPath))
                {
                    if (string.IsNullOrWhiteSpace(m_message) || string.Equals(messageAtStart, m_message))
                    {
                        LogError("Unknown ParentIonUpdater error");
                    }
                    return false;
                }

                // Confirm that clsParentIonUpdater created a new .mzML file

                var updatedMzMLFile = new FileInfo(updatedMzMLPath);
                if (!updatedMzMLFile.Exists)
                {
                    LogError("ParentIonUpdater did not create file " + mgfFile.Name);
                    return false;
                }

                var finalMsXmlFilePath = string.Copy(sourceMsXmlFile.FullName);

                // Delete the original mzML file
                sourceMsXmlFile.Delete();

                // Rename the updated mzML file so that it does not end in _new.mzML
                updatedMzMLFile.MoveTo(finalMsXmlFilePath);

                // Re-index the mzML file using MSConvert

                var success = ReindexMzML(finalMsXmlFilePath);

                return success;
            }
            catch (Exception ex)
            {
                LogError("RecalculatePrecursorIonsUpdateMzML error", ex);
                m_message = "Exception in RecalculatePrecursorIonsUpdateMzML";
                return false;
            }
        }

        private bool ReindexMzML(string mzMLFilePath)
        {
            try
            {
                if (m_DebugLevel > 4)
                {
                    LogDebug("Re-index the mzML file using MSConvert");
                }

                var msXmlGenerator = m_jobParams.GetParam("MSXMLGenerator");
                // Must be MSConvert.exe

                if (!msXmlGenerator.ToLower().Contains("msconvert"))
                {
                    LogError("ParentIonUpdater only supports MSConvert, not " + msXmlGenerator);
                    return false;
                }

                if (string.IsNullOrEmpty(mMSXmlGeneratorAppPath))
                {
                    LogError("mMSXmlGeneratorAppPath is empty; this is unexpected");
                    return false;
                }

                var eRawDataType = clsAnalysisResources.eRawDataTypeConstants.mzML;
                var outputFileType = clsAnalysisResources.MSXMLOutputTypeConstants.mzML;

                var sourcefileBase = Path.GetFileNameWithoutExtension(mzMLFilePath);

                var msConvertRunner = new clsMSXmlGenMSConvert(
                    m_WorkDir, mMSXmlGeneratorAppPath,
                    sourcefileBase, eRawDataType, outputFileType,
                    centroidMS1: false, centroidMS2: false, centroidPeakCountToRetain: 0)
                {
                    ConsoleOutputSuffix = "_Reindex",
                    DebugLevel = m_DebugLevel
                };

                RegisterEvents(msConvertRunner);

                if (!File.Exists(mMSXmlGeneratorAppPath))
                {
                    LogError("MsXmlGenerator not found: " + mMSXmlGeneratorAppPath);
                    return false;
                }

                // Create the file
                var success = msConvertRunner.CreateMSXMLFile();

                if (!success)
                {
                    LogError(msConvertRunner.ErrorMessage);
                    return false;
                }

                m_jobParams.AddResultFileToSkip(msConvertRunner.ConsoleOutputFileName);

                if (msConvertRunner.ErrorMessage.Length > 0)
                {
                    LogError(msConvertRunner.ErrorMessage);
                }

                // Replace the original .mzML file with the new .mzML file
                var reindexedMzMLFile = new FileInfo(Path.Combine(m_WorkDir, msConvertRunner.OutputFileName));

                if (!reindexedMzMLFile.Exists)
                {
                    LogError("Reindexed mzML file not found at " + reindexedMzMLFile.FullName);
                    m_message = "Reindexed mzML file not found";
                    return false;
                }

                // Replace the original .mzML file with the indexed one
                File.Delete(mzMLFilePath);

                reindexedMzMLFile.MoveTo(mzMLFilePath);
                return true;
            }
            catch (Exception ex)
            {
                LogError("ReindexMzML error", ex);
                m_message = "Exception in ReindexMzML";
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo>();

            // Determine the path to the XML Generator
            // ReAdW.exe or MSConvert.exe
            var msXmlGenerator = m_jobParams.GetParam("MSXMLGenerator");

            mMSXmlGeneratorAppPath = string.Empty;
            if (msXmlGenerator.ToLower().Contains("readw"))
            {
                // ReAdW
                // Note that msXmlGenerator will likely be ReAdW.exe
                mMSXmlGeneratorAppPath = DetermineProgramLocation("ReAdWProgLoc", msXmlGenerator);
            }
            else if (msXmlGenerator.ToLower().Contains("msconvert"))
            {
                // MSConvert
                // MSConvert.exe is stored in the ProteoWizard folder
                var ProteoWizardDir = m_mgrParams.GetParam("ProteoWizardDir");
                mMSXmlGeneratorAppPath = Path.Combine(ProteoWizardDir, msXmlGenerator);
            }
            else
            {
                LogError("Invalid value for MSXMLGenerator; should be 'ReAdW' or 'MSConvert'");
                return false;
            }

            if (!string.IsNullOrEmpty(mMSXmlGeneratorAppPath))
            {
                ioToolFiles.Add(new FileInfo(mMSXmlGeneratorAppPath));
            }
            else
            {
                // Invalid value for ProgramPath
                LogError("MSXMLGenerator program path is empty");
                return false;
            }

            var recalculatePrecursors = m_jobParams.GetJobParameter("RecalculatePrecursors", false);

            if (recalculatePrecursors)
            {
                string recalculatePrecursorsTool;
                var recalculatePrecursorsToolProgLoc = GetRecalculatePrecursorsToolProgLoc(out recalculatePrecursorsTool);

                if (!string.IsNullOrEmpty(recalculatePrecursorsToolProgLoc))
                {
                    ioToolFiles.Add(new FileInfo(recalculatePrecursorsToolProgLoc));
                }
            }

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, saveToolVersionTextFile: true);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        #endregion

        #region "Event Handlers"

        /// <summary>
        /// Event handler for msXmlGen.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void MSXmlGen_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_MSXML_GEN_RUNNING);
            LogProgress("MSXmlGen");
        }

        /// <summary>
        /// Event handler for msXmlGen.ProgRunnerStarting event
        /// </summary>
        /// <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
        /// <remarks></remarks>
        private void MSXmlGen_ProgRunnerStarting(string CommandLine)
        {
            LogDebug(CommandLine);
        }

        #endregion
    }
}
