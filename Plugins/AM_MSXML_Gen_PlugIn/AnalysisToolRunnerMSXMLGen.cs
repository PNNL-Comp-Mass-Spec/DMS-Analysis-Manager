﻿//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 02/06/2009
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;

namespace AnalysisManagerMsXmlGenPlugIn
{
    /// <summary>
    /// Class for running MS XML generator to generate mzXML or .mzML files
    /// </summary>
    public class AnalysisToolRunnerMSXMLGen : AnalysisToolRunnerBase
    {
        // Ignore Spelling: Reindex, mgf

        #region "Module Variables"

        private const float PROGRESS_PCT_MSXML_GEN_RUNNING = 5;

        private string mMSXmlGeneratorAppPath = string.Empty;

        private AnalysisResources.MSXMLOutputTypeConstants mMSXmlOutputFileType;

        private DirectoryInfo mMSXmlCacheFolder;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs ReAdW or MSConvert
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
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

            var storeInCache = mJobParams.GetJobParameter("StoreMSXmlInCache", true);
            if (storeInCache)
            {
                var msXMLCacheFolderPath = mMgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
                mMSXmlCacheFolder = new DirectoryInfo(msXMLCacheFolderPath);

                if (!mMSXmlCacheFolder.Exists)
                {
                    LogError("MSXmlCache folder not found: " + msXMLCacheFolderPath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            var result = CreateMSXMLFile();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            if (!PostProcessMSXmlFile())
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Generate the mzXML or mzML file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType CreateMSXMLFile()
        {
            try
            {
                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerMSXMLGen.CreateMSXMLFile(): Enter");
                }

                var msXmlGenerator = mJobParams.GetParam("MSXMLGenerator");          // ReAdW.exe or MSConvert.exe
                var msXmlFormat = mJobParams.GetParam("MSXMLOutputType");            // Typically mzXML or mzML

                // Determine the output type
                if (msXmlFormat.Equals(MSXmlGen.MZXML_FILE_FORMAT, StringComparison.OrdinalIgnoreCase))
                {
                    mMSXmlOutputFileType = AnalysisResources.MSXMLOutputTypeConstants.mzXML;
                }
                else if (msXmlFormat.Equals(MSXmlGen.MZML_FILE_FORMAT, StringComparison.OrdinalIgnoreCase))
                {
                    mMSXmlOutputFileType = AnalysisResources.MSXMLOutputTypeConstants.mzML;
                }
                else if (msXmlFormat.Equals(MSXmlGen.MGF_FILE_FORMAT, StringComparison.OrdinalIgnoreCase))
                {
                    mMSXmlOutputFileType = AnalysisResources.MSXMLOutputTypeConstants.mgf;
                }
                else
                {
                    LogWarning("msXmlFormat string is not mzXML, mzML, or mgf (" + msXmlFormat + "); will default to mzML");
                    mMSXmlOutputFileType = AnalysisResources.MSXMLOutputTypeConstants.mzML;
                }

                // Lookup Centroid Settings
                var centroidMSXML = mJobParams.GetJobParameter("CentroidMSXML", false);
                var centroidMS1 = mJobParams.GetJobParameter("CentroidMS1", false);
                var centroidMS2 = mJobParams.GetJobParameter("CentroidMS2", false);

                if (centroidMSXML)
                {
                    centroidMS1 = true;
                    centroidMS2 = true;
                }

                // Look for parameter CentroidPeakCountToRetain in the MSXMLGenerator section
                // If the value is -1, will retain all data points
                var centroidPeakCountToRetain = mJobParams.GetJobParameter("MSXMLGenerator", "CentroidPeakCountToRetain", 0);

                if (centroidPeakCountToRetain == 0)
                {
                    // Look for parameter CentroidPeakCountToRetain in any section
                    centroidPeakCountToRetain = mJobParams.GetJobParameter("CentroidPeakCountToRetain",
                        MSXmlGenMSConvert.DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN);
                }

                // Look for custom processing arguments
                var customMSConvertArguments = mJobParams.GetJobParameter("MSXMLGenerator", "CustomMSConvertArguments", "");

                if (string.IsNullOrEmpty(mMSXmlGeneratorAppPath))
                {
                    LogWarning("mMSXmlGeneratorAppPath is empty; this is unexpected");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var rawDataTypeName = mJobParams.GetParam("RawDataType");
                var rawDataType = AnalysisResources.GetRawDataType(rawDataTypeName);

                MSXmlGen msXmlGen;

                // Determine the program path and Instantiate the processing class
                if (msXmlGenerator.IndexOf("readw", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    // ReAdW
                    // mMSXmlGeneratorAppPath should have been populated during the call to StoreToolVersionInfo()

                    msXmlGen = new MSXMLGenReadW(
                        mWorkDir, mMSXmlGeneratorAppPath, mDatasetName,
                        rawDataType, mMSXmlOutputFileType,
                        centroidMS1 || centroidMS2,
                        mJobParams);

                    if (rawDataTypeName != AnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES)
                    {
                        LogError("ReAdW can only be used with .Raw files, not with " + rawDataTypeName);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else if (msXmlGenerator.IndexOf("msconvert", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    // MSConvert

                    if (string.IsNullOrWhiteSpace(customMSConvertArguments))
                    {
                        msXmlGen = new MSXmlGenMSConvert(
                            mWorkDir, mMSXmlGeneratorAppPath, mDatasetName,
                            rawDataType, mMSXmlOutputFileType,
                            centroidMS1, centroidMS2,
                            centroidPeakCountToRetain,
                            mJobParams);
                    }
                    else
                    {
                        msXmlGen = new MSXmlGenMSConvert(
                            mWorkDir, mMSXmlGeneratorAppPath, mDatasetName,
                            rawDataType, mMSXmlOutputFileType,
                            customMSConvertArguments, mJobParams);
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

                msXmlGen.DebugLevel = mDebugLevel;

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
            recalculatePrecursorsTool = mJobParams.GetJobParameter("RecalculatePrecursorsTool", string.Empty);
            if (string.IsNullOrWhiteSpace(recalculatePrecursorsTool))
            {
                LogError("Job parameter RecalculatePrecursorsTool is not defined in the settings file; cannot determine tool to use");
                return string.Empty;
            }

            if (string.Equals(recalculatePrecursorsTool, RawConverterRunner.RAW_CONVERTER_FILENAME, StringComparison.OrdinalIgnoreCase))
            {
                var rawConverterDir = mMgrParams.GetParam("RawConverterProgLoc");
                if (string.IsNullOrWhiteSpace(rawConverterDir))
                {
                    LogError("Manager parameter RawConverterProgLoc is not defined; cannot find the directory for " +
                             RawConverterRunner.RAW_CONVERTER_FILENAME);
                    return string.Empty;
                }

                return Path.Combine(rawConverterDir, RawConverterRunner.RAW_CONVERTER_FILENAME);
            }

            return string.Empty;
        }

        private bool PostProcessMSXmlFile()
        {
            try
            {
                var resultFileExtension = mMSXmlOutputFileType switch
                {
                    AnalysisResources.MSXMLOutputTypeConstants.mzML => AnalysisResources.DOT_MZML_EXTENSION,
                    AnalysisResources.MSXMLOutputTypeConstants.mzXML => AnalysisResources.DOT_MZXML_EXTENSION,
                    AnalysisResources.MSXMLOutputTypeConstants.mgf => AnalysisResources.DOT_MGF_EXTENSION,
                    _ => throw new Exception("Unrecognized MSXMLOutputType value")
                };

                var msXmlFilePath = Path.Combine(mWorkDir, mDatasetName + resultFileExtension);
                var msXmlFile = new FileInfo(msXmlFilePath);

                if (!msXmlFile.Exists)
                {
                    LogError(resultFileExtension + " file not found: " + Path.GetFileName(msXmlFilePath));
                    return false;
                }

                // Possibly update the file using results from RawConverter

                var recalculatePrecursors = mJobParams.GetJobParameter("RecalculatePrecursors", false);
                if (recalculatePrecursors && mMSXmlOutputFileType != AnalysisResources.MSXMLOutputTypeConstants.mgf)
                {
                    var success = RecalculatePrecursorIons(msXmlFile);
                    if (!success)
                    {
                        return false;
                    }
                }

                // Compress the file using GZip
                LogMessage("GZipping " + msXmlFile.Name);

                // Note that if this process turns out to be slow, we can have MSConvert do this for us using --gzip
                // However, that will not work if RecalculatePrecursors is true
                var msXmlFileZipped = GZipFile(msXmlFile);
                if (msXmlFileZipped == null)
                {
                    return false;
                }

                var storeInDataset = mJobParams.GetJobParameter("StoreMSXmlInDataset", false);
                var storeInCache = mJobParams.GetJobParameter("StoreMSXmlInCache", true);

                if (!storeInDataset && !storeInCache)
                    storeInCache = true;

                if (!storeInDataset)
                {
                    // Do not move the .mzXML or .mzML file to the result folder
                    mJobParams.AddResultFileExtensionToSkip(resultFileExtension);
                    mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_GZ_EXTENSION);
                }

                if (storeInCache)
                {
                    // Copy the .mzXML or .mzML file to the MSXML cache
                    var remoteCacheFilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, msXmlFileZipped.FullName, purgeOldFilesIfNeeded: true);

                    if (string.IsNullOrEmpty(remoteCacheFilePath))
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            LogError("CopyFileToServerCache returned false for " + msXmlFileZipped.Name);
                        }
                        return false;
                    }

                    // Create the _CacheInfo.txt file
                    var cacheInfoFilePath = msXmlFilePath + "_CacheInfo.txt";
                    using var writer = new StreamWriter(new FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                    writer.WriteLine(remoteCacheFilePath);
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
        private bool RecalculatePrecursorIons(FileSystemInfo sourceMsXmlFile)
        {
            if (mMSXmlOutputFileType != AnalysisResources.MSXMLOutputTypeConstants.mzML)
            {
                LogError("Unsupported file extension for RecalculatePrecursors=True; must be mzML, not " + mMSXmlOutputFileType);
                return false;
            }

            var rawDataTypeName = mJobParams.GetParam("RawDataType");
            var rawDataType = AnalysisResources.GetRawDataType(rawDataTypeName);
            string rawFilePath;

            if (rawDataType == AnalysisResources.eRawDataTypeConstants.ThermoRawFile)
            {
                rawFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_RAW_EXTENSION);
            }
            else
            {
                LogError("Unsupported dataset type for RecalculatePrecursors=True; must be .Raw, not " + rawDataType);
                return false;
            }

            var recalculatePrecursorsToolProgLoc = GetRecalculatePrecursorsToolProgLoc(out var recalculatePrecursorsTool);
            if (string.IsNullOrWhiteSpace(recalculatePrecursorsToolProgLoc))
            {
                return false;
            }

            if (string.Equals(recalculatePrecursorsTool, RawConverterRunner.RAW_CONVERTER_FILENAME, StringComparison.OrdinalIgnoreCase))
            {
                // Using RawConverter.exe
                var rawConverterExe = new FileInfo(recalculatePrecursorsToolProgLoc);

                if (rawConverterExe.Directory == null)
                {
                    LogError("Cannot determine the parent directory of " + rawConverterExe.FullName);
                    return false;
                }

                var rawConverterSuccess = RecalculatePrecursorIonsCreateMGF(rawConverterExe.Directory.FullName, rawFilePath, out var mgfFile);
                if (!rawConverterSuccess)
                    return false;

                var mzMLUpdated = RecalculatePrecursorIonsUpdateMzML(sourceMsXmlFile, mgfFile);
                return mzMLUpdated;
            }

            LogError("Unsupported tool for precursor recalculation: " + recalculatePrecursorsTool);
            return false;
        }

        /// <summary>
        /// Use RawConverter to process the Thermo .Raw file and recalculate the precursor ion information, writing the results to a .MGF file
        /// </summary>
        /// <param name="rawConverterDir"></param>
        /// <param name="rawFilePath"></param>
        /// <param name="mgfFile"></param>
        private bool RecalculatePrecursorIonsCreateMGF(string rawConverterDir, string rawFilePath, out FileInfo mgfFile)
        {
            try
            {
                if (mMessage == null)
                    mMessage = string.Empty;
                var messageAtStart = string.Copy(mMessage);

                var converter = new RawConverterRunner(rawConverterDir, mDebugLevel);
                RegisterEvents(converter);

                var success = converter.ConvertRawToMGF(rawFilePath);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(mMessage) || string.Equals(messageAtStart, mMessage))
                    {
                        LogError("Unknown RawConverter error");
                    }
                    mgfFile = null;
                    return false;
                }

                // Confirm that RawConverter created a .mgf file

                mgfFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MGF_EXTENSION));
                if (!mgfFile.Exists)
                {
                    LogError("RawConverter did not create file " + mgfFile.Name);
                    return false;
                }

                mJobParams.AddResultFileToSkip(mgfFile.Name);

                return true;
            }
            catch (Exception ex)
            {
                LogError("RawConverter error", ex);
                mMessage = "Exception running RawConverter";
                mgfFile = null;
                return false;
            }
        }

        private bool RecalculatePrecursorIonsUpdateMzML(FileSystemInfo sourceMsXmlFile, FileSystemInfo mgfFile)
        {
            try
            {
                var messageAtStart = string.Copy(mMessage);

                var updater = new ParentIonUpdater();
                RegisterEvents(updater);

                var updatedMzMLPath = updater.UpdateMzMLParentIonInfoUsingMGF(sourceMsXmlFile.FullName, mgfFile.FullName, false);

                if (string.IsNullOrEmpty(updatedMzMLPath))
                {
                    if (string.IsNullOrWhiteSpace(mMessage) || string.Equals(messageAtStart, mMessage))
                    {
                        LogError("Unknown ParentIonUpdater error");
                    }
                    return false;
                }

                // Confirm that ParentIonUpdater created a new .mzML file

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
                mMessage = "Exception in RecalculatePrecursorIonsUpdateMzML";
                return false;
            }
        }

        private bool ReindexMzML(string mzMLFilePath)
        {
            if (string.IsNullOrWhiteSpace(mzMLFilePath))
            {
                LogWarning("ReindexMzML: mzMLFilePath is an empty string");
                return false;
            }

            try
            {
                if (mDebugLevel > 4)
                {
                    LogDebug("Re-index the mzML file using MSConvert");
                }

                var msXmlGenerator = mJobParams.GetParam("MSXMLGenerator");
                // Must be MSConvert.exe

                if (msXmlGenerator.IndexOf("msconvert", StringComparison.OrdinalIgnoreCase) < 0)
                {
                    LogError("ParentIonUpdater only supports MSConvert, not " + msXmlGenerator);
                    return false;
                }

                if (string.IsNullOrEmpty(mMSXmlGeneratorAppPath))
                {
                    LogError("mMSXmlGeneratorAppPath is empty; this is unexpected");
                    return false;
                }

                const AnalysisResources.eRawDataTypeConstants rawDataType = AnalysisResources.eRawDataTypeConstants.mzML;
                const AnalysisResources.MSXMLOutputTypeConstants outputFileType = AnalysisResources.MSXMLOutputTypeConstants.mzML;

                var sourceFileBase = Path.GetFileNameWithoutExtension(mzMLFilePath);

                var msConvertRunner = new MSXmlGenMSConvert(
                    mWorkDir, mMSXmlGeneratorAppPath,
                    sourceFileBase, rawDataType, outputFileType,
                    centroidMS1: false, centroidMS2: false,
                    centroidPeakCountToRetain: 0,
                    jobParams: mJobParams)
                {
                    ConsoleOutputSuffix = "_Reindex",
                    DebugLevel = mDebugLevel
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

                mJobParams.AddResultFileToSkip(msConvertRunner.ConsoleOutputFileName);

                if (msConvertRunner.ErrorMessage.Length > 0)
                {
                    LogError(msConvertRunner.ErrorMessage);
                }

                // Replace the original .mzML file with the new .mzML file
                var reIndexedMzMLFile = new FileInfo(Path.Combine(mWorkDir, msConvertRunner.OutputFileName));

                if (!reIndexedMzMLFile.Exists)
                {
                    LogError("Re-indexed mzML file not found at " + reIndexedMzMLFile.FullName);
                    mMessage = "Re-indexed mzML file not found";
                    return false;
                }

                // Replace the original .mzML file with the indexed one
                File.Delete(mzMLFilePath);

                reIndexedMzMLFile.MoveTo(mzMLFilePath);
                return true;
            }
            catch (Exception ex)
            {
                LogError("ReindexMzML error", ex);
                mMessage = "Exception in ReindexMzML";
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>();

            // Determine the path to the XML Generator
            // ReAdW.exe or MSConvert.exe
            var msXmlGenerator = mJobParams.GetParam("MSXMLGenerator");

            mMSXmlGeneratorAppPath = string.Empty;
            if (msXmlGenerator.IndexOf("readw", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // ReAdW
                // Note that msXmlGenerator will likely be ReAdW.exe
                mMSXmlGeneratorAppPath = DetermineProgramLocation("ReAdWProgLoc", msXmlGenerator);
            }
            else if (msXmlGenerator.IndexOf("msconvert", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // MSConvert
                // MSConvert.exe is stored in the ProteoWizard folder
                var proteoWizardDir = mMgrParams.GetParam("ProteoWizardDir");
                mMSXmlGeneratorAppPath = Path.Combine(proteoWizardDir, msXmlGenerator);

                var success = mToolVersionUtilities.GetMSConvertToolVersion(mMSXmlGeneratorAppPath, out var msConvertVersion);
                if (!success)
                {
                    LogError(string.Format("Unable to determine the version of {0}", msXmlGenerator), true);
                }
                else
                {
                    toolVersionInfo = Global.AppendToComment(toolVersionInfo, msConvertVersion);
                }

                // File Tool_Version_Info_MSXML_Gen.txt will contain the MSConvert version,
                // so the following statement is commented out to prevent the creation of Tool_Version_Info_MSConvert.txt
                // mToolVersionUtilities.SaveToolVersionInfoFile(mWorkDir, msConvertVersion, "MSConvert");
            }
            else
            {
                LogError("Invalid value for MSXMLGenerator; should be 'ReAdW' or 'MSConvert'");
                return false;
            }

            if (!string.IsNullOrEmpty(mMSXmlGeneratorAppPath))
            {
                toolFiles.Add(new FileInfo(mMSXmlGeneratorAppPath));
            }
            else
            {
                // Invalid value for ProgramPath
                LogError("MSXMLGenerator program path is empty");
                return false;
            }

            var recalculatePrecursors = mJobParams.GetJobParameter("RecalculatePrecursors", false);

            if (recalculatePrecursors)
            {
                var recalculatePrecursorsToolProgLoc = GetRecalculatePrecursorsToolProgLoc(out _);

                if (!string.IsNullOrEmpty(recalculatePrecursorsToolProgLoc))
                {
                    toolFiles.Add(new FileInfo(recalculatePrecursorsToolProgLoc));
                }
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile: true);
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
        private void MSXmlGen_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_MSXML_GEN_RUNNING);
            LogProgress("MSXmlGen");
        }

        /// <summary>
        /// Event handler for msXmlGen.ProgRunnerStarting event
        /// </summary>
        /// <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
        private void MSXmlGen_ProgRunnerStarting(string CommandLine)
        {
            LogDebug(CommandLine);
        }

        #endregion
    }
}