//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
//*********************************************************************************************************

using PHRPReader;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMSGFPlugin
{
    /// <summary>
    /// Retrieve resources for the MSGF plugin
    /// </summary>
    public class AnalysisResourcesMSGF : AnalysisResources
    {
        // Ignore Spelling: MODa, ModDefs, msgfdb

        /// <summary>
        /// ModDefs file suffix
        /// </summary>
        public const string PHRP_MOD_DEFS_SUFFIX = "_ModDefs.txt";

        /// <summary>
        /// Keys are the original file name, values are the new name
        /// </summary>
        private Dictionary<string, string> mPendingFileRenames;

        /// <summary>
        /// Gets all files needed by MSGF
        /// </summary>
        /// <returns>CloseOutType specifying results</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            mPendingFileRenames = new Dictionary<string, string>();

            var scriptName = mJobParams.GetParam("ToolName");

            if (!scriptName.StartsWith("MSGFPlus", StringComparison.OrdinalIgnoreCase))
            {
                // Make sure the machine has enough free memory to run MSGF
                if (!ValidateFreeMemorySize("MSGFJavaMemorySize"))
                {
                    mInsufficientFreeMemory = true;
                    mMessage = "Not enough free memory to run MSGF";
                    return CloseOutType.CLOSEOUT_RESET_JOB_STEP_INSUFFICIENT_MEMORY;
                }
            }

            // Get analysis results files
            var resultTypeName = GetResultType(mJobParams);

            var retrievalResult = GetInputFiles(resultTypeName);

            if (retrievalResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieves input files needed for MSGF
        /// </summary>
        /// <param name="resultTypeName">String specifying type of analysis results input to extraction process</param>
        /// <returns>CloseOutType specifying results</returns>
        private CloseOutType GetInputFiles(string resultTypeName)
        {
            bool onlyCopyFirstHitsAndSynopsisFiles;

            // Make sure the ResultType is valid
            var resultType = ReaderFactory.GetPeptideHitResultType(resultTypeName);

            bool validToolType;

            if (resultType is
                PeptideHitResultTypes.Sequest or
                PeptideHitResultTypes.XTandem or
                PeptideHitResultTypes.Inspect or
                PeptideHitResultTypes.MSGFPlus or
                PeptideHitResultTypes.MODa or
                PeptideHitResultTypes.MODPlus or
                PeptideHitResultTypes.MSPathFinder)
            {
                validToolType = true;
            }
            else
            {
                LogError("Invalid tool result type in AnalysisResourcesMSGF.GetInputFiles (not supported by MSGF): " + resultType);
                validToolType = false;
            }

            if (!validToolType)
            {
                return CloseOutType.CLOSEOUT_NO_OUT_FILES;
            }

            // Make sure the dataset type is valid
            var rawDataTypeName = mJobParams.GetParam("RawDataType");
            var rawDataType = GetRawDataType(rawDataTypeName);
            var mgfInstrumentData = mJobParams.GetJobParameter("MGFInstrumentData", false);

            if (resultType == PeptideHitResultTypes.MSGFPlus)
            {
                // We do not need the mzML file, the parameter file, or various other files if we are running MS-GF+ and running MSGF v6432 or later
                // Determine this by looking for job parameter MSGF_Version

                var msgfStepToolVersion = mJobParams.GetParam("MSGF_Version");

                if (string.IsNullOrWhiteSpace(msgfStepToolVersion))
                {
                    // Production version of MS-GF+; don't need the parameter file or mzML file
                    onlyCopyFirstHitsAndSynopsisFiles = true;
                }
                else
                {
                    // Specific version of MSGF is defined
                    // Check whether the version is one of the known versions for the old MSGF
                    onlyCopyFirstHitsAndSynopsisFiles = !MSGFRunner.IsLegacyMSGFVersion(msgfStepToolVersion);
                }
            }
            else if (resultType is
                PeptideHitResultTypes.MODa or
                PeptideHitResultTypes.MODPlus or
                PeptideHitResultTypes.MSPathFinder)
            {
                // We do not need any raw data files for MODa, ModPlus, or MSPathFinder since we don't run MSGF on top-down results
                onlyCopyFirstHitsAndSynopsisFiles = true;
            }
            else
            {
                // Not running MS-GF+ or running MS-GF+ but using legacy MSGF
                onlyCopyFirstHitsAndSynopsisFiles = false;

                if (!mgfInstrumentData)
                {
                    switch (rawDataType)
                    {
                        case RawDataTypeConstants.ThermoRawFile:
                        case RawDataTypeConstants.mzML:
                        case RawDataTypeConstants.mzXML:
                            // This is a valid data type
                            break;

                        default:
                            mMessage = "Dataset type " + rawDataType + " is not supported by MSGF";

                            LogDebug(
                                "{0}; must be one of the following: {1}, {2}, {3}",
                                mMessage,
                                RAW_DATA_TYPE_DOT_RAW_FILES,
                                RAW_DATA_TYPE_DOT_MZML_FILES,
                                RAW_DATA_TYPE_DOT_MZXML_FILES);

                            return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            if (!onlyCopyFirstHitsAndSynopsisFiles)
            {
                // Get the SEQUEST, X!Tandem, Inspect, MS-GF+, MODa, MODPlus, or MSPathFinder parameter file
                var paramFile = mJobParams.GetParam("ParamFileName");

                if (!FileSearchTool.FindAndRetrieveMiscFiles(paramFile, false))
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
                mJobParams.AddResultFileToSkip(paramFile);

                // Also copy the _ProteinMods.txt file
                var proteinModsFile = ReaderFactory.GetPHRPProteinModsFileName(resultType, DatasetName);

                if (!FileSearchTool.FindAndRetrieveMiscFiles(proteinModsFile, false))
                {
                    // Ignore this error; we don't really need this file
                }
                else
                {
                    mJobParams.AddResultFileToKeep(proteinModsFile);
                }
            }

            // Get the PHRP _syn.txt file
            var synopsisFile = ReaderFactory.GetPHRPSynopsisFileName(resultType, DatasetName);
            string synFilePath;

            if (!string.IsNullOrEmpty(synopsisFile))
            {
                var synopsisFileFound = FileSearchTool.FindAndRetrievePHRPDataFile(ref synopsisFile, "");

                if (!synopsisFileFound)
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
                synFilePath = Path.Combine(mWorkDir, synopsisFile);
            }
            else
            {
                synFilePath = string.Empty;
            }

            // Get the PHRP _fht.txt file
            var firstHitsFile = ReaderFactory.GetPHRPFirstHitsFileName(resultType, DatasetName);

            if (!string.IsNullOrEmpty(firstHitsFile))
            {
                var firstHitsFileFound = FileSearchTool.FindAndRetrievePHRPDataFile(ref firstHitsFile, synFilePath);

                if (!firstHitsFileFound)
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
            }

            // Get the PHRP _ResultToSeqMap.txt file
            var resultToSeqMapFile = ReaderFactory.GetPHRPResultToSeqMapFileName(resultType, DatasetName);
            bool resultToSeqMapFileFound;

            if (!string.IsNullOrEmpty(resultToSeqMapFile))
            {
                resultToSeqMapFileFound = FileSearchTool.FindAndRetrievePHRPDataFile(ref resultToSeqMapFile, synFilePath);

                if (!resultToSeqMapFileFound)
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
            }
            else
            {
                resultToSeqMapFileFound = false;
            }

            // Get the PHRP _SeqToProteinMap.txt file
            var seqToProteinMapFile = ReaderFactory.GetPHRPSeqToProteinMapFileName(resultType, DatasetName);
            bool seqToProteinMapFileFound;

            if (!string.IsNullOrEmpty(seqToProteinMapFile))
            {
                seqToProteinMapFileFound = FileSearchTool.FindAndRetrievePHRPDataFile(ref seqToProteinMapFile, synFilePath);

                if (!seqToProteinMapFileFound)
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
            }
            else
            {
                seqToProteinMapFileFound = false;
            }

            // Get the PHRP _PepToProtMapMTS.txt file
            var pepToProteinMapFile = ReaderFactory.GetPHRPPepToProteinMapFileName(resultType, DatasetName);

            if (!string.IsNullOrEmpty(pepToProteinMapFile))
            {
                // We're passing a dummy syn file name to FileSearch.FindAndRetrievePHRPDataFile
                // because there are a few jobs that have file _msgfplus_fht.txt (created by the November 2016 version of the DataExtractor tool)
                // but also have file msgfdb_PepToProtMapMTS.txt (created by an older version of the MSGFPlus tool)
                var pepToProteinMapFileFound = FileSearchTool.FindAndRetrievePHRPDataFile(ref pepToProteinMapFile, "Dataset_msgfdb.txt");

                if (!pepToProteinMapFileFound)
                {
                    if (mJobParams.GetJobParameter("IgnorePeptideToProteinMapError", false))
                    {
                        LogWarning("Ignoring missing _PepToProtMapMTS.txt file since 'IgnorePeptideToProteinMapError' is true");
                    }
                    else if (mJobParams.GetJobParameter("SkipProteinMods", false))
                    {
                        LogWarning("Ignoring missing _PepToProtMapMTS.txt file since 'SkipProteinMods' is true");
                    }
                    else
                    {
                        // Errors were reported in method call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }

            var downloadQueueProcessed = ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories);

            if (!downloadQueueProcessed)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var synopsisFileInfo = new FileInfo(synFilePath);
            long synFileSizeBytes = synopsisFileInfo.Exists ? synopsisFile.Length : 0;

            // Get the ModSummary.txt file
            // Note that MSGFResultsSummarizer will use this file to determine if a dynamic reporter ion search was performed (e.g. dynamic TMT)

            var modSummaryFile = ReaderFactory.GetPHRPModSummaryFileName(resultType, DatasetName);
            var modSummaryFileFound = FileSearchTool.FindAndRetrievePHRPDataFile(ref modSummaryFile, synFilePath);

            if (!modSummaryFileFound)
            {
                // _ModSummary.txt file not found
                // This will happen if the synopsis file is empty
                // Try to copy the _ModDefs.txt file instead

                if (synFileSizeBytes == 0)
                {
                    // If the synopsis file is 0-bytes, the _ModSummary.txt file won't exist; that's OK

                    var modDefsFile = Path.GetFileNameWithoutExtension(mJobParams.GetParam("ParamFileName")) + PHRP_MOD_DEFS_SUFFIX;

                    if (FileSearchTool.FindAndRetrieveMiscFiles(modDefsFile, false))
                    {
                        // Rename the file to end in _ModSummary.txt
                        mPendingFileRenames.Add(modDefsFile, modSummaryFile);
                    }
                    else
                    {
                        // Errors were reported in method call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
                else
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
            }

            // Copy the PHRP files so that the PHRPReader can determine the modified residues and extract the protein names
            // MSGFResultsSummarizer also uses these files

            if (!string.IsNullOrEmpty(resultToSeqMapFile))
            {
                if (!resultToSeqMapFileFound)
                {
                    if (synFileSizeBytes == 0)
                    {
                        // If the synopsis file is 0-bytes, the _ResultToSeqMap.txt file won't exist
                        // That's OK; we'll create an empty file with just a header line
                        if (!CreateEmptyResultToSeqMapFile(resultToSeqMapFile))
                        {
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }
                    else
                    {
                        // Errors were reported in method call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }

            if (!string.IsNullOrEmpty(seqToProteinMapFile))
            {
                if (!seqToProteinMapFileFound)
                {
                    if (synFileSizeBytes == 0)
                    {
                        // If the synopsis file is 0-bytes, the _SeqToProteinMap.txt file won't exist
                        // That's OK; we'll create an empty file with just a header line
                        if (!CreateEmptySeqToProteinMapFile(seqToProteinMapFile))
                        {
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }
                    else
                    {
                        // Errors were reported in method call, so just return
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }

            var seqInfoFile = ReaderFactory.GetPHRPSeqInfoFileName(resultType, DatasetName);

            if (!string.IsNullOrEmpty(seqInfoFile))
            {
                var seqInfoFileFound = FileSearchTool.FindAndRetrievePHRPDataFile(ref seqInfoFile, synFilePath);

                if (!seqInfoFileFound)
                {
                    LogWarning("SeqInfo file not found (" + seqInfoFile + "); modifications will be inferred using the ModSummary.txt file");
                }
            }

            if (mgfInstrumentData)
            {
                var fileToFind = DatasetName + DOT_MGF_EXTENSION;

                if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToFind, false))
                {
                    mMessage = "Instrument data not found: " + fileToFind;
                    LogError("AnalysisResourcesMSGF.GetResources: " + mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION);
            }
            else if (!onlyCopyFirstHitsAndSynopsisFiles)
            {
                // See if a .mzXML file already exists for this dataset
                var mzXmlFileRetrieved = FileSearchTool.RetrieveMZXmlFile(false, out var mzXMLFilePath);

                // Make sure we don't move the .mzXML file into the results folder
                mJobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);

                if (mzXmlFileRetrieved)
                {
                    // .mzXML file found and copied locally; no need to retrieve the .Raw file
                    if (mDebugLevel >= 1)
                    {
                        LogMessage("Existing .mzXML file found: " + mzXMLFilePath);
                    }

                    // Possibly unzip the .mzXML file
                    var mzXMLFile = new FileInfo(Path.Combine(mWorkDir, DatasetName + DOT_MZXML_EXTENSION + DOT_GZ_EXTENSION));

                    if (mzXMLFile.Exists)
                    {
                        mJobParams.AddResultFileExtensionToSkip(DOT_GZ_EXTENSION);

                        if (!GUnzipFile(mzXMLFile.FullName))
                        {
                            mMessage = "Error decompressing .mzXML.gz file";
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                }
                else
                {
                    // .mzXML file not found
                    // Retrieve the .Raw file so that we can make the .mzXML file prior to running MSGF
                    if (FileSearchTool.RetrieveSpectra(rawDataTypeName))
                    {
                        mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                        // Raw file
                        mJobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);
                        // mzXML file
                    }
                    else
                    {
                        LogError("AnalysisResourcesMSGF.GetResources: Error occurred retrieving spectra.");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            var downloadQueueProcessed2 = ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories);

            if (!downloadQueueProcessed2)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            foreach (var entry in mPendingFileRenames)
            {
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, entry.Key));

                if (sourceFile.Exists)
                {
                    sourceFile.MoveTo(Path.Combine(mWorkDir, entry.Value));
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool CreateEmptyResultToSeqMapFile(string resultToSeqMapFile)
        {
            try
            {
                var filePath = Path.Combine(mWorkDir, resultToSeqMapFile);
                using var writer = new StreamWriter(new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read));

                writer.WriteLine("Result_ID\tUnique_Seq_ID");
            }
            catch (Exception ex)
            {
                var errorMessage = "Error creating empty ResultToSeqMap file: " + ex.Message;
                LogError(errorMessage);
                return false;
            }

            return true;
        }

        private bool CreateEmptySeqToProteinMapFile(string fileName)
        {
            try
            {
                var filePath = Path.Combine(mWorkDir, fileName);
                using var writer = new StreamWriter(new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read));

                writer.WriteLine("Unique_Seq_ID\tCleavage_State\tTerminus_State\tProtein_Name\tProtein_Expectation_Value_Log(e)\tProtein_Intensity_Log(I)");
            }
            catch (Exception ex)
            {
                LogError("Error creating empty SeqToProteinMap file: " + ex.Message);
                return false;
            }

            return true;
        }
    }
}
