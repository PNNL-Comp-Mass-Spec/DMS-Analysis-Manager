//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 09/22/2008
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using AnalysisManagerMSGFDBPlugIn;
using PHRPReader;
using PRISM.Logging;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Manages retrieval of all files needed for data extraction
    /// </summary>
    public class AnalysisResourcesExtraction : AnalysisResources
    {
        // ReSharper disable once CommentTypo

        // Ignore Spelling: ascore, bioml, Defs, diff, dta, foreach, gzipped
        // Ignore Spelling: mgf, MSGFPlus, MODa, msgfdb, mzml, mzxml
        // Ignore Spelling: Parm, phospho, phosphorylation, PHRP, resourcer, txt

        /// <summary>
        /// ModDefs file suffix
        /// </summary>
        public const string MOD_DEFS_FILE_SUFFIX = "_ModDefs.txt";

        /// <summary>
        /// Mass Correction Tags filename
        /// </summary>
        public const string MASS_CORRECTION_TAGS_FILENAME = "Mass_Correction_Tags.txt";

        /// <summary>
        /// Job parameter used to instruct ExtractToolRunner to run AScore
        /// </summary>
        public const string JOB_PARAM_RUN_ASCORE = "RunAScore";

        /// <summary>
        /// Job parameter used to instruct ExtractToolRunner to skip PHRP since the PHRP result files are already up-to-date
        /// </summary>
        public const string JOB_PARAM_SKIP_PHRP = "SkipPHRP";

        private bool mRetrieveOrganismDB;

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);

            // Always retrieve the FASTA file because PHRP uses it
            // This includes for MS-GF+ because it uses the order of the proteins in the
            // FASTA file to determine the protein to include in the FHT file

            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Examines the SEQUEST, X!Tandem, or MS-GF+ param file to determine if ETD mode is enabled
        /// </summary>
        /// <param name="resultType">Result type</param>
        /// <param name="searchToolParamFilePath">Parameter file path for the search tool</param>
        /// <returns>True if we should run AScore, otherwise false</returns>
        private bool CheckAScoreRequired(string resultType, string searchToolParamFilePath)
        {
            if (string.IsNullOrEmpty(searchToolParamFilePath))
            {
                LogError("PeptideHit param file path is empty; unable to continue");
                return false;
            }

            mStatusTools.CurrentOperation = "Checking whether we should run AScore after Data Extraction";

            bool runAscore;

            switch (resultType)
            {
                case RESULT_TYPE_SEQUEST:
                    runAscore = CheckAScoreRequiredSEQUEST(searchToolParamFilePath);
                    break;

                case RESULT_TYPE_XTANDEM:
                    runAscore = CheckAScoreRequiredXTandem(searchToolParamFilePath);
                    break;

                case RESULT_TYPE_MSGFPLUS:
                    runAscore = CheckAScoreRequiredMSGFPlus(searchToolParamFilePath);
                    break;

                case RESULT_TYPE_DIANN:
                case RESULT_TYPE_INSPECT:
                case RESULT_TYPE_MAXQUANT:
                case RESULT_TYPE_MODA:
                case RESULT_TYPE_MODPLUS:
                case RESULT_TYPE_MSALIGN:
                case RESULT_TYPE_MSFRAGGER:
                case RESULT_TYPE_MSPATHFINDER:
                case RESULT_TYPE_TOPPIC:
                    LogDebug("{0} does not support running AScore as part of data extraction", resultType);
                    runAscore = false;
                    break;

                default:
                    LogDebug("Unrecognized result type: " + resultType);
                    runAscore = false;
                    break;
            }

            return runAscore;
        }

        /// <summary>
        /// Examines the MS-GF+ param file to determine if phospho STY is enabled
        /// </summary>
        /// <param name="searchToolParamFilePath">MS-GF+ parameter file to read</param>
        /// <returns>True if we should run AScore, otherwise false</returns>
        private bool CheckAScoreRequiredMSGFPlus(string searchToolParamFilePath)
        {
            const string DYNAMIC_MOD_TAG = "DynamicMod";

            var runAscore = false;

            try
            {
                if (mDebugLevel >= 2)
                {
                    LogDebug("Reading the MS-GF+ parameter file: " + searchToolParamFilePath);
                }

                // Read the data from the MS-GF+ Param file
                using var reader = new StreamReader(new FileStream(searchToolParamFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine) || !dataLine.StartsWith(DYNAMIC_MOD_TAG))
                        continue;

                    // Check whether this line has HO3P or mod mass 79.966 on S, T, or Y
                    // Alternatively, if the mod name is Phospho assume this is a phosphorylation search

                    if (mDebugLevel >= 3)
                    {
                        LogDebug("MS-GF+ " + DYNAMIC_MOD_TAG + " line found: " + dataLine);
                    }

                    // Look for the equals sign
                    var charIndex = dataLine.IndexOf('=');

                    if (charIndex > 0)
                    {
                        var modDef = dataLine.Substring(charIndex + 1).Trim();

                        var commentIndex = dataLine.IndexOf('#');
                        List<string> modDefParts;

                        if (commentIndex > 1)
                        {
                            modDefParts = modDef.Substring(0, commentIndex).Trim().Split(',').ToList();
                        }
                        else
                        {
                            modDefParts = modDef.Split(',').ToList();
                        }

                        if (modDefParts.Count < 5)
                        {
                            if (!modDef.StartsWith("None", StringComparison.OrdinalIgnoreCase))
                            {
                                LogWarning("Incomplete mod def line in MS-GF+ parameter file: " + modDef);
                            }
                            continue;
                        }

                        var modMassOrFormula = modDefParts[0].Trim();
                        var residues = modDefParts[1].Trim();
                        var modName = modDefParts[4].Trim();

                        if (!HasAnyResidue(residues, "STY"))
                        {
                            // Mod doesn't affect S, T, or Y
                            continue;
                        }

                        if (string.Equals(modMassOrFormula, "HO3P") ||
                            modMassOrFormula.StartsWith("79.96") ||
                            modMassOrFormula.StartsWith("79.97"))
                        {
                            runAscore = true;
                            break;
                        }

                        if (string.Equals(modName, "phospho", StringComparison.InvariantCultureIgnoreCase))
                        {
                            runAscore = true;
                            break;
                        }
                    }
                    else
                    {
                        LogWarning("MS-GF+ " + DYNAMIC_MOD_TAG + " line does not have an equals sign; ignoring " + dataLine);
                    }
                }

                return runAscore;
            }
            catch (Exception ex)
            {
                LogError("Error reading the MS-GF+ param file", ex);
                return false;
            }
        }

        /// <summary>
        /// Examines the SEQUEST param file to determine if phospho STY is enabled
        /// </summary>
        /// <param name="searchToolParamFilePath">SEQUEST parameter file to read</param>
        /// <returns>True if we should run AScore, otherwise false</returns>
        private bool CheckAScoreRequiredSEQUEST(string searchToolParamFilePath)
        {
            const string DIFF_SEARCH_OPTIONS_TAG = "diff_search_options";

            var runAscore = false;

            try
            {
                if (mDebugLevel >= 2)
                {
                    LogDebug("Reading the SEQUEST parameter file: " + searchToolParamFilePath);
                }

                // Read the data from the SEQUEST Param file
                using var reader = new StreamReader(new FileStream(searchToolParamFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine) || !dataLine.StartsWith(DIFF_SEARCH_OPTIONS_TAG))
                        continue;

                    // Check whether the dynamic mods line has 79.9663 STY (or similar)

                    if (mDebugLevel >= 3)
                    {
                        LogDebug("SEQUEST " + DIFF_SEARCH_OPTIONS_TAG + " line found: " + dataLine);
                    }

                    // Look for the equals sign
                    var charIndex = dataLine.IndexOf('=');

                    if (charIndex > 0)
                    {
                        var modDef = dataLine.Substring(charIndex + 1).Trim();

                        // Split modDef on spaces
                        var modDefParts = modDef.Split(' ');

                        if (modDefParts.Length >= 2)
                        {
                            for (var i = 0; i < modDefParts.Length; i += 2)
                            {
                                if (modDefParts.Length <= i + 1)
                                    break;

                                var residues = modDefParts[i + 1];

                                if (!HasAnyResidue(residues, "STY"))
                                {
                                    continue;
                                }

                                if (!double.TryParse(modDefParts[i], out var modMass))
                                    continue;

                                if (Math.Abs(79.966331 - modMass) < 0.01)
                                {
                                    runAscore = true;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            LogWarning("SEQUEST " + DIFF_SEARCH_OPTIONS_TAG + " line is not valid: " + dataLine);
                        }
                    }
                    else
                    {
                        LogWarning("SEQUEST " + DIFF_SEARCH_OPTIONS_TAG + " line does not have an equals sign: " + dataLine);
                    }

                    // No point in checking any further since we've parsed the ion_series line
                    break;
                }

                return runAscore;
            }
            catch (Exception ex)
            {
                LogError("Error reading the SEQUEST param file", ex);
                return false;
            }
        }

        /// <summary>
        /// Examines the X!Tandem param file to determine if phospho STY is enabled
        /// </summary>
        /// <param name="searchToolParamFilePath">X!Tandem XML parameter file to read</param>
        /// <returns>True if we should run AScore, otherwise false</returns>
        private bool CheckAScoreRequiredXTandem(string searchToolParamFilePath)
        {
            var runAscore = false;

            try
            {
                if (mDebugLevel >= 2)
                {
                    LogDebug("Reading the X!Tandem parameter file: " + searchToolParamFilePath);
                }

                // Open the parameter file
                // Look for either of these lines:
                //   <note type="input" label="residue, potential modification mass">79.9663@S,79.9663@T,79.9663@Y</note>
                //   <note type="input" label="refine, potential modification mass">79.9663@S,79.9663@T,79.9663@Y</note>

                var paramFile = new XmlDocument
                {
                    PreserveWhitespace = true
                };
                paramFile.Load(searchToolParamFilePath);

                if (paramFile.DocumentElement == null)
                {
                    LogError("Error reading the X!Tandem param file: DocumentElement is null");
                    return false;
                }

                for (var settingIndex = 0; settingIndex <= 1; settingIndex++)
                {
                    var selectedNodes = settingIndex switch
                    {
                        0 => paramFile.DocumentElement.SelectNodes("/bioml/note[@label='residue, potential modification mass']"),
                        1 => paramFile.DocumentElement.SelectNodes("/bioml/note[@label='refine, potential modification mass']"),
                        _ => null
                    };

                    if (selectedNodes == null)
                    {
                        continue;
                    }

                    for (var matchIndex = 0; matchIndex <= selectedNodes.Count - 1; matchIndex++)
                    {
                        var xmlAttributes = selectedNodes.Item(matchIndex)?.Attributes;

                        // Make sure this node has an attribute named type with value "input"
                        var attributeNode = xmlAttributes?.GetNamedItem("type");

                        if (attributeNode == null)
                        {
                            // Node does not have an attribute named "type"
                            continue;
                        }

                        if (!string.Equals(attributeNode.Value, "input", StringComparison.OrdinalIgnoreCase))
                            continue;

                        // Valid node; examine its InnerText value
                        var modDefList = selectedNodes.Item(matchIndex)?.InnerText;

                        if (string.IsNullOrWhiteSpace(modDefList))
                            continue;

                        // modDefList is of the form:
                        // 79.9663@S,79.9663@T,79.9663@Y

                        var modDefs = modDefList.Split(',');

                        foreach (var modDef in modDefs)
                        {
                            var modDefParts = modDef.Split('@');

                            if (modDefParts.Length < 2)
                                continue;

                            if (!HasAnyResidue(modDefParts[1], "STY"))
                                continue;

                            if (!double.TryParse(modDefParts[0], out var modMass))
                                continue;

                            if (Math.Abs(79.966331 - modMass) < 0.01)
                            {
                                runAscore = true;
                                break;
                            }
                        }
                    }

                    if (runAscore)
                        break;
                }

                return runAscore;
            }
            catch (Exception ex)
            {
                LogError("Error reading the X!Tandem param file", ex);

                return false;
            }
        }

        /// <summary>
        /// Gets all files needed to perform data extraction
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

            // Set this to true for now
            // It will be changed to false if processing Inspect results and the _PepToProtMap.txt file is successfully retrieved
            mRetrieveOrganismDB = true;

            var resultTypeName = GetResultType(mJobParams);

            if (string.IsNullOrWhiteSpace(resultTypeName))
            {
                LogError("Job parameter ResultType is missing; cannot get resources for extraction");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Get analysis results files
            if (GetInputFiles(resultTypeName, out var createPepToProtMapFile) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Get misc files
            if (RetrieveMiscFiles(resultTypeName) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!mRetrieveOrganismDB)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            var skipProteinMods = mJobParams.GetJobParameter("SkipProteinMods", false);

            if (skipProteinMods && !createPepToProtMapFile)
                return CloseOutType.CLOSEOUT_SUCCESS;

            // Examine the FASTA file size
            // If it is over 2 GB in size, do not retrieve the file, and force skipProteinMods to false
            const float MAX_LEGACY_FASTA_SIZE_GB = 2;

            // Retrieve the FASTA file; required to create the _ProteinMods.txt file
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

            if (RetrieveOrgDB(orgDbDirectoryPath, out var resultCode, MAX_LEGACY_FASTA_SIZE_GB, out var fastaFileSizeGB))
                return CloseOutType.CLOSEOUT_SUCCESS;

            if (fastaFileSizeGB < MAX_LEGACY_FASTA_SIZE_GB)
            {
                return resultCode;
            }

            mJobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SkipProteinMods", "true");
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieves input files needed for extraction
        /// </summary>
        /// <param name="resultTypeName">String specifying type of analysis results input to extraction process</param>
        /// <param name="createPepToProtMapFile">Output: if true, create the PepToProtMap.txt file after the FASTA file is retrieved</param>
        /// <returns>CloseOutType specifying results</returns>
        private CloseOutType GetInputFiles(string resultTypeName, out bool createPepToProtMapFile)
        {
            createPepToProtMapFile = false;

            try
            {
                var inputFolderName = mJobParams.GetParam("inputFolderName");

                if (string.IsNullOrWhiteSpace(inputFolderName))
                {
                    LogError("Input_Folder_Name is not defined for this job step in table sw.t_job_steps (job parameter inputFolderName); cannot retrieve input files");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                CloseOutType result;
                switch (resultTypeName)
                {
                    case RESULT_TYPE_DIANN:
                        result = GetDiaNNFiles();
                        break;

                    case RESULT_TYPE_INSPECT:
                        result = GetInspectFiles();
                        break;

                    case RESULT_TYPE_MAXQUANT:
                        result = GetMaxQuantFiles();
                        break;

                    case RESULT_TYPE_MODA:
                        result = GetMODaFiles();
                        break;

                    case RESULT_TYPE_MODPLUS:
                        result = GetMODPlusFiles();
                        break;

                    case RESULT_TYPE_MSALIGN:
                        result = GetMSAlignFiles();
                        break;

                    case RESULT_TYPE_MSFRAGGER:
                        result = GetMSFraggerFiles();
                        break;

                    case RESULT_TYPE_MSGFPLUS:
                        result = GetMSGFPlusFiles(out createPepToProtMapFile);
                        break;

                    case RESULT_TYPE_MSPATHFINDER:
                        result = GetMSPathFinderFiles();
                        mJobParams.AddResultFileExtensionToSkip(".tsv");
                        break;

                    case RESULT_TYPE_SEQUEST:
                        result = GetSEQUESTFiles();
                        break;

                    case RESULT_TYPE_TOPPIC:
                        result = GetTopPICFiles();
                        break;

                    case RESULT_TYPE_XTANDEM:
                        result = GetXTandemFiles();
                        break;

                    default:
                        LogError("Invalid tool result type: " + resultTypeName);
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                RetrieveToolVersionFile(resultTypeName);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error retrieving input files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType GetDiaNNFiles()
        {
            var reportParquetFile = AnalysisToolRunnerBase.GetDiannResultsFilePath(mWorkDir, DatasetName, "report.parquet");
            var reportTsvFile = AnalysisToolRunnerBase.GetDiannResultsFilePath(mWorkDir, DatasetName, "report.tsv");
            var scanInfoFile = AnalysisToolRunnerBase.GetDiannResultsFilePath(mWorkDir, DatasetName, "ScanInfo.txt");

            bool usingParquetFile;

            if (FileSearchTool.FindAndRetrieveMiscFiles(reportParquetFile.Name, false, true, logFileNotFound: false))
            {
                mJobParams.AddResultFileToSkip(reportParquetFile.Name);
                usingParquetFile = true;
            }
            else if (FileSearchTool.FindAndRetrieveMiscFiles(reportTsvFile.Name, false, true, logFileNotFound: false))
            {
                // Do not add the report.tsv file to the list of files to skip, since method UpdateDiannReportFile() updates it to remove duplicate .mzML file names
                usingParquetFile = false;
            }
            else
            {
                LogError("Could not find the DIA-NN report file ({0} or {1})", reportParquetFile.Name, reportTsvFile.Name);
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Look for a _ScanInfo.txt file (created when running DIA-NN 2.0 or newer)
            if (usingParquetFile)
            {
                if (!FileSearchTool.FindAndRetrieveMiscFiles(scanInfoFile.Name, false))
                {
                    LogError("Scan info file not found: " + scanInfoFile.Name);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mJobParams.AddResultFileToSkip(scanInfoFile.Name);
            }

            // Note that we'll obtain the DIA-NN parameter file in RetrieveMiscFiles
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetInspectFiles()
        {
            // Get the zipped Inspect results files

            // This file contains the p-value filtered results
            var fileToGet = DatasetName + "_inspect.zip";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_NO_INSPECT_FILES;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // This file contains top hit for each scan (no filters)
            fileToGet = DatasetName + "_inspect_fht.zip";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_NO_INSPECT_FILES;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Get the peptide to protein mapping file
            fileToGet = DatasetName + "_inspect_PepToProtMap.txt";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in method call

                // See if IgnorePeptideToProteinMapError is true
                if (mJobParams.GetJobParameter("IgnorePeptideToProteinMapError", false))
                {
                    LogWarning(
                        "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' is true");
                }
                else
                {
                    return CloseOutType.CLOSEOUT_NO_INSPECT_FILES;
                }
            }
            else
            {
                // The OrgDB (aka FASTA file) is not required
                mRetrieveOrganismDB = false;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the Inspect parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Copy MaxQuant files to the working directory
        /// </summary>
        /// <remarks>Does not support retrieving PrecursorInfo.txt files from MyEMSL</remarks>
        private CloseOutType GetMaxQuantFiles()
        {
            const string msmsFile = "msms.txt";

            // Look for the file in the various directories
            // A message will be logged if the file is not found
            var sourceDirPath = FileSearchTool.FindDataFile(msmsFile, true);

            if (string.IsNullOrWhiteSpace(sourceDirPath))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!FileSearchTool.FindAndRetrieveMiscFiles(msmsFile, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(msmsFile);

            const string peptidesFile = "peptides.txt";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(peptidesFile, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(peptidesFile);

            // Retrieve the precursor info file (or files if we used a data package to define multiple datasets)
            // They should be in the parent directory above sourceDirPath

            var sourceDirectory = new DirectoryInfo(sourceDirPath);
            var fileCountCopied = GetMaxQuantPrecursorInfoFiles(sourceDirectory, out var fileCopyError);

            if (fileCopyError)
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (fileCountCopied == 0 && sourceDirectory.Parent != null)
            {
                // Look for files in the parent directory, since sourceDirectory is likely the txt subdirectory
                var fileCountCopiedParent = GetMaxQuantPrecursorInfoFiles(sourceDirectory.Parent, out var fileCopyErrorParent);

                if (fileCopyErrorParent)
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (fileCountCopiedParent == 0)
                {
                    EvalMessage = "Could not find any _PrecursorInfo.txt files; will not be able to compute mass error values";
                    LogWarning(EvalMessage);
                }
            }

            mJobParams.AddResultFileExtensionToSkip("_PrecursorInfo.txt");
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private int GetMaxQuantPrecursorInfoFiles(DirectoryInfo sourceDirectory, out bool fileCopyError)
        {
            var fileCountCopied = 0;
            fileCopyError = false;

            foreach (var candidateFile in sourceDirectory.GetFiles("*_PrecursorInfo.txt"))
            {
                if (!mFileCopyUtilities.CopyFileToWorkDir(candidateFile.Name, sourceDirectory.FullName, mWorkDir, BaseLogger.LogLevels.ERROR, false))
                {
                    fileCopyError = true;
                    return fileCountCopied;
                }

                fileCountCopied++;
            }

            return fileCountCopied;
        }

        private CloseOutType GetMODaFiles()
        {
            var fileToGet = DatasetName + "_moda.zip";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);
            mJobParams.AddResultFileExtensionToSkip("_moda.txt");

            fileToGet = DatasetName + "_mgf_IndexToScanMap.txt";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the MODa parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMODPlusFiles()
        {
            // ReSharper disable StringLiteralTypo

            var fileToGet = DatasetName + "_modp.zip";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);
            mJobParams.AddResultFileExtensionToSkip("_modp.txt");

            // ReSharper restore StringLiteralTypo

            // Delete the MSConvert_ConsoleOutput.txt and MODPlus_ConsoleOutput files that were in the zip file; we don't need them

            var workingDirectory = new DirectoryInfo(mWorkDir);
            var filesToDelete = new List<FileInfo>();

            filesToDelete.AddRange(workingDirectory.GetFiles("MODPlus_ConsoleOutput_Part*.txt"));
            filesToDelete.AddRange(workingDirectory.GetFiles("MSConvert_ConsoleOutput.txt"));
            filesToDelete.AddRange(workingDirectory.GetFiles("TDA_Plus_ConsoleOutput.txt"));

            foreach (var targetFile in filesToDelete)
            {
                targetFile.Delete();
            }

            // Note that we'll obtain the MODPlus parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMSAlignFiles()
        {
            var fileToGet = DatasetName + "_MSAlign_ResultTable.txt";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the MSAlign parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieve MSFragger or FragPipe result files
        /// </summary>
        /// <returns>CloseOutType specifying results</returns>
        private CloseOutType GetMSFraggerFiles()
        {
            var filesToGet = new List<string>();

            // First copy the _psm.tsv file locally
            if (Global.IsMatch(DatasetName, AGGREGATION_JOB_DATASET) || IsDataPackageDataset(DatasetName))
            {
                // The results directory will have a file named Aggregation_psm.tsv if no experiment groups are defined
                // If experiment groups are defined, there will usually be one _psm.tsv file for each experiment group

                // However, there are cases where there are multiple experiment groups, but there is only an Aggregation_psm.tsv file,
                // in particular when FragPipe runs DIA-NN with a spectral library

                // Retrieve metadata about the datasets in this data package
                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                var success = LookupDataPackageInfo(dataPackageID, out var datasetIDsByExperimentGroup, out var dataPackageError, storeJobParameters: true);

                if (!success && dataPackageError)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        mMessage = string.Format("Error retrieving the metadata for the datasets associated with data package {0}", dataPackageID);
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!success || datasetIDsByExperimentGroup.Count <= 1)
                {
                    // Retrieve file Aggregation_psm.tsv
                    filesToGet.Add(AGGREGATION_JOB_DATASET + "_psm.tsv");
                }
                else
                {
                    // The results directory may have a file named Dataset_PSM_tsv.zip with _ion.tsv, _peptide.tsv, _protein.tsv, and _psm.tsv files
                    // This file is only created if more than three experiment groups exist, and was not created prior to 2022-04-28

                    filesToGet.Add(ZIPPED_MSFRAGGER_PSM_TSV_FILES);

                    // Retrieve file Aggregation_psm.tsv
                    filesToGet.Add(AGGREGATION_JOB_DATASET + "_psm.tsv");

                    // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var item in datasetIDsByExperimentGroup)
                    {
                        var experimentGroupName = item.Key;
                        filesToGet.Add(experimentGroupName + "_psm.tsv");
                    }
                }
            }
            else
            {
                filesToGet.Add(DatasetName + "_psm.tsv");
            }

            var sourceDirPath = string.Empty;
            var retrievedFiles = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            // If datasetName is "Aggregation", list filesToGet should have the following files:
            //   Dataset_PSM_tsv.zip
            //   Aggregation_psm.tsv
            //   ExperimentGroup1_psm.tsv
            //   ExperimentGroup2_psm.tsv
            //   etc.
            // If datasetName is not "Aggregation", list filesToGet will have just one _psm.tsv file

            foreach (var fileName in filesToGet)
            {
                if (fileName.Equals(ZIPPED_MSFRAGGER_PSM_TSV_FILES))
                {
                    // Zip file Dataset_PSM_tsv.zip is optional
                    if (FileSearchTool.FindAndRetrieveMiscFiles(fileName, false, true, out var sourceDirPathZipFile, logFileNotFound: false))
                    {
                        if (string.IsNullOrWhiteSpace(sourceDirPath))
                        {
                            sourceDirPath = sourceDirPathZipFile;
                        }

                        mJobParams.AddResultFileToSkip(fileName);

                        // Extract the TSV files from the zip file
                        var zipTools = new ZipFileTools(mDebugLevel, mWorkDir);
                        RegisterEvents(zipTools);

                        var zipFilePath = Path.Combine(mWorkDir, fileName);
                        zipTools.UnzipFile(zipFilePath, mWorkDir);

                        foreach (var tsvFile in zipTools.MostRecentUnzippedFiles)
                        {
                            mJobParams.AddResultFileToSkip(tsvFile.Key);
                            retrievedFiles.Add(tsvFile.Key);
                        }
                    }

                    continue;
                }

                if (retrievedFiles.Contains(fileName))
                {
                    // File already retrieved (via zip file Dataset_PSM_tsv.zip)
                    continue;
                }

                if (fileName.Equals(AGGREGATION_JOB_DATASET + "_psm.tsv"))
                {
                    // Look for file Aggregation_psm.tsv
                    // If found, do not search for any additional files in filesToGet

                    if (FileSearchTool.FindAndRetrieveMiscFiles(fileName, false, true, out var sourceDirPathAggregationFile, logFileNotFound: false))
                    {
                        if (string.IsNullOrWhiteSpace(sourceDirPath))
                        {
                            sourceDirPath = sourceDirPathAggregationFile;
                        }

                        mJobParams.AddResultFileToSkip(fileName);
                        retrievedFiles.Add(fileName);

                        // Exit the for loop since file Aggregation_psm.tsv was copied locally
                        break;
                    }
                }

                if (!FileSearchTool.FindAndRetrieveMiscFiles(fileName, false, true, out var sourceDirPathCurrent))
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (string.IsNullOrWhiteSpace(sourceDirPath))
                    sourceDirPath = sourceDirPathCurrent;

                mJobParams.AddResultFileToSkip(fileName);
                retrievedFiles.Add(fileName);
            }

            // Now copy the individual Dataset.tsv file(s), which we'll treat as optional
            // PHRP reads data from columns num_matched_ions and tot_num_ions and includes these in the synopsis file
            // For FragPipe, the Dataset.tsv files are stored in subdirectories when more than one experiment group is defined

            var sourceDirectory = new DirectoryInfo(sourceDirPath);

            foreach (var tsvFile in sourceDirectory.GetFiles("*.tsv", SearchOption.AllDirectories))
            {
                if (tsvFile.Name.EndsWith("_ion.tsv", StringComparison.OrdinalIgnoreCase) ||
                    tsvFile.Name.EndsWith("_peptide.tsv", StringComparison.OrdinalIgnoreCase) ||
                    tsvFile.Name.EndsWith("_protein.tsv", StringComparison.OrdinalIgnoreCase) ||
                    tsvFile.Name.Equals("reprint.int.tsv", StringComparison.OrdinalIgnoreCase) ||
                    tsvFile.Name.Equals("reprint.spc.tsv", StringComparison.OrdinalIgnoreCase) ||
                    tsvFile.Name.StartsWith("combined_site_", StringComparison.OrdinalIgnoreCase) ||
                    retrievedFiles.Contains(tsvFile.Name))
                {
                    // Either this is not a Dataset_psm.tsv file, or the file was already retrieved (via Dataset_PSM_tsv.zip)
                    continue;
                }

                if (tsvFile.Directory == null)
                {
                    LogWarning("Unable to determine the parent directory of the Dataset.tsv file: {0}", tsvFile.FullName);
                    continue;
                }

                if (tsvFile.Directory.Name.Equals("tmt-report", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (mFileCopyUtilities.CopyFileToWorkDir(tsvFile.Name, tsvFile.Directory.FullName, mWorkDir, BaseLogger.LogLevels.ERROR, false))
                {
                    mJobParams.AddResultFileToSkip(tsvFile.Name);
                }
            }

            // Note that we'll obtain the MSFragger parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMSGFPlusFiles(out bool createPepToProtMapFile)
        {
            var currentStep = "Initializing";
            createPepToProtMapFile = false;

            var splitFastaEnabled = mJobParams.GetJobParameter("SplitFasta", false);

            var numberOfClonedSteps = 1;
            var lastProgressTime = DateTime.UtcNow;

            try
            {
                string suffixToAdd;

                if (splitFastaEnabled)
                {
                    numberOfClonedSteps = mJobParams.GetJobParameter("NumberOfClonedSteps", 0);

                    if (numberOfClonedSteps == 0)
                    {
                        LogError("Settings file is missing parameter NumberOfClonedSteps; cannot retrieve MSGFPlus results");
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    suffixToAdd = "_Part1";
                }
                else
                {
                    suffixToAdd = string.Empty;
                }

                currentStep = "Determining results file type based on the results file name";
                var useLegacyMSGFDB = false;

                // Look for file DatasetName_msgfplus.mzid.gz
                // or for split FASTA, DatasetName_msgfplus_Part1.mzid.gz

                var fileToFind = DatasetName + "_msgfplus" + suffixToAdd + ".mzid.gz";
                var sourceDir = FileSearchTool.FindDataFile(fileToFind, true, false);
                string mzidSuffix;

                if (!string.IsNullOrEmpty(sourceDir))
                {
                    // Running MS-GF+ with gzipped results
                    mzidSuffix = ".mzid.gz";
                }
                else
                {
                    // File not found; look for DatasetName_msgfdb.mzid.gz or DatasetName_msgfdb_Part1.mzid.gz
                    var fileToGetAlternative = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(fileToFind, DatasetName + "_msgfdb.txt");
                    var mzidSourceDirAlt = FileSearchTool.FindDataFile(fileToGetAlternative, true, false);

                    if (!string.IsNullOrEmpty(mzidSourceDirAlt))
                    {
                        // Running MS-GF+ with gzipped results
                        mzidSuffix = ".mzid.gz";
                        sourceDir = mzidSourceDirAlt;
                    }
                    else
                    {
                        // File not found; look for a .zip file
                        var zipSourceDir = FileSearchTool.FindDataFile(DatasetName + "_msgfplus" + suffixToAdd + ".zip", true, false);

                        if (!string.IsNullOrEmpty(zipSourceDir))
                        {
                            // Running MS-GF+ with zipped results
                            mzidSuffix = ".zip";
                            sourceDir = zipSourceDir;
                        }
                        else
                        {
                            // File not found; try _msgfdb
                            var zipSourceDirAlt = FileSearchTool.FindDataFile(DatasetName + "_msgfdb" + suffixToAdd + ".zip", true, false);

                            if (!string.IsNullOrEmpty(zipSourceDirAlt))
                            {
                                // File Found
                                useLegacyMSGFDB = true;
                                mzidSuffix = ".zip";
                                sourceDir = zipSourceDirAlt;
                            }
                            else
                            {
                                // File not found; log a warning
                                LogWarning(
                                    "Could not find the _msgfplus.mzid.gz, _msgfplus.zip file, or the _msgfdb.zip file; assuming we're running MS-GF+");
                                mzidSuffix = ".mzid.gz";
                            }
                        }
                    }
                }

                var newestMzIdOrTsvFile = DateTime.MinValue;
                var ignorePepToProtMapErrors = false;

                for (var iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                {
                    var skipMSGFResultsZipFileCopy = false;
                    string baseName;

                    if (splitFastaEnabled)
                    {
                        suffixToAdd = "_Part" + iteration;
                    }
                    else
                    {
                        suffixToAdd = string.Empty;
                    }

                    if (useLegacyMSGFDB)
                    {
                        baseName = DatasetName + "_msgfdb";

                        if (splitFastaEnabled)
                        {
                            LogError("GetMSGFPlusFiles does not support SplitFasta mode for legacy MSGF-DB results");
                            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        }
                    }
                    else
                    {
                        baseName = DatasetName + "_msgfplus" + suffixToAdd;

                        var tsvFileName = baseName + ".tsv";
                        currentStep = "Retrieving " + tsvFileName;

                        var tsvSourceDir = FileSearchTool.FindDataFile(tsvFileName, false, false);

                        if (string.IsNullOrEmpty(tsvSourceDir))
                        {
                            var fileToGetAlternative = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(tsvFileName, DatasetName + "_msgfdb.txt");
                            var tsvSourceDirAlt = FileSearchTool.FindDataFile(fileToGetAlternative, false, false);

                            if (!string.IsNullOrEmpty(tsvSourceDirAlt))
                            {
                                tsvFileName = fileToGetAlternative;
                                tsvSourceDir = tsvSourceDirAlt;
                            }
                        }

                        if (!string.IsNullOrEmpty(tsvSourceDir))
                        {
                            if (!tsvSourceDir.StartsWith(MYEMSL_PATH_FLAG))
                            {
                                // Examine the date of the TSV file
                                // If less than 4 hours old, retrieve it; otherwise, grab the _msgfplus.mzid.gz file and re-generate the .tsv file

                                var tsvFile = new FileInfo(Path.Combine(tsvSourceDir, tsvFileName));

                                if (DateTime.UtcNow.Subtract(tsvFile.LastWriteTimeUtc).TotalHours < 4)
                                {
                                    // File is recent; grab it
                                    if (!CopyFileToWorkDir(tsvFileName, tsvSourceDir, mWorkDir))
                                    {
                                        // File copy failed; that's OK; we'll grab the _msgfplus.mzid.gz file
                                    }
                                    else
                                    {
                                        skipMSGFResultsZipFileCopy = true;
                                        mJobParams.AddResultFileToSkip(tsvFileName);

                                        if (tsvFile.LastWriteTimeUtc > newestMzIdOrTsvFile)
                                        {
                                            newestMzIdOrTsvFile = tsvFile.LastWriteTimeUtc;
                                        }
                                    }
                                }

                                mJobParams.AddServerFileToDelete(tsvFile.FullName);
                            }
                        }
                    }

                    // Retrieve the .mzid.gz file if skipMSGFResultsZipFileCopy is false
                    // or if extracting data from a SplitFASTA search (since we need to merge the .mzid files together)
                    if (!skipMSGFResultsZipFileCopy || splitFastaEnabled)
                    {
                        var mzidFile = baseName + mzidSuffix;
                        currentStep = "Retrieving " + mzidFile;

                        if (!FileSearchTool.FindAndRetrieveMiscFiles(mzidFile, unzip: true, searchArchivedDatasetDir: true, logFileNotFound: true))
                        {
                            // Errors were reported in method call, so just return
                            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        }
                        mJobParams.AddResultFileToSkip(mzidFile);

                        var mzidFileInfo = new FileInfo(Path.Combine(mWorkDir, mzidFile));

                        if (!mzidFileInfo.Exists)
                        {
                            LogError(string.Format("FileSearch.FindAndRetrieveMiscFiles returned true, but {0} was not found in the working directory", mzidFile));
                        }

                        if (mzidFileInfo.LastWriteTimeUtc > newestMzIdOrTsvFile)
                        {
                            newestMzIdOrTsvFile = mzidFileInfo.LastWriteTimeUtc;
                        }

                        // Also retrieve the console output file; the command line used to call the MzidToTsvConverter.exe will be appended to the file
                        // The console output file is not critical, so pass false to logFileNotFound
                        FileSearchTool.FindAndRetrieveMiscFiles(MSGFPlusUtils.MSGFPLUS_CONSOLE_OUTPUT_FILE, unzip: false, searchArchivedDatasetDir: true, logFileNotFound: false);
                    }

                    // Manually add several files to skip
                    if (splitFastaEnabled)
                    {
                        mJobParams.AddResultFileToSkip(DatasetName + "_msgfplus_Part" + iteration + ".txt");
                        mJobParams.AddResultFileToSkip(DatasetName + "_msgfplus_Part" + iteration + ".mzid");
                        mJobParams.AddResultFileToSkip(DatasetName + "_msgfplus_Part" + iteration + ".tsv");
                        mJobParams.AddResultFileToSkip(DatasetName + "_msgfdb_Part" + iteration + ".txt");
                        mJobParams.AddResultFileToSkip(DatasetName + "_msgfdb_Part" + iteration + ".tsv");
                    }
                    else
                    {
                        mJobParams.AddResultFileToSkip(DatasetName + "_msgfplus.txt");
                        mJobParams.AddResultFileToSkip(DatasetName + "_msgfplus.mzid");
                        mJobParams.AddResultFileToSkip(DatasetName + "_msgfplus.tsv");
                        mJobParams.AddResultFileToSkip(DatasetName + "_msgfdb.txt");
                        mJobParams.AddResultFileToSkip(DatasetName + "_msgfdb.tsv");
                    }

                    // Get the peptide to protein mapping file
                    var pepToProtMapFileName = baseName + "_PepToProtMap.txt";
                    currentStep = "Retrieving " + pepToProtMapFileName;

                    if (!FileSearchTool.FindAndRetrievePHRPDataFile(ref pepToProtMapFileName, synopsisFileName: "", addToResultFileSkipList: true, logFileNotFound: false))
                    {
                        // Errors were reported in method call

                        if (splitFastaEnabled && !ignorePepToProtMapErrors)
                        {
                            // If PHRP has already finished, separate PepToProtMap.txt files will not exist for each job step

                            var pepToProtMapSourceDir = FileSearchTool.FindDataFile(DatasetName + "_msgfplus_PepToProtMap.txt", false, false);

                            if (!string.IsNullOrEmpty(pepToProtMapSourceDir))
                            {
                                ignorePepToProtMapErrors = true;
                            }
                        }

                        if (!ignorePepToProtMapErrors)
                        {
                            // See if IgnorePeptideToProteinMapError is true
                            if (mJobParams.GetJobParameter("IgnorePeptideToProteinMapError", false))
                            {
                                LogWarning(
                                    "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' is true");
                            }
                            else if (mJobParams.GetJobParameter("SkipProteinMods", false))
                            {
                                LogWarning(
                                    "Ignoring missing _PepToProtMap.txt file since 'SkipProteinMods' is true");
                            }
                            else
                            {
                                if (useLegacyMSGFDB)
                                {
                                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                                }

                                // This class will auto-create the PepToProtMap.txt file after the FASTA file is retrieved
                                createPepToProtMapFile = true;
                            }
                        }
                    }
                    else
                    {
                        if (splitFastaEnabled && !string.IsNullOrWhiteSpace(sourceDir))
                        {
                            var pepToProtMapFile = new FileInfo(Path.Combine(sourceDir, pepToProtMapFileName));
                            mJobParams.AddServerFileToDelete(pepToProtMapFile.FullName);
                        }
                    }

                    if (splitFastaEnabled)
                    {
                        // Retrieve the _ConsoleOutput file for this cloned step

                        var consoleOutputFile = "MSGFPlus_ConsoleOutput" + suffixToAdd + ".txt";
                        currentStep = "Retrieving " + consoleOutputFile;

                        if (!FileSearchTool.FindAndRetrieveMiscFiles(consoleOutputFile, unzip: false,
                                                                 searchArchivedDatasetDir: true, logFileNotFound: false))
                        {
                            // This is not an important error; ignore it
                        }

                        mJobParams.AddResultFileToSkip(consoleOutputFile);
                    }

                    var subTaskProgress = iteration / (float)numberOfClonedSteps * 100;
                    var progressOverall = AnalysisToolRunnerBase.ComputeIncrementalProgress(0, ExtractToolRunner.PROGRESS_EXTRACTION_START, subTaskProgress);

                    if (DateTime.UtcNow.Subtract(lastProgressTime).TotalSeconds > 15)
                    {
                        lastProgressTime = DateTime.UtcNow;

                        mStatusTools.UpdateAndWrite(MgrStatusCodes.RUNNING, TaskStatusCodes.RUNNING, TaskStatusDetailCodes.RUNNING_TOOL,
                                                     progressOverall, 0, "", "", "", false);
                    }
                } // foreach cloned step

                if (newestMzIdOrTsvFile <= DateTime.MinValue)
                {
                    // No .mzid files were found; this indicates a problem
                    LogError("Did not find any .tsv or .mzid.gz files for this job");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Check whether PHRP has already complete successfully,
                // and whether the timestamp of the PHRP result files
                // is newer than the .mzid.gz file(s)

                var phrpResultsSourceDir = FileSearchTool.FindDataFile(DatasetName + "_msgfplus_fht.txt", false, false);

                if (string.IsNullOrWhiteSpace(phrpResultsSourceDir))
                {
                    // PHRP has not been run yet (which will typically be the case)
                    // Note that we'll obtain the MS-GF+ parameter file in RetrieveMiscFiles
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var phrpFilesToFind = new List<string>
                {
                    DatasetName + "_msgfplus_syn_ModDetails.txt",
                    DatasetName + "_msgfplus_syn_ModSummary.txt",
                    DatasetName + "_msgfplus_syn_ResultToSeqMap.txt",
                    DatasetName + "_msgfplus_syn_SeqInfo.txt",
                    DatasetName + "_msgfplus_syn_SeqToProteinMap.txt",
                    DatasetName + "_msgfplus_syn.txt",
                    DatasetName + "_msgfplus_fht.txt",
                    DatasetName + "_msgfplus_PepToProtMap.txt"
                };

                var oldestPhrpFile = DateTime.MaxValue;
                var existingPhrpFileCount = 0;

                foreach (var phrpFileName in phrpFilesToFind)
                {
                    var phrpFile = new FileInfo(Path.Combine(phrpResultsSourceDir, phrpFileName));

                    if (!phrpFile.Exists)
                    {
                        // File not found; need to run PHRP
                        continue;
                    }

                    existingPhrpFileCount++;

                    if (phrpFile.LastWriteTimeUtc < oldestPhrpFile)
                    {
                        oldestPhrpFile = phrpFile.LastWriteTimeUtc;
                    }
                }

                if (existingPhrpFileCount < phrpFilesToFind.Count)
                {
                    // One or more PHRP files was missing; we'll run PHRP
                    if (existingPhrpFileCount > 0)
                    {
                        LogMessage("Found {0} out of {1} existing PHRP files; will re-run PHRP", existingPhrpFileCount, phrpFilesToFind.Count);
                    }
                }
                else if (oldestPhrpFile > newestMzIdOrTsvFile)
                {
                    // PHRP files are up-to-date; no need to re-run PHRP
                    var fileLabel = splitFastaEnabled ? "files" : "file";

                    LogMessage(
                        "PHRP files are all newer than the .mzid.gz {0} ({1} > {2}); will skip running PHRP on this job",
                        fileLabel,
                        oldestPhrpFile.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT),
                        newestMzIdOrTsvFile.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));

                    mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, JOB_PARAM_SKIP_PHRP, true);
                }

                // Note that we'll obtain the MS-GF+ parameter file in RetrieveMiscFiles
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in GetMSGFPlusFiles at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType GetMSPathFinderFiles()
        {
            var fileToGet = DatasetName + "_IcTsv.zip";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            mJobParams.AddResultFileToSkip(fileToGet);
            mJobParams.AddResultFileExtensionToSkip(".tsv");

            // Note that we'll obtain the MSPathFinder parameter file in RetrieveMiscFiles
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetSEQUESTFiles()
        {
            // Get the concatenated .out file
            if (!FileSearchTool.RetrieveOutFiles(false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_NO_OUT_FILES;
            }

            // Note that we'll obtain the SEQUEST parameter file in RetrieveMiscFiles

            // Add all the extensions of the files to delete after run
            mJobParams.AddResultFileExtensionToSkip("_dta.zip");    // Zipped DTA
            mJobParams.AddResultFileExtensionToSkip("_dta.txt");    // Unzipped, concatenated DTA
            mJobParams.AddResultFileExtensionToSkip("_out.zip");    // Zipped OUT
            mJobParams.AddResultFileExtensionToSkip("_out.txt");    // Unzipped, concatenated OUT
            mJobParams.AddResultFileExtensionToSkip(".dta");        // DTA files
            mJobParams.AddResultFileExtensionToSkip(".out");        // DTA files

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetTopPICFiles()
        {
            var filesToGet = DatasetName + "*_TopPIC_PrSMs.txt";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(filesToGet, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var workingDirectory = new DirectoryInfo(mWorkDir);

            foreach (var item in workingDirectory.GetFiles(filesToGet))
            {
                mJobParams.AddResultFileToSkip(item.Name);
            }

            // Note that we'll obtain the TopPIC parameter file in RetrieveMiscFiles
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetXTandemFiles()
        {
            var fileToGet = DatasetName + "_xt.zip";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_NO_XT_FILES;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Manually adding this file to FilesToDelete; we don't want the unzipped .xml file to be copied to the server
            mJobParams.AddResultFileToSkip(DatasetName + "_xt.xml");

            // Note that we'll obtain the X!Tandem parameter file in RetrieveMiscFiles

            // However, we need to obtain the "input.xml" file and "default_input.xml" files now
            fileToGet = "input.xml";

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            if (!CopyFileToWorkDir("default_input.xml", mJobParams.GetParam("ParamFileStoragePath"), mWorkDir))
            {
                LogError("Failed retrieving default_input.xml file");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetCDTAFile()
        {
            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

            if (FileSearchTool.RetrieveDtaFiles())
                return CloseOutType.CLOSEOUT_SUCCESS;

            var sharedResultsFolders = mJobParams.GetParam(JOB_PARAM_SHARED_RESULTS_FOLDERS);

            if (string.IsNullOrEmpty(sharedResultsFolders))
            {
                mMessage = Global.AppendToComment(mMessage, "Job parameter SharedResultsFolders is empty");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (sharedResultsFolders.Contains(","))
            {
                mMessage = Global.AppendToComment(mMessage, "shared results folders: " + sharedResultsFolders);
            }
            else
            {
                mMessage = Global.AppendToComment(mMessage, "shared results folder " + sharedResultsFolders);
            }

            // Errors were reported in method call, so just return
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }

        /// <summary>
        /// Examine residuesToSearch to look for any residue in residuesToFind
        /// </summary>
        /// <param name="residuesToSearch">One letter amino acid symbols to search</param>
        /// <param name="residuesToFind">One letter amino acid symbols to find</param>
        /// <returns>True if residuesToSearch has any of the residues in residuesToFind</returns>
        private bool HasAnyResidue(string residuesToSearch, string residuesToFind)
        {
            foreach (var residue in residuesToFind)
            {
                if (residuesToSearch.Contains(residue))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Retrieves misc files needed for extraction
        /// (including the search engine parameter file, _ModDefs.txt and MassCorrectionTags.txt)
        /// </summary>
        /// <returns>CloseOutType specifying results</returns>
        private CloseOutType RetrieveMiscFiles(string resultTypeName)
        {
            var paramFileName = mJobParams.GetParam("ParamFileName");
            var modDefsFilename = Path.GetFileNameWithoutExtension(paramFileName) + MOD_DEFS_FILE_SUFFIX;

            try
            {
                // Call RetrieveGeneratedParamFile() now to re-create the parameter file, retrieve the _ModDefs.txt file,
                //   and retrieve the MassCorrectionTags.txt file
                // Although the ModDefs file should have been created when SEQUEST, X!Tandem, Inspect, MS-GF+, or MSAlign ran,
                //   we re-generate it here just in case T_Param_File_Mass_Mods had missing information
                // Furthermore, we need the search engine parameter file for the PHRPReader

                // Note that the _ModDefs.txt file is obtained using this query:
                //  SELECT Local_Symbol, Monoisotopic_Mass, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag, MaxQuant_Mod_Name
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                var success = RetrieveGeneratedParamFile(paramFileName);

                if (!success)
                {
                    LogError("Error retrieving parameter file and ModDefs.txt file");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                // Confirm that the file was actually created
                var modDefsFile = new FileInfo(Path.Combine(mWorkDir, modDefsFilename));

                if (!modDefsFile.Exists && resultTypeName.Equals(RESULT_TYPE_MSPATHFINDER))
                {
                    // MSPathFinder should have already created the ModDefs file during the previous step
                    // Retrieve it from the transfer directory now
                    FileSearchTool.FindAndRetrieveMiscFiles(modDefsFilename, false);
                    modDefsFile.Refresh();
                }

                if (resultTypeName.Equals(RESULT_TYPE_XTANDEM))
                {
                    // Retrieve the taxonomy.xml file (PHRPReader uses for it)
                    FileSearchTool.FindAndRetrieveMiscFiles("taxonomy.xml", false);
                }

                if (!modDefsFile.Exists && !resultTypeName.Equals(RESULT_TYPE_MSALIGN) && !resultTypeName.Equals(RESULT_TYPE_TOPPIC))
                {
                    mMessage = "Unable to create the ModDefs.txt file; update T_Param_File_Mass_Mods";
                    LogWarning("Unable to create the ModDefs.txt file; " +
                               "define the modifications in table T_Param_File_Mass_Mods for parameter file " + paramFileName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mJobParams.AddResultFileToSkip(paramFileName);
                mJobParams.AddResultFileToSkip(MASS_CORRECTION_TAGS_FILENAME);

                var logModFilesFileNotFound = resultTypeName.Equals(RESULT_TYPE_MSALIGN);

                // Check whether the newly generated ModDefs file matches the existing one
                // If it doesn't match, or if the existing one is missing, we need to keep the file
                // Otherwise, we can skip it
                var remoteModDefsDirectory = FileSearchTool.FindDataFile(
                    modDefsFilename,
                    searchArchivedDatasetDir: false,
                    logFileNotFound: logModFilesFileNotFound);

                var transferDirectory = mJobParams.GetParam(JOB_PARAM_TRANSFER_DIRECTORY_PATH);

                if (string.IsNullOrEmpty(remoteModDefsDirectory))
                {
                    // ModDefs file not found on the server
                    if (modDefsFile.Length == 0)
                    {
                        // File is empty; no point in keeping it
                        mJobParams.AddResultFileToSkip(modDefsFilename);
                    }
                }
                else if (remoteModDefsDirectory.StartsWith(transferDirectory, StringComparison.OrdinalIgnoreCase))
                {
                    if (Global.FilesMatch(modDefsFile.FullName, Path.Combine(remoteModDefsDirectory, modDefsFilename)))
                    {
                        mJobParams.AddResultFileToSkip(modDefsFilename);
                    }
                }

                // Examine the parameter file to check whether a phospho STY search was performed
                // If so, retrieve the instrument data file so that we can run AScore

                var runAScore = CheckAScoreRequired(resultTypeName, Path.Combine(mWorkDir, paramFileName));

                if (!runAScore)
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, JOB_PARAM_RUN_ASCORE, true);

                // If existing PHRP files were found and SkipPHRP was set to true,
                // assure that a .mzid or .mzid.gz file exists in the working directory
                // If one is not found, change SkipPHRP back to false
                var skipPHRPEnabled = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, JOB_PARAM_SKIP_PHRP, false);

                if (skipPHRPEnabled)
                {
                    var workDir = new DirectoryInfo(mWorkDir);

                    var mzidGzFiles = workDir.GetFiles(DatasetName + "*.mzid.gz");
                    var mzidFiles = workDir.GetFiles(DatasetName + "*.mzid");

                    if (mzidGzFiles.Length == 0 && mzidFiles.Length == 0)
                    {
                        LogWarning("Changing job parameter {0} back to false because no .mzid files were found", JOB_PARAM_SKIP_PHRP);
                        mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, JOB_PARAM_SKIP_PHRP, false);
                    }
                }

                LogMessage("TODO: Retrieve the instrument data file so that we can run AScore");
                return CloseOutType.CLOSEOUT_SUCCESS;

#pragma warning disable CS0162 // Unreachable code detected

                // Retrieve the instrument data file

                // The ToolName job parameter holds the name of the pipeline script we are executing
                var scriptName = mJobParams.GetParam("ToolName");

                if (scriptName.IndexOf("mzXML", StringComparison.OrdinalIgnoreCase) >= 0 || scriptName.IndexOf("msgfplus_bruker", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return GetMzXMLFile();
                }

                if (scriptName.IndexOf("mzML", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return GetMzMLFile();
                }

                return GetCDTAFile();
#pragma warning restore CS0162 // Unreachable code detected
            }
            catch (Exception ex)
            {
                LogError("Error retrieving miscellaneous files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void RetrieveToolVersionFile(string resultTypeName)
        {
            try
            {
                // Make sure the ResultType is valid
                var resultType = ReaderFactory.GetPeptideHitResultType(resultTypeName);

                if (resultType == PeptideHitResultTypes.Unknown)
                {
                    LogWarning("Result type is unknown; cannot retrieve the tool version file");
                    return;
                }

                var toolVersionUtility = new ToolVersionUtilities(mMgrParams, mJobParams, mJob, DatasetName, StepToolName, mDebugLevel, mWorkDir);
                RegisterEvents(toolVersionUtility);

                toolVersionUtility.RetrieveToolVersionInfoFile(FileSearchTool, resultType);
            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveToolVersionFile: " + ex.Message);
            }
        }
    }
}
