//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 09/22/2008
//
//*********************************************************************************************************

using AnalysisManagerBase;
using AnalysisManagerMSGFDBPlugIn;
using PHRPReader;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Manages retrieval of all files needed for data extraction
    /// </summary>
    public class AnalysisResourcesExtraction : AnalysisResources
    {
        // Ignore Spelling: Defs, diff, bioml, Parm, mgf, MODa, msgfdb, foreach, dta

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

        protected bool mRetrieveOrganismDB;

        // Keys are the original file name, values are the new name
        protected Dictionary<string, string> mPendingFileRenames;

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
        /// Examines the SEQUEST, X!Tandem, Inspect, or MS-GF+ param file to determine if ETD mode is enabled
        /// </summary>
        /// <param name="resultType"></param>
        /// <param name="searchToolParamFilePath"></param>
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

                case RESULT_TYPE_INSPECT:
                case RESULT_TYPE_MSALIGN:
                case RESULT_TYPE_MODA:
                case RESULT_TYPE_MODPLUS:
                case RESULT_TYPE_MSPATHFINDER:
                case RESULT_TYPE_TOPPIC:
                    LogDebug(string.Format("{0} does not support running AScore as part of data extraction", resultType));
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
                using (var reader = new StreamReader(new FileStream(searchToolParamFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
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
                                modDefParts= modDef.Substring(0, commentIndex).Trim().Split(',').ToList();
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

                var objParamFile = new XmlDocument
                {
                    PreserveWhitespace = true
                };
                objParamFile.Load(searchToolParamFilePath);

                if (objParamFile.DocumentElement == null)
                {
                    LogError("Error reading the X!Tandem param file: DocumentElement is null");
                    return false;
                }

                for (var settingIndex = 0; settingIndex <= 1; settingIndex++)
                {
                    var objSelectedNodes = settingIndex switch
                    {
                        0 => objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='residue, potential modification mass']"),
                        1 => objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='refine, potential modification mass']"),
                        _ => null
                    };

                    if (objSelectedNodes == null)
                    {
                        continue;
                    }

                    for (var matchIndex = 0; matchIndex <= objSelectedNodes.Count - 1; matchIndex++)
                    {
                        var xmlAttributes = objSelectedNodes.Item(matchIndex)?.Attributes;

                        // Make sure this node has an attribute named type with value "input"
                        var objAttributeNode = xmlAttributes?.GetNamedItem("type");

                        if (objAttributeNode == null)
                        {
                            // Node does not have an attribute named "type"
                            continue;
                        }

                        if (!string.Equals(objAttributeNode.Value, "input", StringComparison.OrdinalIgnoreCase))
                            continue;

                        // Valid node; examine its InnerText value
                        var modDefList = objSelectedNodes.Item(matchIndex)?.InnerText;
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
            // It will be changed to False if processing Inspect results and the _PepToProtMap.txt file is successfully retrieved
            mRetrieveOrganismDB = true;
            mPendingFileRenames = new Dictionary<string, string>();

            var resultType = mJobParams.GetParam("ResultType");

            // Get analysis results files
            if (GetInputFiles(resultType, out var createPepToProtMapFile) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Get misc files
            if (RetrieveMiscFiles(resultType) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            foreach (var entry in mPendingFileRenames)
            {
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, entry.Key));
                if (sourceFile.Exists)
                {
                    sourceFile.MoveTo(Path.Combine(mWorkDir, entry.Value));
                }
            }

            if (!mRetrieveOrganismDB)
                return CloseOutType.CLOSEOUT_SUCCESS;

            var skipProteinMods = mJobParams.GetJobParameter("SkipProteinMods", false);
            if (!skipProteinMods || createPepToProtMapFile)
            {
                // Examine the FASTA file size
                // If it is over 2 GB in size, do not retrieve the file, and force skipProteinMods to false
                const float MAX_LEGACY_FASTA_SIZE_GB = 2;

                // Retrieve the Fasta file; required to create the _ProteinMods.txt file
                var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");
                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode, MAX_LEGACY_FASTA_SIZE_GB, out var fastaFileSizeGB))
                {
                    if (fastaFileSizeGB >= MAX_LEGACY_FASTA_SIZE_GB)
                    {
                        mJobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SkipProteinMods", "true");
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }

                    return resultCode;
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieves input files needed for extraction
        /// </summary>
        /// <param name="resultType">String specifying type of analysis results input to extraction process</param>
        /// <param name="createPepToProtMapFile"></param>
        /// <returns>CloseOutType specifying results</returns>
        private CloseOutType GetInputFiles(string resultType, out bool createPepToProtMapFile)
        {
            createPepToProtMapFile = false;

            try
            {
                var inputFolderName = mJobParams.GetParam("inputFolderName");
                if (string.IsNullOrWhiteSpace(inputFolderName))
                {
                    LogError("Input_Folder is not defined for this job step (job parameter inputFolderName); cannot retrieve input files");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                CloseOutType result;
                switch (resultType)
                {
                    case RESULT_TYPE_SEQUEST:
                        result = GetSEQUESTFiles();
                        break;

                    case RESULT_TYPE_XTANDEM:
                        result = GetXTandemFiles();
                        break;

                    case RESULT_TYPE_INSPECT:
                        result = GetInspectFiles();
                        break;

                    case RESULT_TYPE_MSGFPLUS:
                        result = GetMSGFPlusFiles(out createPepToProtMapFile);
                        break;

                    case RESULT_TYPE_MSALIGN:
                        result = GetMSAlignFiles();
                        break;

                    case RESULT_TYPE_MODA:
                        result = GetMODaFiles();
                        break;

                    case RESULT_TYPE_MODPLUS:
                        result = GetMODPlusFiles();
                        break;

                    case RESULT_TYPE_MSPATHFINDER:
                        result = GetMSPathFinderFiles();
                        mJobParams.AddResultFileExtensionToSkip(".tsv");
                        break;

                    case RESULT_TYPE_TOPPIC:
                        result = GetTopPICFiles();
                        break;

                    default:
                        LogError("Invalid tool result type: " + resultType);
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                RetrieveToolVersionFile(resultType);
            }
            catch (Exception ex)
            {
                LogError("Error retrieving input files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetSEQUESTFiles()
        {
            // Get the concatenated .out file
            if (!FileSearch.RetrieveOutFiles(false))
            {
                // Errors were reported in function call, so just return
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

        private CloseOutType GetXTandemFiles()
        {
            var fileToGet = DatasetName + "_xt.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_XT_FILES;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Manually adding this file to FilesToDelete; we don't want the unzipped .xml file to be copied to the server
            mJobParams.AddResultFileToSkip(DatasetName + "_xt.xml");

            // Note that we'll obtain the X!Tandem parameter file in RetrieveMiscFiles

            // However, we need to obtain the "input.xml" file and "default_input.xml" files now
            fileToGet = "input.xml";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            if (!CopyFileToWorkDir("default_input.xml", mJobParams.GetParam("ParmFileStoragePath"), mWorkDir))
            {
                LogError("Failed retrieving default_input.xml file");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetInspectFiles()
        {
            // Get the zipped Inspect results files

            // This file contains the p-value filtered results
            var fileToGet = DatasetName + "_inspect.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_INSP_FILES;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // This file contains top hit for each scan (no filters)
            fileToGet = DatasetName + "_inspect_fht.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_NO_INSP_FILES;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Get the peptide to protein mapping file
            fileToGet = DatasetName + "_inspect_PepToProtMap.txt";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call

                // See if IgnorePeptideToProteinMapError=True
                if (mJobParams.GetJobParameter("IgnorePeptideToProteinMapError", false))
                {
                    LogWarning(
                        "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' = True");
                }
                else
                {
                    return CloseOutType.CLOSEOUT_NO_INSP_FILES;
                }
            }
            else
            {
                // The OrgDB (aka fasta file) is not required
                mRetrieveOrganismDB = false;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the Inspect parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMODaFiles()
        {
            var fileToGet = DatasetName + "_moda.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);
            mJobParams.AddResultFileExtensionToSkip("_moda.txt");

            fileToGet = DatasetName + "_mgf_IndexToScanMap.txt";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the MODa parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMODPlusFiles()
        {
            var fileToGet = DatasetName + "_modp.zip";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);
            mJobParams.AddResultFileExtensionToSkip("_modp.txt");

            // Delete the MSConvert_ConsoleOutput.txt and MODPlus_ConsoleOutput files that were in the zip file; we don't need them

            var diWorkDir = new DirectoryInfo(mWorkDir);
            var filesToDelete = new List<FileInfo>();

            filesToDelete.AddRange(diWorkDir.GetFiles("MODPlus_ConsoleOutput_Part*.txt"));
            filesToDelete.AddRange(diWorkDir.GetFiles("MSConvert_ConsoleOutput.txt"));
            filesToDelete.AddRange(diWorkDir.GetFiles("TDA_Plus_ConsoleOutput.txt"));

            foreach (var fiFile in filesToDelete)
            {
                fiFile.Delete();
            }

            // Note that we'll obtain the MODPlus parameter file in RetrieveMiscFiles

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
                // or for split fasta, DatasetName_msgfplus_Part1.mzid.gz

                var fileToFind = DatasetName + "_msgfplus" + suffixToAdd + ".mzid.gz";
                var sourceDir = FileSearch.FindDataFile(fileToFind, true, false);
                string mzidSuffix;
                if (!string.IsNullOrEmpty(sourceDir))
                {
                    // Running MS-GF+ with gzipped results
                    mzidSuffix = ".mzid.gz";
                }
                else
                {
                    // File not found; look for DatasetName_msgfdb.mzid.gz or DatasetName_msgfdb_Part1.mzid.gz
                    var fileToGetAlternative = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(fileToFind, DatasetName + "_msgfdb.txt");
                    var mzidSourceDirAlt = FileSearch.FindDataFile(fileToGetAlternative, true, false);

                    if (!string.IsNullOrEmpty(mzidSourceDirAlt))
                    {
                        // Running MS-GF+ with gzipped results
                        mzidSuffix = ".mzid.gz";
                        sourceDir = mzidSourceDirAlt;
                    }
                    else
                    {
                        // File not found; look for a .zip file
                        var zipSourceDir = FileSearch.FindDataFile(DatasetName + "_msgfplus" + suffixToAdd + ".zip", true, false);
                        if (!string.IsNullOrEmpty(zipSourceDir))
                        {
                            // Running MS-GF+ with zipped results
                            mzidSuffix = ".zip";
                            sourceDir = zipSourceDir;
                        }
                        else
                        {
                            // File not found; try _msgfdb
                            var zipSourceDirAlt = FileSearch.FindDataFile(DatasetName + "_msgfdb" + suffixToAdd + ".zip", true, false);
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

                        var tsvFile = baseName + ".tsv";
                        currentStep = "Retrieving " + tsvFile;

                        var tsvSourceDir = FileSearch.FindDataFile(tsvFile, false, false);
                        if (string.IsNullOrEmpty(tsvSourceDir))
                        {
                            var fileToGetAlternative = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(tsvFile, DatasetName + "_msgfdb.txt");
                            var tsvSourceDirAlt = FileSearch.FindDataFile(fileToGetAlternative, false, false);
                            if (!string.IsNullOrEmpty(tsvSourceDirAlt))
                            {
                                tsvFile = fileToGetAlternative;
                                tsvSourceDir = tsvSourceDirAlt;
                            }
                        }

                        if (!string.IsNullOrEmpty(tsvSourceDir))
                        {
                            if (!tsvSourceDir.StartsWith(MYEMSL_PATH_FLAG))
                            {
                                // Examine the date of the TSV file
                                // If less than 4 hours old, retrieve it; otherwise, grab the _msgfplus.mzid.gz file and re-generate the .tsv file

                                var fiTSVFile = new FileInfo(Path.Combine(tsvSourceDir, tsvFile));
                                if (DateTime.UtcNow.Subtract(fiTSVFile.LastWriteTimeUtc).TotalHours < 4)
                                {
                                    // File is recent; grab it
                                    if (!CopyFileToWorkDir(tsvFile, tsvSourceDir, mWorkDir))
                                    {
                                        // File copy failed; that's OK; we'll grab the _msgfplus.mzid.gz file
                                    }
                                    else
                                    {
                                        skipMSGFResultsZipFileCopy = true;
                                        mJobParams.AddResultFileToSkip(tsvFile);

                                        if (fiTSVFile.LastWriteTimeUtc > newestMzIdOrTsvFile)
                                        {
                                            newestMzIdOrTsvFile = fiTSVFile.LastWriteTimeUtc;
                                        }
                                    }
                                }

                                mJobParams.AddServerFileToDelete(fiTSVFile.FullName);
                            }
                        }
                    }

                    // Retrieve the .mzid.gz file if skipMSGFResultsZipFileCopy is false
                    // or if extracting data from a SplitFASTA search (since we need to merge the .mzid files together)
                    if (!skipMSGFResultsZipFileCopy || splitFastaEnabled)
                    {
                        var mzidFile = baseName + mzidSuffix;
                        currentStep = "Retrieving " + mzidFile;

                        if (!FileSearch.FindAndRetrieveMiscFiles(mzidFile, unzip: true,
                                                                 searchArchivedDatasetDir: true, logFileNotFound: true))
                        {
                            // Errors were reported in function call, so just return
                            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        }
                        mJobParams.AddResultFileToSkip(mzidFile);

                        var fiMzidFile = new FileInfo(Path.Combine(mWorkDir, mzidFile));
                        if (!fiMzidFile.Exists)
                        {
                            LogError(string.Format(
                                         "FileSearch.FindAndRetrieveMiscFiles returned true, but {0} was not found in the working directory", mzidFile));
                        }

                        if (fiMzidFile.LastWriteTimeUtc > newestMzIdOrTsvFile)
                        {
                            newestMzIdOrTsvFile = fiMzidFile.LastWriteTimeUtc;
                        }

                        // Also retrieve the ConsoleOutput file; the command line used to call the MzidToTsvConverter.exe will be appended to this file
                        // This file is not critical, so pass false to logFileNotFound
                        FileSearch.FindAndRetrieveMiscFiles(MSGFPlusUtils.MSGFPLUS_CONSOLE_OUTPUT_FILE, unzip: false,
                                                            searchArchivedDatasetDir: true, logFileNotFound: false);
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
                    var pepToProtMapFile = baseName + "_PepToProtMap.txt";
                    currentStep = "Retrieving " + pepToProtMapFile;

                    if (!FileSearch.FindAndRetrievePHRPDataFile(ref pepToProtMapFile, synopsisFileName: "", addToResultFileSkipList: true, logFileNotFound: false))
                    {
                        // Errors were reported in function call

                        if (splitFastaEnabled && !ignorePepToProtMapErrors)
                        {
                            // If PHRP has already finished, separate PepToProtMap.txt files will not exist for each job step

                            var pepToProtMapSourceDir = FileSearch.FindDataFile(DatasetName + "_msgfplus_PepToProtMap.txt", false, false);
                            if (!string.IsNullOrEmpty(pepToProtMapSourceDir))
                            {
                                ignorePepToProtMapErrors = true;
                            }
                        }

                        if (!ignorePepToProtMapErrors)
                        {
                            // See if IgnorePeptideToProteinMapError=True
                            if (mJobParams.GetJobParameter("IgnorePeptideToProteinMapError", false))
                            {
                                LogWarning(
                                    "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' = True");
                            }
                            else if (mJobParams.GetJobParameter("SkipProteinMods", false))
                            {
                                LogWarning(
                                    "Ignoring missing _PepToProtMap.txt file since 'SkipProteinMods' = True");
                            }
                            else
                            {
                                if (useLegacyMSGFDB)
                                {
                                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                                }

                                // This class will auto-create the PepToProtMap.txt file after the fasta file is retrieved
                                createPepToProtMapFile = true;
                            }
                        }
                    }
                    else
                    {
                        if (splitFastaEnabled && !string.IsNullOrWhiteSpace(sourceDir))
                        {
                            var fiPepToProtMapFile = new FileInfo(Path.Combine(sourceDir, pepToProtMapFile));
                            mJobParams.AddServerFileToDelete(fiPepToProtMapFile.FullName);
                        }
                    }

                    if (splitFastaEnabled)
                    {
                        // Retrieve the _ConsoleOutput file for this cloned step

                        var consoleOutputFile = "MSGFPlus_ConsoleOutput" + suffixToAdd + ".txt";
                        currentStep = "Retrieving " + consoleOutputFile;

                        if (!FileSearch.FindAndRetrieveMiscFiles(consoleOutputFile, unzip: false,
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

                var phrpResultsSourceDir = FileSearch.FindDataFile(DatasetName + "_msgfplus_fht.txt", false, false);
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
                        LogMessage(string.Format("Found {0} out of {1} existing PHRP files; will re-run PHRP",
                                                 existingPhrpFileCount, phrpFilesToFind.Count));
                    }
                }
                else if (oldestPhrpFile > newestMzIdOrTsvFile)
                {
                    // PHRP files are up-to-date; no need to re-run PHRP
                    var fileLabel = splitFastaEnabled ? "files" : "file";

                    LogMessage(string.Format("PHRP files are all newer than the .mzid.gz {0} ({1} > {2}); will skip running PHRP on this job",
                                             fileLabel,
                                             oldestPhrpFile.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT),
                                             newestMzIdOrTsvFile.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT)));

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

        private CloseOutType GetMSAlignFiles()
        {
            var fileToGet = DatasetName + "_MSAlign_ResultTable.txt";
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the MSAlign parameter file in RetrieveMiscFiles

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMSPathFinderFiles()
        {
            var fileToGet = DatasetName + "_IcTsv.zip";

            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            mJobParams.AddResultFileToSkip(fileToGet);
            mJobParams.AddResultFileExtensionToSkip(".tsv");

            // Note that we'll obtain the MSPathFinder parameter file in RetrieveMiscFiles
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetTopPICFiles()
        {
            var fileToGet = DatasetName + "_TopPIC_PrSMs.txt";

            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            mJobParams.AddResultFileToSkip(fileToGet);

            // Note that we'll obtain the TopPIC parameter file in RetrieveMiscFiles
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetCDTAFile()
        {
            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

            if (FileSearch.RetrieveDtaFiles())
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

            // Errors were reported in function call, so just return
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }

        /// <summary>
        /// Examine residuesToSearch to look for any residue in residuesToFind
        /// </summary>
        /// <param name="residuesToSearch"></param>
        /// <param name="residuesToFind"></param>
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
        protected internal CloseOutType RetrieveMiscFiles(string resultType)
        {
            var paramFileName = mJobParams.GetParam("ParmFileName");
            var modDefsFilename = Path.GetFileNameWithoutExtension(paramFileName) + MOD_DEFS_FILE_SUFFIX;

            try
            {
                // Call RetrieveGeneratedParamFile() now to re-create the parameter file, retrieve the _ModDefs.txt file,
                //   and retrieve the MassCorrectionTags.txt file
                // Although the ModDefs file should have been created when SEQUEST, X!Tandem, Inspect, MS-GF+, or MSAlign ran,
                //   we re-generate it here just in case T_Param_File_Mass_Mods had missing information
                // Furthermore, we need the search engine parameter file for the PHRPReader

                // Note that the _ModDefs.txt file is obtained using this query:
                //  SELECT Local_Symbol, Monoisotopic_Mass, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                var success = RetrieveGeneratedParamFile(paramFileName);

                if (!success)
                {
                    LogError("Error retrieving parameter file and ModDefs.txt file");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                // Confirm that the file was actually created
                var fiModDefsFile = new FileInfo(Path.Combine(mWorkDir, modDefsFilename));

                if (!fiModDefsFile.Exists && resultType == RESULT_TYPE_MSPATHFINDER)
                {
                    // MSPathFinder should have already created the ModDefs file during the previous step
                    // Retrieve it from the transfer directory now
                    FileSearch.FindAndRetrieveMiscFiles(modDefsFilename, false);
                    fiModDefsFile.Refresh();
                }

                if (resultType == RESULT_TYPE_XTANDEM)
                {
                    // Retrieve the taxonomy.xml file (PHRPReader uses for it)
                    FileSearch.FindAndRetrieveMiscFiles("taxonomy.xml", false);
                }

                if (!fiModDefsFile.Exists && resultType != RESULT_TYPE_MSALIGN && resultType != RESULT_TYPE_TOPPIC)
                {
                    mMessage = "Unable to create the ModDefs.txt file; update T_Param_File_Mass_Mods";
                    LogWarning("Unable to create the ModDefs.txt file; " +
                               "define the modifications in table T_Param_File_Mass_Mods for parameter file " + paramFileName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mJobParams.AddResultFileToSkip(paramFileName);
                mJobParams.AddResultFileToSkip(MASS_CORRECTION_TAGS_FILENAME);

                var logModFilesFileNotFound = (resultType == RESULT_TYPE_MSALIGN);

                // Check whether the newly generated ModDefs file matches the existing one
                // If it doesn't match, or if the existing one is missing, we need to keep the file
                // Otherwise, we can skip it
                var remoteModDefsDirectory = FileSearch.FindDataFile(modDefsFilename,
                                                                     searchArchivedDatasetDir: false,
                                                                     logFileNotFound: logModFilesFileNotFound);
                if (string.IsNullOrEmpty(remoteModDefsDirectory))
                {
                    // ModDefs file not found on the server
                    if (fiModDefsFile.Length == 0)
                    {
                        // File is empty; no point in keeping it
                        mJobParams.AddResultFileToSkip(modDefsFilename);
                    }
                }
                else if (remoteModDefsDirectory.StartsWith(@"\\proto", StringComparison.OrdinalIgnoreCase))
                {
                    if (Global.FilesMatch(fiModDefsFile.FullName, Path.Combine(remoteModDefsDirectory, modDefsFilename)))
                    {
                        mJobParams.AddResultFileToSkip(modDefsFilename);
                    }
                }

                // Examine the parameter file to check whether a phospho STY search was performed
                // If so, retrieving the instrument data file so that we can run AScore

                var runAScore = CheckAScoreRequired(resultType, Path.Combine(mWorkDir, paramFileName));

                if (!runAScore)
                    return CloseOutType.CLOSEOUT_SUCCESS;

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
                        LogWarning(string.Format("Changing job parameter {0} back to False because no .mzid files were found",
                                                 JOB_PARAM_SKIP_PHRP));
                        mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, JOB_PARAM_SKIP_PHRP, false);
                    }
                }

                LogMessage("TODO: Retrieve the instrument data file so that we can run AScore");
                return CloseOutType.CLOSEOUT_SUCCESS;

#pragma warning disable CS0162 // Unreachable code detected

                // Retrieve the instrument data file

                // The ToolName job parameter holds the name of the job script we are executing
                var scriptName = mJobParams.GetParam("ToolName");

                if (scriptName.IndexOf("mzxml", StringComparison.OrdinalIgnoreCase) >= 0 || scriptName.IndexOf("msgfplus_bruker", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return GetMzXMLFile();
                }

                if (scriptName.IndexOf("mzml", StringComparison.OrdinalIgnoreCase) >= 0)
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

        protected bool RetrieveToolVersionFile(string resultTypeName)
        {
            bool success;

            try
            {
                // Make sure the ResultType is valid
                var resultType = clsPHRPReader.GetPeptideHitResultType(resultTypeName);

                var toolVersionFile = clsPHRPReader.GetToolVersionInfoFilename(resultType);
                var toolVersionFileNewName = string.Empty;

                var toolNameForScript = mJobParams.GetJobParameter("ToolName", string.Empty);
                if (resultType == clsPHRPReader.PeptideHitResultTypes.MSGFPlus && toolNameForScript == "MSGFPlus_IMS")
                {
                    // PeptideListToXML expects the ToolVersion file to be named "Tool_Version_Info_MSGFPlus.txt"
                    // However, this is the MSGFPlus_IMS script, so the file is currently "Tool_Version_Info_MSGFPlus_IMS.txt"
                    // We'll copy the current file locally, then rename it to the expected name
                    toolVersionFileNewName = string.Copy(toolVersionFile);
                    toolVersionFile = "Tool_Version_Info_MSGFPlus_IMS.txt";
                }

                success = FileSearch.FindAndRetrieveMiscFiles(toolVersionFile, false, false);

                if (success && !string.IsNullOrEmpty(toolVersionFileNewName))
                {
                    mPendingFileRenames.Add(toolVersionFile, toolVersionFileNewName);

                    toolVersionFile = toolVersionFileNewName;
                }
                else if (!success)
                {
                    if (toolVersionFile.IndexOf("msgfplus", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        const string toolVersionFileLegacy = "Tool_Version_Info_MSGFDB.txt";
                        success = FileSearch.FindAndRetrieveMiscFiles(toolVersionFileLegacy, false, false);
                        if (success)
                        {
                            // Rename the Tool_Version file to the expected name (Tool_Version_Info_MSGFPlus.txt)
                            mPendingFileRenames.Add(toolVersionFileLegacy, toolVersionFile);
                            mJobParams.AddResultFileToSkip(toolVersionFileLegacy);
                        }
                    }
                }

                mJobParams.AddResultFileToSkip(toolVersionFile);
            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveToolVersionFile: " + ex.Message);
                return false;
            }

            return success;
        }
    }
}
