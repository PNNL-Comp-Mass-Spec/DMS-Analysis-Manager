using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using PRISMDatabaseUtils;

namespace AnalysisManagerDiaNNPlugIn
{
    /// <summary>
    /// Retrieve resources for the DIA-NN plugin
    /// </summary>
    public class AnalysisResourcesDiaNN : AnalysisResources
    {
        // Ignore Spelling: DIA, silico

        internal const string DIA_NN_SPEC_LIB_STEP_TOOL = "DiaNN_SpecLib";

        internal const string DIA_NN_STEP_TOOL = "DiaNN";

        /// <summary>
        /// This job parameter tracks the spectral library file ID
        /// </summary>
        internal const string SPECTRAL_LIBRARY_FILE_ID = "SpectralLibraryID";

        /// <summary>
        /// This job parameter tracks the remote spectral library file path
        /// </summary>
        /// <remarks>If a new spectral library, this step tool will copy the file to the remote path after it is created</remarks>
        internal const string SPECTRAL_LIBRARY_FILE_REMOTE_PATH_JOB_PARAM = "SpectralLibraryFileRemotePath";

        internal enum SpectralLibraryStatusCodes
        {
            /// <summary>
            /// Unknown status
            /// </summary>
            Unknown = 0,

            /// <summary>
            /// This job needs to create a new spectral library
            /// </summary>
            CreatingNewLibrary = 1,

            /// <summary>
            /// The spectral library already exists
            /// </summary>
            LibraryAlreadyCreated = 2,

            /// <summary>
            /// Another job is already creating the spectral library that this job needs
            /// </summary>
            CreationInProgressByOtherJob = 3,

            /// <summary>
            /// Indicates that the DIA-NN parameter file has a spectral library defined using option ExistingSpectralLibrary
            /// </summary>
            UsingExistingLibrary = 4,

            /// <summary>
            /// Found a spectral library with a failed state; a DB admin either needs to delete the row from t_spectral_library or change its state to 5
            /// </summary>
            FoundFailedLibrary = 5,

            /// <summary>
            /// Error determining the spectral library status
            /// </summary>
            Error = 6
        }

        private bool mBuildingSpectralLibrary;

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

                switch (StepToolName)
                {
                    case DIA_NN_SPEC_LIB_STEP_TOOL:
                        mBuildingSpectralLibrary = true;
                        break;

                    case DIA_NN_STEP_TOOL:
                        mBuildingSpectralLibrary = false;
                        break;

                    default:
                        LogError("Unrecognized step tool name: " + StepToolName);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                // The ToolName job parameter holds the name of the pipeline script we are executing
                var scriptName = mJobParams.GetParam("ToolName");


                // If the script is DiaNN or DiaNN_DataPkg, the input file is a .mzML file
                // If the script is DiaNN_timsTOF or DiaNN_timsTOF_DataPkg, the input file is the .tdf file in the .d directory

                var retrieveMsXmlFiles = !scriptName.StartsWith("DiaNN_timsTOF", StringComparison.OrdinalIgnoreCase);

                LogMessage("Retrieving the DIA-NN param file", 2);

                // Retrieve the parameter file
                // This will also obtain the _ModDefs.txt file using query
                //  SELECT Local_Symbol, Monoisotopic_Mass, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag, MaxQuant_Mod_Name, UniMod_Mod_Name
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                currentTask = "RetrieveGeneratedParamFile " + paramFileName;

                if (!RetrieveGeneratedParamFile(paramFileName))
                {
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                var paramFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));

                var options = new DiaNNOptions();
                RegisterEvents(options);

                options.LoadDiaNNOptions(paramFile.FullName);

                if (!options.ValidateDiaNNOptions(out var existingSpectralLibraryFile))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var jobStep = JobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);

                if (mBuildingSpectralLibrary && existingSpectralLibraryFile != null)
                {
                    // Skip this job step
                    LogMessage("Skipping step {0} for job {1} since using an existing spectral library: {2}", jobStep, mJob, existingSpectralLibraryFile.FullName);

                    EvalMessage = string.Format("Skipped step since using spectral library {0}", existingSpectralLibraryFile.FullName);

                    return CloseOutType.CLOSEOUT_SKIPPED_DIA_NN_SPEC_LIB;
                }

                // Retrieve the FASTA file
                var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;
                const int maxLegacyFASTASizeGB = 100;

                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode, maxLegacyFASTASizeGB, out _))
                    return resultCode;

                var remoteSpectralLibraryFile = GetRemoteSpectralLibraryFile(options, out var libraryStatusCode, out var spectralLibraryID, out var createNewLibrary);

                if (remoteSpectralLibraryFile == null)
                {
                    if (!string.IsNullOrWhiteSpace(mMessage))
                        return CloseOutType.CLOSEOUT_FAILED;

                    LogError(
                        createNewLibrary
                        ? "GetSpectralLibraryFile indicated that this job should create a new library, but the library file path is null"
                        : "GetSpectralLibraryFile returned a null value for the remote spectral library file");

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                switch (libraryStatusCode)
                {
                    case SpectralLibraryStatusCodes.Unknown or SpectralLibraryStatusCodes.Error:
                        if (string.IsNullOrWhiteSpace(mMessage))
                        {
                            LogError("Spectral library status code returned by GetRemoteSpectralLibraryFile is {0}", libraryStatusCode);
                        }

                        return CloseOutType.CLOSEOUT_FAILED;

                    case SpectralLibraryStatusCodes.FoundFailedLibrary:
                        const string msg = "Found a spectral library with a failed state; " +
                                           "a database admin either needs to delete the row from t_spectral_library or change its state to 5";

                        LogError(msg);
                        UpdateStatusMessage(msg);

                        return CloseOutType.CLOSEOUT_FAILED;

                    case SpectralLibraryStatusCodes.CreationInProgressByOtherJob:
                        EvalMessage = string.Format("Waiting for other job to create spectral library {0}", remoteSpectralLibraryFile.Name);

                        return CloseOutType.CLOSEOUT_WAITING_FOR_DIA_NN_SPEC_LIB;
                }

                if (spectralLibraryID == 0)
                {
                    if (!string.IsNullOrWhiteSpace(mMessage))
                        return CloseOutType.CLOSEOUT_FAILED;

                    LogError(string.Format("The spectral library ID for {0} is 0, which is invalid", remoteSpectralLibraryFile.Name));

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, SPECTRAL_LIBRARY_FILE_ID, spectralLibraryID);
                mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, SPECTRAL_LIBRARY_FILE_REMOTE_PATH_JOB_PARAM, remoteSpectralLibraryFile.FullName);

                if (mBuildingSpectralLibrary)
                {
                    // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
                    switch (libraryStatusCode)
                    {
                        case SpectralLibraryStatusCodes.CreatingNewLibrary:
                            // Do not retrieve any dataset files
                            return CloseOutType.CLOSEOUT_SUCCESS;

                        case SpectralLibraryStatusCodes.LibraryAlreadyCreated:
                            // Skip this job step
                            LogMessage("Skipping step {0} for job {1} since using a spectral library that was previously created: {2}", jobStep, mJob, remoteSpectralLibraryFile.FullName);

                            EvalMessage = string.Format("Skipped step since using spectral library {0}", remoteSpectralLibraryFile.FullName);

                            return CloseOutType.CLOSEOUT_SKIPPED_DIA_NN_SPEC_LIB;

                        default:
                            LogError(
                                "Unexpected library status code when mBuildingSpectralLibrary = true; {0}",
                                (int)libraryStatusCode);

                            return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                if (libraryStatusCode != SpectralLibraryStatusCodes.LibraryAlreadyCreated)
                {
                    LogError(
                        "Spectral library status code is {0} ({1}); expecting LibraryAlreadyCreated ({2})",
                        libraryStatusCode, (int)libraryStatusCode, (int)SpectralLibraryStatusCodes.LibraryAlreadyCreated);

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the spectral library file

                LogMessage("Copying spectral library from {0} to the working directory", remoteSpectralLibraryFile.FullName);

                var startTime = DateTime.UtcNow;

                var localSpectralLibraryFile = new FileInfo(Path.Combine(mWorkDir, remoteSpectralLibraryFile.Name));

                mFileTools.CopyFileUsingLocks(remoteSpectralLibraryFile.FullName, localSpectralLibraryFile.FullName);

                LogDebug("Spectral library file retrieved in {0:F2} seconds", DateTime.UtcNow.Subtract(startTime).TotalSeconds);

                mJobParams.AddResultFileToSkip(localSpectralLibraryFile.Name);

                var datasetFileRetriever = new DatasetFileRetriever(this);
                RegisterEvents(datasetFileRetriever);

                var datasetCopyResult = datasetFileRetriever.RetrieveInstrumentFilesForJobDatasets(
                    dataPackageID,
                    retrieveMsXmlFiles,
                    AnalysisToolRunnerDiaNN.PROGRESS_PCT_INITIALIZING,
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
                LogError("Error in GetResources (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private static int BoolToTinyInt(bool value)
        {
            return value ? 1 : 0;
        }

        /// <summary>
        /// Convert a list of modification definitions to a semicolon separated string
        /// </summary>
        /// <param name="modDefinitions">List of modification definitions</param>
        /// <param name="removeSpaces">If true, remove spaces</param>
        private string CollapseModifications(List<ModificationInfo> modDefinitions, bool removeSpaces = true)
        {
            var modificationList = new StringBuilder();

            foreach (var item in modDefinitions)
            {
                if (modificationList.Length > 0)
                    modificationList.Append("; ");

                modificationList.Append(item.ModificationDefinition);
            }

            return removeSpaces
                ? modificationList.ToString().Replace(" ", string.Empty)
                : modificationList.ToString();
        }

        private bool GetDiannVersion(out Version diannVersion)
        {
            try
            {
                var diaNNProgLoc = AnalysisToolRunnerBase.DetermineProgramLocation(
                    mMgrParams, mJobParams, StepToolName,
                    "DiaNNProgLoc", AnalysisToolRunnerDiaNN.DIA_NN_EXE_NAME,
                    out var errorMessage, out _);

                if (string.IsNullOrWhiteSpace(diaNNProgLoc))
                {
                    LogError("Error finding the DIA-NN executable ({0}): {1}", AnalysisToolRunnerDiaNN.DIA_NN_EXE_NAME, errorMessage);
                    diannVersion = null;
                    return false;
                }

                var diaNNVersionConsoleOutputFile = new FileInfo(Path.Combine(mWorkDir, "DiaNN_VersionInfo_ConsoleOutput.txt"));
                mJobParams.AddResultFileToSkip(diaNNVersionConsoleOutputFile.Name);

                var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = diaNNVersionConsoleOutputFile.FullName
                };

                RegisterEvents(cmdRunner);

                LogMessage("Determining DIA-NN version");

                LogDebug(diaNNProgLoc);

                // Start the program and wait for it to finish
                var processingSuccess = cmdRunner.RunProgram(diaNNProgLoc, string.Empty, "DIA-NN", true);

                if (!processingSuccess)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("Error determining DIA-NN version");
                    }

                    diannVersion = null;
                    return false;
                }

                diaNNVersionConsoleOutputFile.Refresh();
                if (!diaNNVersionConsoleOutputFile.Exists)
                {
                    LogError("Error determining DIA-NN version; console output file not found: {0}", diaNNVersionConsoleOutputFile.FullName);

                    diannVersion = null;
                    return false;
                }

                var reader = new StreamReader(new FileStream(diaNNVersionConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (true)
                {
                    var dataLine = reader.ReadLine();
                    if (dataLine == null)
                        break;

                    if (!dataLine.StartsWith("DIA-NN", StringComparison.OrdinalIgnoreCase))
                        continue;

                    var versionMatcher = new Regex("DIA-NN (?<Version>[0-9.]+)", RegexOptions.IgnoreCase);

                    var versionMatch = versionMatcher.Match(dataLine);

                    if (!versionMatch.Success)
                    {
                        LogError(string.Format("Unable to extract the DIA-NN version from \"{0}\" using the Regex", dataLine));
                        diannVersion = null;
                        return false;
                    }

                    diannVersion = new Version(versionMatch.Groups["Version"].ToString());
                    return true;
                }

                LogError(string.Format("Did not find a line that starts with DIA-NN in the console output from {0}", diaNNProgLoc));

                diannVersion = null;
                return false;
            }
            catch (Exception ex)
            {
                LogError("Error determining the version of DIA-NN on this computer", ex);
                diannVersion = null;
                return false;
            }
        }

        /// <summary>
        /// Determine the filename to use for the spectral library
        /// </summary>
        /// <remarks>
        /// <para>If options.ExistingSpectralLibrary has a filename defined, use it</para>
        /// <para>Otherwise, contact the database to determine the name to use (possibly creating a new spectral library)</para>
        /// </remarks>
        /// <param name="options">Options</param>
        /// <param name="libraryStatusCode">Output: spectral library status code</param>
        /// <param name="spectralLibraryID">Output: spectral library ID (from the database)</param>
        /// <param name="createNewLibrary">Output: true if this job should create the spectral library</param>
        /// <returns>FileInfo object, or null if an error</returns>
        private FileInfo GetRemoteSpectralLibraryFile(
            DiaNNOptions options,
            out SpectralLibraryStatusCodes libraryStatusCode,
            out int spectralLibraryID,
            out bool createNewLibrary)
        {
            if (!string.IsNullOrWhiteSpace(options.ExistingSpectralLibrary))
            {
                libraryStatusCode = SpectralLibraryStatusCodes.UsingExistingLibrary;
                spectralLibraryID = 0;
                createNewLibrary = false;

                var remoteSpectralLibraryFile = new FileInfo(options.ExistingSpectralLibrary);

                if (remoteSpectralLibraryFile.Exists)
                    return remoteSpectralLibraryFile;

                LogError("Spectral library file defined in the parameter file does not exist: " + remoteSpectralLibraryFile.FullName);
                return null;
            }

            // Determine the name to use for the in-silico based spectral library created using the
            // protein collections or legacy FASTA file associated with this job

            // The spectral library file depends on several settings tracked by options, including
            // the cleavage specificity, peptide lengths, m/z range, charge range, dynamic mods, and static mods

            if (mBuildingSpectralLibrary)
            {
                // Contact the database to determine whether an existing file exists,
                // whether a file is being created by another job, or whether this job should create the file

                return GetSpectraLibraryFromDB(options, true, out libraryStatusCode, out spectralLibraryID, out createNewLibrary);
            }

            // The DiaNN_SpecLib step (either for this job or for another job) should have already created the file
            // Contact the database to determine the spectral library file path

            return GetSpectraLibraryFromDB(options, false, out libraryStatusCode, out spectralLibraryID, out createNewLibrary);
        }

        /// <summary>
        /// Contact the database to determine if an existing spectral library exists, or if a new one needs to be created
        /// </summary>
        /// <param name="options">Options</param>
        /// <param name="allowCreateNewLibrary">True if this step tool can create a new spectral library, false if this step tool requires that a spectral library already exists</param>
        /// <param name="libraryStatusCode">Output: spectral library status code</param>
        /// <param name="spectralLibraryID">Output: spectral library ID (from the database)</param>
        /// <param name="createNewLibrary">Output: true if this job should create the spectral library</param>
        /// <returns>FileInfo instance for the remote spectral library file (the file will not exist if a new library); null if an error</returns>
        private FileInfo GetSpectraLibraryFromDB(
            DiaNNOptions options,
            bool allowCreateNewLibrary,
            out SpectralLibraryStatusCodes libraryStatusCode,
            out int spectralLibraryID,
            out bool createNewLibrary)
        {
            const string SP_NAME_GET_SPECTRAL_LIBRARY_ID = "get_spectral_library_id";

            try
            {
                // Determine the version of DIA-NN
                if (!GetDiannVersion(out var diannVersion))
                {
                    libraryStatusCode = SpectralLibraryStatusCodes.Error;
                    spectralLibraryID = 0;
                    createNewLibrary = false;
                    return null;
                }

                var diannNameAndVersion = string.Format("DIA-NN_{0}.{1}", diannVersion.Major, diannVersion.Minor);

                var proteinCollectionInfo = new ProteinCollectionInfo(mJobParams);

                // SQL Server: Data Source=Gigasax;Initial Catalog=DMS5
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                var dmsConnectionString = mMgrParams.GetParam("ConnectionString");

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, mMgrName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                RegisterEvents(dbTools);

                var dbServerType = DbToolsFactory.GetServerTypeFromConnectionString(dbTools.ConnectStr);

                var cmd = dbTools.CreateCommand(SP_NAME_GET_SPECTRAL_LIBRARY_ID, CommandType.StoredProcedure);

                if (dbServerType == DbServerTypes.MSSQLServer)
                    dbTools.AddParameter(cmd, "@allowAddNew", SqlType.Bit).Value = BoolToTinyInt(allowCreateNewLibrary);
                else
                    dbTools.AddParameter(cmd, "@allowAddNew", SqlType.Boolean).Value = allowCreateNewLibrary;

                dbTools.AddParameter(cmd, "@dmsSourceJob", SqlType.Int).Value = mJob;
                dbTools.AddParameter(cmd, "@proteinCollectionList", SqlType.VarChar, 2000).Value = proteinCollectionInfo.ProteinCollectionList;
                dbTools.AddParameter(cmd, "@organismDbFile", SqlType.VarChar, 128).Value = proteinCollectionInfo.LegacyFastaName;
                dbTools.AddParameter(cmd, "@fragmentIonMzMin", SqlType.Real).Value = (float)options.FragmentIonMzMin;
                dbTools.AddParameter(cmd, "@fragmentIonMzMax", SqlType.Real).Value = (float)options.FragmentIonMzMax;

                if (dbServerType == DbServerTypes.MSSQLServer)
                    dbTools.AddParameter(cmd, "@trimNTerminalMet", SqlType.Bit).Value = BoolToTinyInt(options.TrimNTerminalMethionine);
                else
                    dbTools.AddParameter(cmd, "@trimNTerminalMet", SqlType.Boolean).Value = options.TrimNTerminalMethionine;

                dbTools.AddParameter(cmd, "@cleavageSpecificity", SqlType.VarChar, 64).Value = options.CleavageSpecificity;
                dbTools.AddParameter(cmd, "@missedCleavages", SqlType.Int).Value = options.MissedCleavages;
                dbTools.AddParameter(cmd, "@peptideLengthMin", SqlType.Int).Value = options.PeptideLengthMin;
                dbTools.AddParameter(cmd, "@peptideLengthMax", SqlType.Int).Value = options.PeptideLengthMax;
                dbTools.AddParameter(cmd, "@precursorMzMin", SqlType.Real).Value = (float)options.PrecursorMzMin;
                dbTools.AddParameter(cmd, "@precursorMzMax", SqlType.Real).Value = (float)options.PrecursorMzMax;
                dbTools.AddParameter(cmd, "@precursorChargeMin", SqlType.Int).Value = options.PrecursorChargeMin;
                dbTools.AddParameter(cmd, "@precursorChargeMax", SqlType.Int).Value = options.PrecursorChargeMax;

                if (dbServerType == DbServerTypes.MSSQLServer)
                    dbTools.AddParameter(cmd, "@staticCysCarbamidomethyl", SqlType.Bit).Value = BoolToTinyInt(options.StaticCysCarbamidomethyl);
                else
                    dbTools.AddParameter(cmd, "@staticCysCarbamidomethyl", SqlType.Boolean).Value = options.StaticCysCarbamidomethyl;

                var staticMods = CollapseModifications(options.StaticModDefinitions);

                var dynamicMods = CollapseModifications(options.DynamicModDefinitions);

                dbTools.AddParameter(cmd, "@staticMods", SqlType.VarChar, 512).Value = staticMods;
                dbTools.AddParameter(cmd, "@dynamicMods", SqlType.VarChar, 512).Value = dynamicMods;
                dbTools.AddParameter(cmd, "@maxDynamicMods", SqlType.Int).Value = options.MaxDynamicModsPerPeptide;
                dbTools.AddParameter(cmd, "@programVersion", SqlType.VarChar, 64).Value = diannNameAndVersion;

                // Output parameters
                var libraryIdParam = dbTools.AddParameter(cmd, "@libraryId", SqlType.Int, ParameterDirection.InputOutput);
                var libraryStateIdParam = dbTools.AddParameter(cmd, "@libraryStateId", SqlType.Int, ParameterDirection.InputOutput);
                var libraryNameParam = dbTools.AddParameter(cmd, "@libraryName", SqlType.VarChar, 255, ParameterDirection.InputOutput);
                var storagePathParam = dbTools.AddParameter(cmd, "@storagePath", SqlType.VarChar, 255, ParameterDirection.InputOutput);

                var shouldMakeLibraryParam = dbTools.AddParameter(cmd, "@sourceJobShouldMakeLibrary", dbServerType == DbServerTypes.PostgreSQL ? SqlType.Boolean : SqlType.TinyInt, ParameterDirection.InputOutput);

                var messageParam = dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 255, ParameterDirection.InputOutput);
                var returnCodeParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.InputOutput);

                // Initialize the output parameter values (required for PostgreSQL)
                libraryIdParam.Value = 0;
                libraryStateIdParam.Value = 0;
                libraryNameParam.Value = string.Empty;
                storagePathParam.Value = string.Empty;

                shouldMakeLibraryParam.Value = dbServerType == DbServerTypes.PostgreSQL ? false : 0;

                messageParam.Value = string.Empty;
                returnCodeParam.Value = string.Empty;

                // Call the procedure
                var resCode = dbTools.ExecuteSP(cmd, out var errorMessage);

                var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

                if (resCode != 0 || returnCode != 0)
                {
                    if (resCode != 0 && returnCode == 0)
                    {
                        LogError("ExecuteSP() reported result code {0} calling {1}", resCode, SP_NAME_GET_SPECTRAL_LIBRARY_ID);
                    }
                    else
                    {
                        if (returnCode == 5225)
                        {
                            // messageParam.Value is of the form "Spectral library not found, and @allowAddNew is 0; not creating H_sapiens_UniProt_SPROT_2021-06-20_Tryp_Pig_Bov_93658B34.predicted.speclib"

                            // Spectral library not found, and the DiaNN job step is not allowed to create a new one
                            LogError(string.Format(
                                "Spectral library not found, and the {0} job step is not allowed to create a new one",
                                StepToolName));
                        }
                        else
                        {
                            LogError(
                                "Procedure {0} returned error code {1}{2}",
                                SP_NAME_GET_SPECTRAL_LIBRARY_ID, returnCodeParam.Value.CastDBVal<string>(),
                                string.IsNullOrWhiteSpace(errorMessage)
                                    ? string.Empty
                                    : ": " + errorMessage);
                        }
                    }

                    libraryStatusCode = SpectralLibraryStatusCodes.Error;
                    spectralLibraryID = 0;
                    createNewLibrary = false;
                    return null;
                }

                spectralLibraryID = Convert.ToInt32(libraryIdParam.Value);
                var libraryStateID = Convert.ToInt32(libraryStateIdParam.Value);
                var libraryName = Convert.ToString(libraryNameParam.Value);
                var storagePath = Convert.ToString(storagePathParam.Value);

                if (dbServerType == DbServerTypes.MSSQLServer)
                {
                    var shouldMakeLibrary = Convert.ToInt32(shouldMakeLibraryParam.Value);
                    createNewLibrary = shouldMakeLibrary > 0;
                }
                else
                {
                    createNewLibrary = Convert.ToBoolean(shouldMakeLibraryParam.Value);
                }

                switch (libraryStateID)
                {
                    case 1:
                        // New (no jobs have been assigned to create the library)
                        libraryStatusCode = SpectralLibraryStatusCodes.Unknown;
                        var warningMessage = string.Format("Spectral library {0} has library state {1}, meaning no processes are creating the library", libraryName, libraryStateID);
                        LogWarning(warningMessage);
                        EvalMessage = warningMessage;

                        break;

                    case 2:
                        // In Progress
                        libraryStatusCode = createNewLibrary
                            ? SpectralLibraryStatusCodes.CreatingNewLibrary
                            : SpectralLibraryStatusCodes.CreationInProgressByOtherJob;

                        break;

                    case 3:
                        // Complete
                        libraryStatusCode = SpectralLibraryStatusCodes.LibraryAlreadyCreated;
                        break;

                    case 4:
                        // Failed
                        libraryStatusCode = SpectralLibraryStatusCodes.FoundFailedLibrary;
                        break;

                    case 5:
                        // Inactive
                        libraryStatusCode = SpectralLibraryStatusCodes.Error;
                        break;

                    default:
                        // Unrecognized case
                        LogError(
                            "Procedure {0} returned an unrecognized library state of {1} for library {2}",
                            SP_NAME_GET_SPECTRAL_LIBRARY_ID, libraryStateID, libraryName);

                        libraryStatusCode = SpectralLibraryStatusCodes.Unknown;
                        return null;
                }

                if (string.IsNullOrWhiteSpace(libraryName))
                {
                    return null;
                }

                if (!string.IsNullOrWhiteSpace(storagePath))
                {
                    return new FileInfo(Path.Combine(storagePath, libraryName));
                }

                LogError(
                    "Procedure {0} returned library name {1} but an empty storage path",
                    SP_NAME_GET_SPECTRAL_LIBRARY_ID, libraryName);

                return null;
            }
            catch (Exception ex)
            {
                LogError("Error calling " + SP_NAME_GET_SPECTRAL_LIBRARY_ID, ex);

                libraryStatusCode = SpectralLibraryStatusCodes.Error;
                spectralLibraryID = 0;
                createNewLibrary = false;
                return null;
            }
        }
    }
}
