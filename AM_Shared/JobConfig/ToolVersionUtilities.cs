using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.FileAndDirectoryTools;
using PHRPReader;
using PRISM;
using PRISMDatabaseUtils;

// ReSharper disable UnusedMember.Global

namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// Methods for tracking versions of executables and DLLs
    /// </summary>
    public class ToolVersionUtilities : EventNotifier
    {
        // Ignore Spelling: loc, MSConvert, msgfplus, prog, yyyy-MM-dd hh:mm:ss tt

        /// <summary>
        /// MSConvert executable name, capitalized
        /// </summary>
        public const string MSCONVERT_EXE_NAME = "MSConvert.exe";

        /// <summary>
        /// Stored procedure name for storing the step tool version
        /// </summary>
        private const string SP_NAME_SET_TASK_TOOL_VERSION = "set_step_task_tool_version";

        /// <summary>
        /// Used for tool version info files
        /// </summary>
        public const string TOOL_VERSION_INFO_PREFIX = "Tool_Version_Info_";

        /// <summary>
        /// Lines of text in the Tool_Version_Info file after this line indicate tool versions
        /// </summary>
        public const string TOOL_VERSION_INFO_SECTION_HEADER = "ToolVersionInfo:";

        /// <summary>
        /// Access to the job parameters
        /// </summary>
        private readonly IJobParams mJobParams;

        /// <summary>
        /// Access to manager parameters
        /// </summary>
        private readonly IMgrParams mMgrParams;

        /// <summary>
        /// Dataset name
        /// </summary>
        private string Dataset { get; }

        /// <summary>
        /// Debug level
        /// </summary>
        /// <remarks>Ranges from 0 (minimum output) to 5 (max detail)</remarks>
        private short DebugLevel { get; }

        /// <summary>
        /// Job number
        /// </summary>
        private int Job { get; }

        /// <summary>
        /// Step tool name
        /// </summary>
        public string StepToolName { get; }

        /// <summary>
        /// Tool version info file
        /// </summary>
        public string ToolVersionInfoFile => TOOL_VERSION_INFO_PREFIX + StepToolName + ".txt";

        /// <summary>
        /// Working directory
        /// </summary>
        private string WorkDir { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="job">Job number</param>
        /// <param name="dataset">Dataset name</param>
        /// <param name="stepToolName">Step tool name</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <param name="workDir">Working directory</param>
        public ToolVersionUtilities(IMgrParams mgrParams, IJobParams jobParams, int job, string dataset, string stepToolName, short debugLevel, string workDir)
        {
            mMgrParams = mgrParams;
            mJobParams = jobParams;

            Job = job;
            Dataset = dataset;
            StepToolName = stepToolName;
            DebugLevel = debugLevel;

            WorkDir = workDir;
        }

        /// <summary>
        /// Determines the version of MSConvert.exe
        /// </summary>
        /// <param name="msConvertPath">Full path to msconvert.exe</param>
        /// <param name="versionInfo">Output: version info</param>
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once InconsistentNaming
        public bool GetMSConvertToolVersion(string msConvertPath, out string versionInfo)
        {
            const string MSCONVERT_CONSOLE_OUTPUT = "MSConvert_ConsoleOutput.txt";

            versionInfo = string.Empty;

            try
            {
                var msConvertInfo = new FileInfo(msConvertPath);

                if (!msConvertInfo.Exists)
                {
                    OnWarningEvent("File not found by GetMSConvertToolVersion: " + msConvertPath);
                    return false;
                }

                mJobParams.AddResultFileToSkip(MSCONVERT_CONSOLE_OUTPUT);

                var progRunner = new RunDosProgram(Global.GetAppDirectoryPath(), DebugLevel)
                {
                    CacheStandardOutput = false,
                    CreateNoWindow = true,
                    EchoOutputToConsole = false,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(WorkDir, MSCONVERT_CONSOLE_OUTPUT),
                    MonitorInterval = 250,
                };

                progRunner.DebugEvent += OnDebugEvent;
                progRunner.StatusEvent += OnStatusEvent;
                progRunner.WarningEvent += OnWarningEvent;

                // MSConvert releases prior to August 2023 showed program syntax via the console error stream (when MSConvert is called with --help)
                // "progRunner.RaiseConsoleErrorEvents = false" and " progRunner.SkipConsoleWriteIfNoErrorListener = true" were previously used to treat error messages as normal messages
                // (then the using loop below would examine progRunner.CachedConsoleErrors)

                // Commit https://github.com/ProteoWizard/pwiz/commit/de117c0441f7858275fc69342972f4482d810d92 changed the behavior
                // such that MSConvert --help writes to the normal console output, but running the program without any arguments will still use the error stream

                const string args = "--help";

                var success = progRunner.RunProgram(msConvertInfo.FullName, args, "MSConvert", false);

                if (!success)
                {
                    return false;
                }

                // Read the console output and look for the ProteoWizard version and Build Date

                using (var reader = new StreamReader(new FileStream(progRunner.ConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (reader.Peek() >= 0)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (dataLine.StartsWith("ProteoWizard release", StringComparison.OrdinalIgnoreCase))
                        {
                            // ProteoWizard release: 3.0.18145 (f23158c8f)
                            versionInfo = Global.AppendToComment(versionInfo, dataLine);
                        }
                        else if (dataLine.StartsWith("Build date", StringComparison.OrdinalIgnoreCase))
                        {
                            // Add the executable name to the build date text, giving, for example:
                            // MSConvert Build date: May 24 2018 22:22:11
                            var exeNameAndBuildDate = Path.GetFileNameWithoutExtension(MSCONVERT_EXE_NAME) + " " + dataLine;

                            versionInfo = Global.AppendToComment(versionInfo, exeNameAndBuildDate);
                        }
                    }
                }

                if (!string.IsNullOrWhiteSpace(versionInfo))
                    return true;

                // Did not find ProteoWizard release info in the help text from msconvert.exe
                OnErrorEvent("Did not find ProteoWizard release info in the help text from " + Path.GetFileName(msConvertPath));
                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception determining MSConvert version of " + msConvertPath, ex);
                return false;
            }
        }

        /// <summary>
        /// Extracts the contents of the Version= line in a Tool Version Info file
        /// </summary>
        /// <param name="binaryFilePath">Executable or DLL file path</param>
        /// <param name="versionInfoFilePath">Version info file path</param>
        /// <param name="version">Output: binary file version</param>
        private bool ReadVersionInfoFile(string binaryFilePath, string versionInfoFilePath, out string version)
        {
            version = string.Empty;
            var success = false;

            try
            {
                if (!File.Exists(versionInfoFilePath))
                {
                    OnWarningEvent("Version Info File not found: " + versionInfoFilePath);
                    return false;
                }

                // Open versionInfoFilePath and read the Version= line
                using var reader = new StreamReader(new FileStream(versionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    var equalsLoc = dataLine.IndexOf('=');

                    if (equalsLoc <= 0)
                        continue;

                    var key = dataLine.Substring(0, equalsLoc);
                    string value;

                    if (equalsLoc < dataLine.Length)
                    {
                        value = dataLine.Substring(equalsLoc + 1);
                    }
                    else
                    {
                        value = string.Empty;
                    }

                    switch (key.ToLower())
                    {
                        case "filename":
                        case "path":
                            // Ignore these
                            break;

                        case "version":
                            version = value;

                            if (string.IsNullOrWhiteSpace(version))
                            {
                                OnErrorEvent("Empty version line in Version Info file for " + Path.GetFileName(binaryFilePath));
                                success = false;
                            }
                            else
                            {
                                success = true;
                            }
                            break;

                        case "error":
                            OnErrorEvent("Error reported by DLLVersionInspector for " + Path.GetFileName(binaryFilePath) + ": " + value);
                            success = false;
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error reading Version Info File for " + Path.GetFileName(binaryFilePath), ex);
            }

            return success;
        }

        /// <summary>
        /// Retrieve the tool version info file for the given peptide hit result type
        /// </summary>
        /// <param name="fileSearchUtility">File search utility</param>
        /// <param name="resultType">Peptide hit result type</param>
        /// <returns>True if the file is found and successfully copied, otherwise false</returns>
        public bool RetrieveToolVersionInfoFile(FileSearch fileSearchUtility, PeptideHitResultTypes resultType)
        {
            try
            {
                if (resultType == PeptideHitResultTypes.Unknown)
                {
                    OnErrorEvent("Cannot retrieve a ToolVersionInfo file when the peptide hit result type is Unknown");
                    return false;
                }

                var toolVersionFilenames = ReaderFactory.GetToolVersionInfoFilenames(resultType);

                if (toolVersionFilenames.Count == 0)
                {
                    OnErrorEvent("Error in RetrieveToolVersionInfoFile: GetToolVersionInfoFilenames returned an empty list for result type " + resultType);
                    return false;
                }

                var toolVersionFileNewName = string.Empty;

                // The ToolName job parameter holds the name of the pipeline script we are executing
                var toolNameForScript = mJobParams.GetJobParameter("ToolName", string.Empty);

                if (resultType == PeptideHitResultTypes.MSGFPlus && toolNameForScript == "MSGFPlus_IMS")
                {
                    // PeptideListToXML expects the ToolVersion file to be named "Tool_Version_Info_MSGFPlus.txt"
                    // However, this is the MSGFPlus_IMS script, so the file is currently "Tool_Version_Info_MSGFPlus_IMS.txt"
                    // We'll copy the current file locally, then rename it to the expected name
                    toolVersionFileNewName = toolVersionFilenames[0];
                    toolVersionFilenames[0] = "Tool_Version_Info_MSGFPlus_IMS.txt";
                }

                var toolVersionFileMatched = string.Empty;

                foreach (var candidateToolVersionFile in toolVersionFilenames)
                {
                    var success = fileSearchUtility.FindAndRetrieveMiscFiles(candidateToolVersionFile, unzip: false, searchArchivedDatasetDir: false, logFileNotFound: false);

                    if (!success)
                    {
                        continue;
                    }

                    toolVersionFileMatched = candidateToolVersionFile;

                    if (string.IsNullOrEmpty(toolVersionFileNewName))
                    {
                        break;
                    }

                    var sourceFile = new FileInfo(Path.Combine(WorkDir, candidateToolVersionFile));
                    sourceFile.MoveTo(Path.Combine(WorkDir, toolVersionFileNewName));

                    toolVersionFileMatched = toolVersionFileNewName;
                    break;
                }

                if (string.IsNullOrEmpty(toolVersionFileMatched) && toolVersionFilenames[0].IndexOf("msgfplus", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    const string toolVersionFileLegacy = "Tool_Version_Info_MSGFDB.txt";
                    var success = fileSearchUtility.FindAndRetrieveMiscFiles(toolVersionFileLegacy, false, false);

                    if (success)
                    {
                        // Rename the Tool_Version file to the expected name (Tool_Version_Info_MSGFPlus.txt)
                        var sourceFile = new FileInfo(Path.Combine(WorkDir, toolVersionFileLegacy));
                        sourceFile.MoveTo(Path.Combine(WorkDir, toolVersionFilenames[0]));

                        mJobParams.AddResultFileToSkip(toolVersionFileLegacy);
                        toolVersionFileMatched = toolVersionFilenames[0];
                    }
                }

                if (string.IsNullOrWhiteSpace(toolVersionFileMatched))
                {
                    OnWarningEvent("Warning: Tool version info file not found: " + toolVersionFilenames[0]);
                    return false;
                }

                mJobParams.AddResultFileToSkip(toolVersionFileMatched);
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RetrieveToolVersionInfoFile: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Creates a Tool Version Info file
        /// </summary>
        /// <param name="directoryPath">Directory path</param>
        /// <param name="toolVersionInfo">Tool version info string</param>
        /// <param name="stepToolNameOverride">Optional step tool name to use, instead of the default (which is "Tool_Version_Info_" + StepToolName + ".txt")</param>
        public void SaveToolVersionInfoFile(string directoryPath, string toolVersionInfo, string stepToolNameOverride = "")
        {
            try
            {
                string toolVersionFileName;

                if (string.IsNullOrWhiteSpace(stepToolNameOverride))
                {
                    toolVersionFileName = ToolVersionInfoFile;
                }
                else
                {
                    toolVersionFileName = TOOL_VERSION_INFO_PREFIX + stepToolNameOverride + ".txt";
                }

                var toolVersionFilePath = Path.Combine(directoryPath, toolVersionFileName);

                using var writer = new StreamWriter(new FileStream(toolVersionFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                writer.WriteLine("Date: " + DateTime.Now.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));
                writer.WriteLine("Dataset: " + Dataset);
                writer.WriteLine("Job: " + Job);
                writer.WriteLine("Step: " + mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Step"));
                writer.WriteLine("Tool: " + mJobParams.GetParam("StepTool"));
                writer.WriteLine(TOOL_VERSION_INFO_SECTION_HEADER);

                writer.WriteLine(toolVersionInfo.Replace("; ", Environment.NewLine));
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception saving tool version info", ex);
            }
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        /// <param name="toolVersionInfo">Version info string (maximum length is 900 characters on SQL Server)</param>
        /// <param name="toolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <param name="saveToolVersionTextFile">If true, creates a text file with the tool version information</param>
        /// <returns>True if success, false if an error</returns>
        public bool SetStepTaskToolVersion(string toolVersionInfo, IEnumerable<FileInfo> toolFiles, bool saveToolVersionTextFile = true)
        {
            var exeInfo = string.Empty;
            string toolVersionInfoCombined;

            if (toolFiles != null)
            {
                foreach (var toolFile in toolFiles)
                {
                    try
                    {
                        if (toolFile.Exists)
                        {
                            // Note: the TopFD plugin expects the tool version to be in the format
                            // program.exe: yyyy-MM-dd hh:mm:ss tt
                            var toolFileNameAndDate = string.Format("{0}: {1}",
                                toolFile.Name, toolFile.LastWriteTime.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));

                            exeInfo = Global.AppendToComment(exeInfo, toolFileNameAndDate);

                            if (DebugLevel >= 2)
                            {
                                OnStatusEvent("EXE Info: " + exeInfo);
                            }
                            else
                            {
                                OnDebugEvent("EXE Info: " + exeInfo);
                            }
                        }
                        else
                        {
                            OnStatusEvent("Warning: Tool file not found: " + toolFile.FullName);
                        }
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent("Exception looking up tool version file info", ex);
                    }
                }
            }

            // Append the .Exe info to toolVersionInfo
            if (string.IsNullOrEmpty(exeInfo))
            {
                toolVersionInfoCombined = toolVersionInfo;
            }
            else
            {
                toolVersionInfoCombined = Global.AppendToComment(toolVersionInfo, exeInfo);
            }

            if (saveToolVersionTextFile)
            {
                SaveToolVersionInfoFile(WorkDir, toolVersionInfoCombined);
            }

            if (Global.OfflineMode)
                return true;

            return StoreToolVersionInDatabase(toolVersionInfoCombined);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks>This method is appropriate for plugins that call a .NET executable</remarks>
        /// <param name="progLoc">Path to the primary .exe or .DLL</param>
        /// <param name="saveToolVersionTextFile">If true, creates a text file with the tool version information</param>
        /// <returns>True if success, false if an error</returns>
        public bool StoreDotNETToolVersionInfo(string progLoc, bool saveToolVersionTextFile = true)
        {
            return StoreDotNETToolVersionInfo(progLoc, new List<string>(), saveToolVersionTextFile);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks>This method is appropriate for plugins that call a .NET executable</remarks>
        /// <param name="progLoc">Path to the primary .exe or .DLL</param>
        /// <param name="additionalDLLs">Additional .NET DLLs to examine (either simply names or full paths)</param>
        /// <param name="saveToolVersionTextFile">If true, creates a text file with the tool version information</param>
        /// <returns>True if success, false if an error</returns>
        public bool StoreDotNETToolVersionInfo(string progLoc, IReadOnlyCollection<string> additionalDLLs, bool saveToolVersionTextFile = true)
        {
            var toolVersionInfo = string.Empty;

            if (DebugLevel >= 2)
            {
                OnDebugEvent("Determining tool version info");
            }

            var programInfo = new FileInfo(progLoc);

            if (!programInfo.Exists)
            {
                try
                {
                    return SetStepTaskToolVersion("Unknown", new List<FileInfo>(), saveToolVersionTextFile);
                }
                catch (Exception ex)
                {
                    OnErrorEvent("Error calling SetStepTaskToolVersion", ex);
                    return false;
                }
            }

            // Lookup the version of the .NET program
            StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, programInfo.FullName);

            // Store the path to the .exe or .dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                programInfo
            };

            if (additionalDLLs != null)
            {
                // Add paths to key DLLs
                foreach (var dllNameOrPath in additionalDLLs)
                {
                    if (Path.IsPathRooted(dllNameOrPath) || dllNameOrPath.Contains(Path.DirectorySeparatorChar))
                    {
                        // Absolute or relative path; use as is
                        toolFiles.Add(new FileInfo(dllNameOrPath));
                        continue;
                    }

                    // Assume simply a filename
                    if (programInfo.Directory == null)
                    {
                        // Unable to determine the directory path for programInfo; this shouldn't happen
                        toolFiles.Add(new FileInfo(dllNameOrPath));
                    }
                    else
                    {
                        // Add it as a relative path to programInfo
                        toolFiles.Add(new FileInfo(Path.Combine(programInfo.Directory.FullName, dllNameOrPath)));
                    }
                }
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Store the tool version info in the database
        /// </summary>
        /// <param name="toolVersionInfo">Tool version info string</param>
        public bool StoreToolVersionInDatabase(string toolVersionInfo)
        {
            var analysisTask = new AnalysisJob(mMgrParams, DebugLevel);
            var dbTools = analysisTask.PipelineDBProcedureExecutor;

            // Setup for execution of the stored procedure
            var cmd = dbTools.CreateCommand(SP_NAME_SET_TASK_TOOL_VERSION, CommandType.StoredProcedure);

            dbTools.AddTypedParameter(cmd, "@job", SqlType.Int, value: mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0));
            dbTools.AddTypedParameter(cmd, "@step", SqlType.Int, value: mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0));
            dbTools.AddParameter(cmd, "@toolVersionInfo", SqlType.VarChar, 900, toolVersionInfo);
            var returnCodeParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.InputOutput);

            // Execute the stored procedure (retry the call, up to 4 times)
            var resCode = dbTools.ExecuteSP(cmd, 4);

            var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

            if (resCode == 0 && returnCode == 0)
            {
                return true;
            }

            if (resCode != 0 && returnCode == 0)
            {
                OnErrorEvent("ExecuteSP() reported result code {0} storing tool version in database for current processing step", resCode);
                return false;
            }

            OnErrorEvent(
                "Stored procedure {0} reported return code {1}",
                SP_NAME_SET_TASK_TOOL_VERSION, returnCodeParam.Value.CastDBVal<string>());

            return false;
        }

        /// <summary>
        /// Uses Reflection to determine the version info for an assembly already loaded in memory
        /// </summary>
        /// <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="assemblyName">Assembly Name</param>
        /// <param name="includeRevision">Set to true to include a version of the form 1.5.4821.24755; set to omit the revision, giving a version of the form 1.5.4821</param>
        /// <returns>True if success, false if an error</returns>
        public bool StoreToolVersionInfoForLoadedAssembly(ref string toolVersionInfo, string assemblyName, bool includeRevision = true)
        {
            try
            {
                var assembly = System.Reflection.Assembly.Load(assemblyName).GetName();

                string nameAndVersion;

                if (includeRevision)
                {
                    nameAndVersion = assembly.Name + ", Version=" + assembly.Version;
                }
                else
                {
                    nameAndVersion = assembly.Name + ", Version=" + assembly.Version.Major + "." + assembly.Version.Minor + "." + assembly.Version.Build;
                }

                toolVersionInfo = Global.AppendToComment(toolVersionInfo, nameAndVersion);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception determining Assembly info for " + assemblyName, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Determines the version info for a .NET DLL or executable using reflection
        /// If reflection fails, uses System.Diagnostics.FileVersionInfo
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="filePath">Path to the DLL or .exe</param>
        /// <returns>True if success, false if an error</returns>
        public bool StoreToolVersionInfoOneFile(ref string toolVersionInfo, string filePath)
        {
            bool success;

            try
            {
                var binaryFile = new FileInfo(filePath);

                if (!binaryFile.Exists)
                {
                    OnWarningEvent("Warning: File not found by StoreToolVersionInfoOneFile: " + filePath);
                    return false;
                }

                var assembly = System.Reflection.Assembly.LoadFrom(binaryFile.FullName);
                var assemblyName = assembly.GetName();

                var nameAndVersion = assemblyName.Name + ", Version=" + assemblyName.Version;
                toolVersionInfo = Global.AppendToComment(toolVersionInfo, nameAndVersion);

                success = true;
            }
            catch (BadImageFormatException)
            {
                // Most likely trying to read a 64-bit DLL (if this program is running as 32-bit)
                // Or, if this program is AnyCPU and running as 64-bit, the target DLL or Exe must be 32-bit

                // Instead try StoreToolVersionInfoOneFile32Bit or StoreToolVersionInfoOneFile64Bit

                // Use this when compiled as AnyCPU
                success = StoreToolVersionInfoOneFile32Bit(ref toolVersionInfo, filePath);

                // Use this when compiled as 32-bit
                // success = StoreToolVersionInfoOneFile64Bit(toolVersionInfo, filePath)

            }
            catch (Exception ex)
            {
                // If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, add these lines to the end of file AnalysisManagerProg.exe.config
                //  <startup useLegacyV2RuntimeActivationPolicy="true">
                //    <supportedRuntime version="v4.0" />
                //  </startup>
                OnErrorEvent("Exception determining Assembly info for " + Path.GetFileName(filePath), ex);
                success = false;
            }

            if (!success)
            {
                success = StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, filePath);
            }

            return success;
        }

        /// <summary>
        /// Determines the version info for a .NET DLL or executable using System.Diagnostics.FileVersionInfo
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="filePath">Path to the DLL or executable</param>
        /// <returns>True if success, false if an error</returns>
        public bool StoreToolVersionInfoViaSystemDiagnostics(ref string toolVersionInfo, string filePath)
        {
            try
            {
                var binaryFile = new FileInfo(filePath);

                if (!binaryFile.Exists)
                {
                    OnWarningEvent("File not found by StoreToolVersionInfoViaSystemDiagnostics: " + filePath);
                    return false;
                }

                var fileVersionInfo = FileVersionInfo.GetVersionInfo(filePath);

                var name = fileVersionInfo.FileDescription;

                if (string.IsNullOrWhiteSpace(name))
                {
                    if (binaryFile.Name.Equals("MaxQuantCmd.exe", StringComparison.OrdinalIgnoreCase) &&
                        fileVersionInfo.InternalName.Equals("MaxQuantCmd.dll", StringComparison.OrdinalIgnoreCase))
                    {
                        // Starting with MaxQuant 2.5.0, the FileDescription for MaxQuantCmd.exe is simply a space, and the internal name is "MaxQuantCmd.dll"
                        // For older versions, the FileDescription was "MaxQuantCmd", so use that instead of "MaxQuantCmd.dll"
                        name = "MaxQuantCmd";
                    }
                    else
                    {
                        name = fileVersionInfo.InternalName;
                    }
                }

                if (string.IsNullOrWhiteSpace(name))
                {
                    name = fileVersionInfo.FileName;
                }

                if (string.IsNullOrWhiteSpace(name))
                {
                    name = binaryFile.Name;
                }

                var version = fileVersionInfo.FileVersion;

                if (string.IsNullOrWhiteSpace(version))
                {
                    version = fileVersionInfo.ProductVersion;
                }

                if (string.IsNullOrWhiteSpace(version))
                {
                    version = "??";
                }

                var nameAndVersion = string.Format("{0}, Version={1}", name, version);

                toolVersionInfo = Global.AppendToComment(toolVersionInfo, nameAndVersion);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception determining File Version for " + Path.GetFileName(filePath), ex);
                return false;
            }
        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 32-bit .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="filePath">File path</param>
        /// <returns>True if success, false if an error</returns>
        public bool StoreToolVersionInfoOneFile32Bit(ref string toolVersionInfo, string filePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref toolVersionInfo, filePath, "DLLVersionInspector_x86.exe");
        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 64-bit .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="filePath">File path</param>
        /// <returns>True if success, false if an error</returns>
        public bool StoreToolVersionInfoOneFile64Bit(ref string toolVersionInfo, string filePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref toolVersionInfo, filePath, "DLLVersionInspector_x64.exe");
        }

        /// <summary>
        /// Uses the specified DLLVersionInspector to determine the version of a .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="filePath">File path</param>
        /// <param name="versionInspectorExeName">DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe</param>
        /// <returns>True if success, false if an error</returns>
        private bool StoreToolVersionInfoOneFileUseExe(ref string toolVersionInfo, string filePath, string versionInspectorExeName)
        {
            try
            {
                var appPath = Path.Combine(Global.GetAppDirectoryPath(), versionInspectorExeName);

                var binaryFile = new FileInfo(filePath);

                if (!binaryFile.Exists)
                {
                    OnErrorEvent("File not found by StoreToolVersionInfoOneFileUseExe: " + filePath);
                    return false;
                }

                if (!File.Exists(appPath))
                {
                    OnErrorEvent("DLLVersionInspector not found by StoreToolVersionInfoOneFileUseExe: " + appPath);
                    return false;
                }

                // Call DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe to determine the tool version

                var versionInfoFilePath = Path.Combine(WorkDir, Path.GetFileNameWithoutExtension(binaryFile.Name) + "_VersionInfo.txt");

                var args = Global.PossiblyQuotePath(binaryFile.FullName) + " /O:" + Global.PossiblyQuotePath(versionInfoFilePath);

                if (DebugLevel >= 3)
                {
                    OnDebugEvent(appPath + " " + args);
                }

                var progRunner = new RunDosProgram(Global.GetAppDirectoryPath(), DebugLevel)
                {
                    CacheStandardOutput = false,
                    CreateNoWindow = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false,
                    MonitorInterval = 250
                };

                RegisterEvents(progRunner);

                var success = progRunner.RunProgram(appPath, args, "DLLVersionInspector", false);

                if (!success)
                {
                    return false;
                }

                success = ReadVersionInfoFile(filePath, versionInfoFilePath, out var version);

                // Delete the version info file
                try
                {
                    if (File.Exists(versionInfoFilePath))
                    {
                        File.Delete(versionInfoFilePath);
                    }
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                if (!success || string.IsNullOrWhiteSpace(version))
                {
                    return false;
                }

                toolVersionInfo = Global.AppendToComment(toolVersionInfo, version);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format(
                    "Exception determining Version info for {0} using {1}",
                    Path.GetFileName(filePath), versionInspectorExeName), ex);
            }

            return false;
        }
    }
}
