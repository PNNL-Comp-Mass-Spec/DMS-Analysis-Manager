using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Windows.Forms;
using PRISM;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Methods for tracking versions of executables and DLLs
    /// </summary>
    public class clsToolVersionUtilities : EventNotifier
    {
        #region "Constants"

        /// <summary>
        /// Stored procedure name for storing the step tool version
        /// </summary>
        private const string SP_NAME_SET_TASK_TOOL_VERSION = "SetStepTaskToolVersion";

        public const string TOOL_VERSION_INFO_PREFIX = "Tool_Version_Info_";

        #endregion

        #region "Module variables"

        /// <summary>
        /// Access to the job parameters
        /// </summary>
        private readonly IJobParams mJobParams;

        /// <summary>
        /// Access to manager parameters
        /// </summary>
        private readonly IMgrParams mMgrParams;

        #endregion

        #region "Properties"

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


        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="jobParams"></param>
        /// <param name="job"></param>
        /// <param name="dataset"></param>
        /// <param name="stepToolName"></param>
        /// <param name="debugLevel"></param>
        /// <param name="workDir"></param>
        public clsToolVersionUtilities(IMgrParams mgrParams, IJobParams jobParams, int job, string dataset, string stepToolName, short debugLevel, string workDir)
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
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        public bool GetMSConvertToolVersion(string msConvertPath, out string versionInfo)
        {
            versionInfo = string.Empty;

            try
            {
                var msConvertInfo = new FileInfo(msConvertPath);

                if (!msConvertInfo.Exists)
                {
                    OnWarningEvent("File not found by GetMSConvertToolVersion" + ": " + msConvertPath);
                    return false;
                }

                var progRunner = new clsRunDosProgram(clsGlobal.GetAppDirectoryPath(), DebugLevel)
                {
                    CacheStandardOutput = false,
                    CreateNoWindow = true,
                    EchoOutputToConsole = false,
                    WriteConsoleOutputToFile = false,
                    MonitorInterval = 250,
                };

                progRunner.DebugEvent += this.OnDebugEvent;
                progRunner.StatusEvent += this.OnStatusEvent;
                progRunner.WarningEvent += this.OnWarningEvent;

                progRunner.SkipConsoleWriteIfNoErrorListener = true;

                var args = "--help";

                var success = progRunner.RunProgram(msConvertInfo.FullName, args, "MSConvert", false);

                if (!success)
                {
                    return false;
                }

                // MSConvert reports the syntax and the version via the error stream
                // This has been cached in progRunner.CachedConsoleErrors

                // Read the console errors and look for the ProteoWizard version and Build Date

                using (var reader = new StringReader(progRunner.CachedConsoleErrors))
                {
                    while (reader.Peek() >= 0)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (dataLine.StartsWith("ProteoWizard release", StringComparison.OrdinalIgnoreCase))
                        {
                            // ProteoWizard release: 3.0.18145 (f23158c8f)
                            versionInfo = clsGlobal.AppendToComment(versionInfo, dataLine);
                        } else if (dataLine.StartsWith("Build date", StringComparison.OrdinalIgnoreCase))
                        {
                            // Add the executable name to the build date text, giving, for example:
                            // MSConvert.exe Build date: May 24 2018 22:22:11
                            versionInfo = clsGlobal.AppendToComment(versionInfo, msConvertInfo.Name + " " + dataLine);
                        }
                    }
                }

                if (!string.IsNullOrWhiteSpace(versionInfo))
                    return true;

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
        /// <param name="dllFilePath"></param>
        /// <param name="versionInfoFilePath"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool ReadVersionInfoFile(string dllFilePath, string versionInfoFilePath, out string version)
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
                using (var reader = new StreamReader(new FileStream(versionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

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
                                break;
                            case "path":
                                break;
                            case "version":
                                version = string.Copy(value);
                                if (string.IsNullOrWhiteSpace(version))
                                {
                                    OnErrorEvent("Empty version line in Version Info file for " + Path.GetFileName(dllFilePath));
                                    success = false;
                                }
                                else
                                {
                                    success = true;
                                }
                                break;
                            case "error":
                                OnErrorEvent("Error reported by DLLVersionInspector for " + Path.GetFileName(dllFilePath) + ": " + value);
                                success = false;
                                break;
                                // default:
                                // Ignore the line

                        }
                    }

                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error reading Version Info File for " + Path.GetFileName(dllFilePath), ex);
            }

            return success;

        }

        /// <summary>
        /// Creates a Tool Version Info file
        /// </summary>
        /// <param name="directoryPath"></param>
        /// <param name="toolVersionInfo"></param>
        /// <param name="stepToolNameOverride"></param>
        /// <returns></returns>
        /// <remarks></remarks>
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

                using (var writer = new StreamWriter(new FileStream(toolVersionFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {

                    writer.WriteLine("Date: " + DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                    writer.WriteLine("Dataset: " + Dataset);
                    writer.WriteLine("Job: " + Job);
                    writer.WriteLine("Step: " + mJobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step"));
                    writer.WriteLine("Tool: " + mJobParams.GetParam("StepTool"));
                    writer.WriteLine("ToolVersionInfo:");

                    writer.WriteLine(toolVersionInfo.Replace("; ", Environment.NewLine));

                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception saving tool version info", ex);
            }

        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="toolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <param name="saveToolVersionTextFile">If true, creates a text file with the tool version information</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
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
                            exeInfo = clsGlobal.AppendToComment(exeInfo, toolFile.Name + ": " +
                                                                         toolFile.LastWriteTime.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));

                            if (DebugLevel >= 2)
                                OnStatusEvent("EXE Info: " + exeInfo);
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
                toolVersionInfoCombined = string.Copy(toolVersionInfo);
            }
            else
            {
                toolVersionInfoCombined = clsGlobal.AppendToComment(toolVersionInfo, exeInfo);
            }

            if (saveToolVersionTextFile)
            {
                SaveToolVersionInfoFile(WorkDir, toolVersionInfoCombined);
            }

            if (clsGlobal.OfflineMode)
                return true;

            var success = StoreToolVersionInDatabase(toolVersionInfoCombined);
            return success;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <param name="progLoc">Path to the primary .exe or .DLL</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This method is appropriate for plugins that call a .NET executable</remarks>
        public bool StoreDotNETToolVersionInfo(string progLoc)
        {
            return StoreDotNETToolVersionInfo(progLoc, new List<string>());
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <param name="progLoc">Path to the primary .exe or .DLL</param>
        /// <param name="additionalDLLs">Additional .NET DLLs to examine (either simply names or full paths)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This method is appropriate for plugins that call a .NET executable</remarks>
        public bool StoreDotNETToolVersionInfo(string progLoc, IReadOnlyCollection<string> additionalDLLs)
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
                    return SetStepTaskToolVersion("Unknown", new List<FileInfo>(), saveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    OnErrorEvent("Exception calling SetStepTaskToolVersion", ex);
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
                var success = SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile: false);
                return success;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }

        }

        /// <summary>
        /// Store the tool version info in the database
        /// </summary>
        /// <param name="toolVersionInfo"></param>
        /// <returns></returns>
        public bool StoreToolVersionInDatabase(string toolVersionInfo)
        {

            // Setup for execution of the stored procedure
            var cmd = new SqlCommand
            {
                CommandType = CommandType.StoredProcedure,
                CommandText = SP_NAME_SET_TASK_TOOL_VERSION
            };

            cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
            cmd.Parameters.Add(new SqlParameter("@job", SqlDbType.Int)).Value = mJobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            cmd.Parameters.Add(new SqlParameter("@step", SqlDbType.Int)).Value = mJobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            cmd.Parameters.Add(new SqlParameter("@ToolVersionInfo", SqlDbType.VarChar, 900)).Value = toolVersionInfo;

            var analysisTask = new clsAnalysisJob(mMgrParams, DebugLevel);

            // Execute the stored procedure (retry the call up to 4 times)
            var resCode = analysisTask.PipelineDBProcedureExecutor.ExecuteSP(cmd, 4);

            if (resCode == 0)
            {
                return true;
            }

            OnErrorEvent("Error " + resCode + " storing tool version in database for current processing step");
            return false;
        }

        /// <summary>
        /// Uses Reflection to determine the version info for an assembly already loaded in memory
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="assemblyName">Assembly Name</param>
        /// <param name="includeRevision">Set to True to include a version of the form 1.5.4821.24755; set to omit the revision, giving a version of the form 1.5.4821</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
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

                toolVersionInfo = clsGlobal.AppendToComment(toolVersionInfo, nameAndVersion);

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception determining Assembly info for " + assemblyName, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Determines the version info for a .NET DLL using reflection
        /// If reflection fails, uses System.Diagnostics.FileVersionInfo
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="dllFilePath">Path to the DLL</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        public bool StoreToolVersionInfoOneFile(ref string toolVersionInfo, string dllFilePath)
        {

            bool success;

            try
            {
                var dllFileInfo = new FileInfo(dllFilePath);

                if (!dllFileInfo.Exists)
                {
                    OnWarningEvent("Warning: File not found by StoreToolVersionInfoOneFile: " + dllFilePath);
                    return false;

                }

                var assembly = System.Reflection.Assembly.LoadFrom(dllFileInfo.FullName);
                var assemblyName = assembly.GetName();

                var nameAndVersion = assemblyName.Name + ", Version=" + assemblyName.Version;
                toolVersionInfo = clsGlobal.AppendToComment(toolVersionInfo, nameAndVersion);

                success = true;
            }
            catch (BadImageFormatException)
            {
                // Most likely trying to read a 64-bit DLL (if this program is running as 32-bit)
                // Or, if this program is AnyCPU and running as 64-bit, the target DLL or Exe must be 32-bit

                // Instead try StoreToolVersionInfoOneFile32Bit or StoreToolVersionInfoOneFile64Bit

                // Use this when compiled as AnyCPU
                success = StoreToolVersionInfoOneFile32Bit(ref toolVersionInfo, dllFilePath);

                // Use this when compiled as 32-bit
                // success = StoreToolVersionInfoOneFile64Bit(toolVersionInfo, dllFilePath)

            }
            catch (Exception ex)
            {
                // If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, add these lines to the end of file AnalysisManagerProg.exe.config
                //  <startup useLegacyV2RuntimeActivationPolicy="true">
                //    <supportedRuntime version="v4.0" />
                //  </startup>
                OnErrorEvent("Exception determining Assembly info for " + Path.GetFileName(dllFilePath), ex);
                success = false;
            }

            if (!success)
            {
                success = StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, dllFilePath);
            }

            return success;

        }

        /// <summary>
        /// Determines the version info for a .NET DLL using System.Diagnostics.FileVersionInfo
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="dllFilePath">Path to the DLL</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        public bool StoreToolVersionInfoViaSystemDiagnostics(ref string toolVersionInfo, string dllFilePath)
        {

            try
            {
                var dllFileInfo = new FileInfo(dllFilePath);

                if (!dllFileInfo.Exists)
                {
                    OnWarningEvent("File not found by StoreToolVersionInfoViaSystemDiagnostics: " + dllFilePath);
                    return false;
                }

                var oFileVersionInfo = FileVersionInfo.GetVersionInfo(dllFilePath);

                var name = oFileVersionInfo.FileDescription;
                if (string.IsNullOrEmpty(name))
                {
                    name = oFileVersionInfo.InternalName;
                }

                if (string.IsNullOrEmpty(name))
                {
                    name = oFileVersionInfo.FileName;
                }

                if (string.IsNullOrEmpty(name))
                {
                    name = dllFileInfo.Name;
                }

                var version = oFileVersionInfo.FileVersion;
                if (string.IsNullOrEmpty(version))
                {
                    version = oFileVersionInfo.ProductVersion;
                }

                if (string.IsNullOrEmpty(version))
                {
                    version = "??";
                }

                var nameAndVersion = name + ", Version=" + version;
                toolVersionInfo = clsGlobal.AppendToComment(toolVersionInfo, nameAndVersion);

                return true;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception determining File Version for " + Path.GetFileName(dllFilePath), ex);
                return false;
            }

        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 32-bit .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo"></param>
        /// <param name="dllFilePath"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        public bool StoreToolVersionInfoOneFile32Bit(ref string toolVersionInfo, string dllFilePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref toolVersionInfo, dllFilePath, "DLLVersionInspector_x86.exe");
        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 64-bit .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo"></param>
        /// <param name="dllFilePath"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        public bool StoreToolVersionInfoOneFile64Bit(ref string toolVersionInfo, string dllFilePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref toolVersionInfo, dllFilePath, "DLLVersionInspector_x64.exe");
        }

        /// <summary>
        /// Uses the specified DLLVersionInspector to determine the version of a .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo"></param>
        /// <param name="dllFilePath"></param>
        /// <param name="versionInspectorExeName">DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        private bool StoreToolVersionInfoOneFileUseExe(ref string toolVersionInfo, string dllFilePath, string versionInspectorExeName)
        {

            try
            {
                var appPath = Path.Combine(clsGlobal.GetAppDirectoryPath(), versionInspectorExeName);

                var dllFileInfo = new FileInfo(dllFilePath);

                if (!dllFileInfo.Exists)
                {
                    OnErrorEvent("File not found by StoreToolVersionInfoOneFileUseExe: " + dllFilePath);
                    return false;
                }

                if (!File.Exists(appPath))
                {
                    OnErrorEvent("DLLVersionInspector not found by StoreToolVersionInfoOneFileUseExe: " + appPath);
                    return false;
                }

                // Call DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe to determine the tool version

                var versionInfoFilePath = Path.Combine(WorkDir, Path.GetFileNameWithoutExtension(dllFileInfo.Name) + "_VersionInfo.txt");


                var args = clsGlobal.PossiblyQuotePath(dllFileInfo.FullName) + " /O:" + clsGlobal.PossiblyQuotePath(versionInfoFilePath);

                if (DebugLevel >= 3)
                {
                    OnDebugEvent(appPath + " " + args);
                }

                var progRunner = new clsRunDosProgram(clsGlobal.GetAppDirectoryPath(), DebugLevel)
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

                success = ReadVersionInfoFile(dllFilePath, versionInfoFilePath, out var version);

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

                toolVersionInfo = clsGlobal.AppendToComment(toolVersionInfo, version);

                return true;

            }
            catch (Exception ex)
            {
                var msg = "Exception determining Version info for " + Path.GetFileName(dllFilePath) + "  using " + versionInspectorExeName;
                OnErrorEvent(msg, ex);
            }

            return false;

        }

    }
}
