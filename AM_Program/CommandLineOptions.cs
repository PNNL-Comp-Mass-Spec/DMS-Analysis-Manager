using PRISM;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Tracks processing options, typically loaded from the command line
    /// </summary>
    public class CommandLineOptions
    {
        /// <summary>
        /// When true, disable posting status messages to the message queue
        /// </summary>
        [Option("NQ", HelpShowsDefault = false,
            HelpText = "Disable posting status messages to the message queue")]
        public bool DisableMessageQueue { get; set; }

        /// <summary>
        /// When true, Disable searching for files in MyEMSL
        /// </summary>
        [Option("NoMyEMSL", HelpShowsDefault = false,
            HelpText = "Disable searching for files in MyEMSL. " +
                       "This is useful if MyEMSL is offline or the current user does not have read access to SimpleSearch")]
        public bool DisableMyEMSL { get; set; }

        /// <summary>
        /// When true, start the program in code test mode
        /// </summary>
        [Option("T", "CodeTest", "Test", HelpShowsDefault = false,
            HelpText = "Start the program in code test mode")]
        public bool CodeTestMode { get; set; }

        /// <summary>
        /// When true, show additional debug messages
        /// </summary>
        [Option("Trace", "Verbose", HelpShowsDefault = false,
            HelpText = "Enable trace mode, where debug messages are written to the command prompt")]
        public bool TraceMode { get; set; }


        /// <summary>
        /// When true, display DLL versions
        /// </summary>
        public bool DisplayDllVersions { get; set; }

        /// <summary>
        /// Display the version of all DLLs in the same directory as this .exe
        /// </summary>
        [Option("DLL", ArgExistsProperty = nameof(DisplayDllVersions), HelpShowsDefault = false,
            HelpText = "Display the version of all DLLs in the same directory as this .exe. " +
                       "Can also provide a path to display the version of all DLLs in the specified directory (surround path with double quotes if spaces)")]
        public string DisplayDllPath { get; set; }

        /// <summary>
        /// When true, use the DMSUpdateManager to push new/updated files to the remote host associated with this manager
        /// </summary>
        [Option("PushRemote", HelpShowsDefault = false,
            HelpText = "Use the DMSUpdateManager to push new/updated files to the remote host associated with this manager. " +
                       "This is only valid if the manager has parameter RunJobsRemotely set to True in the Manager Control DB. " +
                       "Ignored if /Offline is used.")]
        public bool PushRemoteMgrFilesOnly { get; set; }

        /// <summary>
        /// When true, enable offline mode (no database access or use of external servers)
        /// </summary>
        [Option("Offline", HelpShowsDefault = false,
            HelpText = "Enable offline mode (database access and use of external servers is disabled). " +
                       "Requires that the ManagerSettingsLocal.xml file has several settings defined, including LocalTaskQueuePath and LocalWorkDirPath")]
        public bool OfflineMode { get; set; }

        /// <summary>
        /// When true, disable access to Windows-specific method
        /// </summary>
        /// <remarks>Both /Offline and /Linux are auto-enabled if the path separation character is /</remarks>
        [Option("Linux", HelpShowsDefault = false,
            HelpText = "Disable access to Windows-specific methods. " +
                       "Both /Offline and /Linux are auto-enabled if the path separation character is /")]
        public bool LinuxOSMode { get; set; }

        /// <summary>
        /// Show the program version and OS version
        /// </summary>
        [Option("V", "Version", HelpShowsDefault = false,
            HelpText = "See the program version and OS version")]
        public bool ShowVersionOnly { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public CommandLineOptions()
        {
            DisplayDllPath = string.Empty;
        }
    }
}
