using PRISM;

namespace AnalysisManagerProg
{
    internal class CommandLineOptions
    {
        [Option("NQ", HelpShowsDefault = false, HelpText = "Disable posting status messages to the message queue")]
        public bool DisableMessageQueue { get; set; }

        [Option("NoMyEMSL", HelpShowsDefault = false, HelpText = "Disable searching for files in MyEMSL. " +
                                                                 "This is useful if MyEMSL is offline or the current user does not have read access to SimpleSearch")]
        public bool DisableMyEMSL { get; set; }

        [Option("T", "CodeTest", "Test", HelpShowsDefault = false, HelpText = "Start the program in code test mode")]
        public bool CodeTestMode { get; set; }

        [Option("Trace", "Verbose", HelpShowsDefault = false, HelpText = "Enable trace mode, where debug messages are written to the command prompt")]
        public bool TraceMode { get; set; }

        public bool DisplayDllVersions { get; set; }

        [Option("DLL", ArgExistsProperty = nameof(DisplayDllVersions), HelpShowsDefault = false, HelpText = "Display the version of all DLLs in the same directory as this .exe. " +
                                                                 "Can also provide a path to display the version of all DLLs in the specified directory (surround path with double quotes if spaces)")]
        public string DisplayDllPath { get; set; }

        [Option("PushRemote", HelpShowsDefault = false, HelpText = "use the DMSUpdateManager to push new/updated files to the remote host associated with this manager. " +
                                                                   "This is only valid if the manager has parameter RunJobsRemotely set to True in the Manager Control DB. " +
                                                                   "Ignored if /Offline is used.")]
        public bool PushRemoteMgrFilesOnly { get; set; }

        [Option("Offline", HelpShowsDefault = false, HelpText = "Enable offline mode (database access and use of external servers is disabled). " +
                                                                "Requires that the ManagerSettingsLocal.xml file has several settings defined, including LocalTaskQueuePath and LocalWorkDirPath")]
        public bool OfflineMode { get; set; }

        [Option("Linux", HelpShowsDefault = false, HelpText = "Disable access to Windows-specific methods. " +
                                                              "Both /Offline and /Linux are auto-enabled if the path separation character is /")]
        public bool LinuxOSMode { get; set; }

        [Option("V", "Version", HelpShowsDefault = false, HelpText = "See the program version and OS version")]
        public bool ShowVersionOnly { get; set; }

        public CommandLineOptions()
        {
            DisplayDllPath = string.Empty;
        }
    }
}
