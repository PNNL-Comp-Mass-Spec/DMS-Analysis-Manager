using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerMSGFDBPlugIn;
using PRISM;

namespace MSGFPlusIndexFileCopier
{
    class Program
    {
        private const string PROGRAM_DATE = "August 25, 2021";
        private const string DEFAULT_REMOTE_SHARE = @"\\proto-7\MSGFPlus_Index_Files\Other";

        private static string mFastaFilePath;
        private static string mRemoteIndexFolderPath;
        private static bool mCreateIndexFileForExistingFiles;

        static int Main()
        {
            var commandLineParser = new clsParseCommandLine();

            mFastaFilePath = string.Empty;
            mRemoteIndexFolderPath = DEFAULT_REMOTE_SHARE;
            mCreateIndexFileForExistingFiles = false;

            try
            {
                var success = false;

                if (commandLineParser.ParseCommandLine())
                {
                    if (SetOptionsUsingCommandLineParameters(commandLineParser))
                        success = true;
                }

                if (!success ||
                    commandLineParser.NeedToShowHelp ||
                    commandLineParser.ParameterCount + commandLineParser.NonSwitchParameterCount == 0)
                {
                    ShowProgramHelp();
                    return -1;
                }

                success = CopyIndexFiles(mFastaFilePath, mRemoteIndexFolderPath, mCreateIndexFileForExistingFiles);

                if (!success)
                {
                    return -1;
                }
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error occurred in Program->Main: " + ex.Message, ex);
                return -1;
            }

            return 0;
        }

        private static bool CopyIndexFiles(string fastaFilePath, string remoteIndexFolderPath, bool createIndexFileForExistingFiles)
        {
            try
            {
                var remoteShare = new DirectoryInfo(remoteIndexFolderPath);

                if (!remoteShare.Exists)
                {
                    ShowErrorMessage("Remote share not found: " + remoteIndexFolderPath);
                    return false;
                }

                var fastaFile = new FileInfo(fastaFilePath);

                if (createIndexFileForExistingFiles)
                {
                    // Update fastaFile to point to the remote share
                    // Note that the FASTA file does not have to exist (and likely won't)
                    var remoteFastaPath = Path.Combine(remoteIndexFolderPath, fastaFile.Name);
                    fastaFile = new FileInfo(remoteFastaPath);
                }
                else
                {
                    if (!fastaFile.Exists)
                    {
                        ShowErrorMessage("FASTA file not found: " + fastaFile.FullName);
                        return false;
                    }
                }

                const int debugLevel = 1;
                const string managerName = "MSGFPlusIndexFileCopier";

                var success = CreateMSGFDBSuffixArrayFiles.CopyIndexFilesToRemote(
                    fastaFile,
                    remoteIndexFolderPath,
                    debugLevel,
                    managerName,
                    createIndexFileForExistingFiles,
                    out var errorMessage);

                if (!success)
                {
                    ShowErrorMessage(errorMessage);
                }
                else
                {
                    // Confirm that the .MSGFPlusIndexFileInfo file was created
                    var indexFile = new FileInfo(Path.Combine(remoteIndexFolderPath, fastaFile.Name + ".MSGFPlusIndexFileInfo"));

                    if (indexFile.Exists)
                    {
                        Console.WriteLine("Index file created: " + indexFile.FullName);
                    }
                    else
                    {
                        ShowErrorMessage("Index file not found: " + indexFile.FullName);
                        success = false;
                    }
                }

                return success;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error in CopyIndexFiles: " + ex.Message, ex);
                return false;
            }
        }

        private static
        string GetAppVersion()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            // Returns True if no problems; otherwise, returns false
            var validParameters = new List<string> { "F", "R", "X" };

            try
            {
                // Make sure no invalid parameters are present
                if (commandLineParser.InvalidParametersPresent(validParameters))
                {
                    var badArguments = new List<string>();

                    foreach (var item in commandLineParser.InvalidParameters(validParameters))
                    {
                        badArguments.Add("/" + item);
                    }

                    ShowErrorMessage("Invalid command line parameters", badArguments);

                    return false;
                }

                // Query commandLineParser to see if various parameters are present

                if (commandLineParser.NonSwitchParameterCount > 0)
                    mFastaFilePath = commandLineParser.RetrieveNonSwitchParameter(0);

                if (commandLineParser.NonSwitchParameterCount > 1)
                    mRemoteIndexFolderPath = commandLineParser.RetrieveNonSwitchParameter(1);

                if (!ParseParameter(commandLineParser, "F", "a FASTA file name or path", ref mFastaFilePath)) return false;

                if (!ParseParameter(commandLineParser, "R", "a remote MSGFPlus Index Folder path", ref mRemoteIndexFolderPath)) return false;

                if (commandLineParser.IsParameterPresent("X"))
                {
                    mCreateIndexFileForExistingFiles = true;
                }

                return true;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error parsing the command line parameters: " + ex.Message, ex);
            }

            return false;
        }

        private static bool ParseParameter(clsParseCommandLine commandLineParser, string parameterName, string description, ref string targetVariable)
        {
            if (commandLineParser.RetrieveValueForParameter(parameterName, out var value))
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    ShowErrorMessage("/" + parameterName + " does not have " + description);
                    return false;
                }
                targetVariable = string.Copy(value);
            }
            return true;
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void ShowErrorMessage(string title, IEnumerable<string> errorMessages)
        {
            ConsoleMsgUtils.ShowErrors(title, errorMessages);
        }

        private static void ShowProgramHelp()
        {
            var exeName = Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

            try
            {
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "This program copies MS-GF+ index files (suffix array files) from the local folder " +
                                      "to the remote share where MS-GF+ index files are stored. Once the copy is complete, " +
                                      "it creates the .MSGFPlusIndexFileInfo file. If the suffix array files " +
                                      "have already been copied to the remote share, use switch /X to create " +
                                      "the .MSGFPlusIndexFileInfo file without copying any files."));
                Console.WriteLine();

                Console.Write("Program syntax #1:" + Environment.NewLine + exeName);
                Console.WriteLine(" FastaFilePath RemoteIndexFileSharePath [/X]");

                Console.WriteLine();
                Console.Write("Program syntax #2:" + Environment.NewLine + exeName);
                Console.WriteLine(" /F:FastaFilePath [/R:RemoteIndexFileSharePath] [/X]");

                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "FastaFilePath specifies the path to the fasta file; " +
                                      "MS-GF+ index files to be copied should be in the same folder as the fasta file. " +
                                      "If using /X, FastaFilePath does not have to point to an existing file; " +
                                      "only the filename will be used"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "RemoteIndexFileSharePath specifies the share where the index files are stored, " +
                                      "along with the .MSGFPlusIndexFileInfo file. The default is " + DEFAULT_REMOTE_SHARE));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Use /X to create the .MSGFPlusIndexFileInfo file in the remote share, " +
                                      "using the existing files that were previously manually copied to the share"));
                Console.WriteLine();
                Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)");
                Console.WriteLine("Version: " + GetAppVersion());
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov");
                Console.WriteLine("Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics or ");
                Console.WriteLine();

                // Delay for 750 msec in case the user double-clicked this file from within Windows Explorer (or started the program via a shortcut)
                System.Threading.Thread.Sleep(750);
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error displaying the program syntax: " + ex.Message, ex);
            }
        }
    }
}
