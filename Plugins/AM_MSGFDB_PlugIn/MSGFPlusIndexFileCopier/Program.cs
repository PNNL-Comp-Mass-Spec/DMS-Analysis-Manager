using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerMSGFDBPlugIn;
using FileProcessor;

namespace MSGFPlusIndexFileCopier
{
	class Program
	{
		protected const string PROGRAM_DATE = "November 6, 2013";
		protected const string DEFAULT_REMOTE_SHARE = @"\\proto-7\MSGFPlus_Index_Files\Other";

		protected static string mFastaFilePath;
		protected static string mRemoteIndexFolderPath;
		protected static bool mCreateIndexFileForExistingFiles;

		static int Main(string[] args)
		{
			var objParseCommandLine = new clsParseCommandLine();

			mFastaFilePath = string.Empty;
			mRemoteIndexFolderPath = DEFAULT_REMOTE_SHARE;
			mCreateIndexFileForExistingFiles = false;

			try
			{
				bool success = false;

				if (objParseCommandLine.ParseCommandLine())
				{
					if (SetOptionsUsingCommandLineParameters(objParseCommandLine))
						success = true;
				}

				if (!success ||
					objParseCommandLine.NeedToShowHelp ||
					objParseCommandLine.ParameterCount + objParseCommandLine.NonSwitchParameterCount == 0)
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
				Console.WriteLine("Error occurred in Program->Main: " + Environment.NewLine + ex.Message);
				Console.WriteLine(ex.StackTrace);
				return -1;
			}

			return 0;
		}

		private static bool CopyIndexFiles(string fastaFilePath, string remoteIndexFolderPath, bool createIndexFileForExistingFiles)
		{
			try
			{
				var diRemoteShare = new DirectoryInfo(remoteIndexFolderPath);
				if (!diRemoteShare.Exists)
				{
					ShowErrorMessage("Remote share not found: " + remoteIndexFolderPath);
					return false;
				}

				var fiFastaFile = new FileInfo(fastaFilePath);

				if (createIndexFileForExistingFiles)
				{
					// Update fiFastaFile to point to the remote share
					// Note that the fasta file does not have to exist (and likely won't)
					string remoteFastaPath = Path.Combine(remoteIndexFolderPath, fiFastaFile.Name);
					fiFastaFile = new FileInfo(remoteFastaPath);
				}
				else
				{
					if (!fiFastaFile.Exists)
					{
						ShowErrorMessage("Fasta file not found: " + fiFastaFile.FullName);
						return false;
					}
				}

				const int debugLevel = 1;
				const string managerName = "MSGFPlusIndexFileCopier";
				string errorMessage;

				bool success = clsCreateMSGFDBSuffixArrayFiles.CopyIndexFilesToRemote(
					fiFastaFile,
					remoteIndexFolderPath,
					debugLevel,
					managerName,
					createIndexFileForExistingFiles,
					out errorMessage);

				if (!success)
				{
					ShowErrorMessage(errorMessage);
				}
				else
				{
					// Confirm that the .MSGFPlusIndexFileInfo file was created
					var fiIndexFile = new FileInfo(Path.Combine(remoteIndexFolderPath, fiFastaFile.Name + ".MSGFPlusIndexFileInfo"));
					if (fiIndexFile.Exists)
					{
						Console.WriteLine("Index file created: " + fiIndexFile.FullName);
					}
					else
					{
						ShowErrorMessage("Index file not found: " + fiIndexFile.FullName);
						success = false;
					}
				}

				return success;
			}
			catch
					(Exception
					ex)
			{
				ShowErrorMessage("Error in CopyIndexFiles: " + ex.Message);
				return false;
			}
		}

		private static
		string GetAppVersion()
		{
			return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
		}

		private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine objParseCommandLine)
		{
			// Returns True if no problems; otherwise, returns false
			var lstValidParameters = new List<string> { "F", "R", "X" };

			try
			{
				// Make sure no invalid parameters are present
				if (objParseCommandLine.InvalidParametersPresent(lstValidParameters))
				{
					var badArguments = new List<string>();
					foreach (string item in objParseCommandLine.InvalidParameters(lstValidParameters))
					{
						badArguments.Add("/" + item);
					}

					ShowErrorMessage("Invalid commmand line parameters", badArguments);

					return false;
				}

				// Query objParseCommandLine to see if various parameters are present						

				if (objParseCommandLine.NonSwitchParameterCount > 0)
					mFastaFilePath = objParseCommandLine.RetrieveNonSwitchParameter(0);

				if (objParseCommandLine.NonSwitchParameterCount > 1)
					mRemoteIndexFolderPath = objParseCommandLine.RetrieveNonSwitchParameter(1);

				if (!ParseParameter(objParseCommandLine, "F", "a fasta file name or path", ref mFastaFilePath)) return false;
				if (!ParseParameter(objParseCommandLine, "R", "a remote MSGFPlus Index Folder path", ref mRemoteIndexFolderPath)) return false;

				string value;
				if (objParseCommandLine.RetrieveValueForParameter("X", out value))
				{
					mCreateIndexFileForExistingFiles = true;
				}

				return true;
			}
			catch (Exception ex)
			{
				ShowErrorMessage("Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
			}

			return false;
		}

		private static bool ParseParameter(clsParseCommandLine objParseCommandLine, string parameterName, string description, ref string targetVariable)
		{
			string value;
			if (objParseCommandLine.RetrieveValueForParameter(parameterName, out value))
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

		private static void ShowErrorMessage(string strMessage)
		{
			const string strSeparator = "------------------------------------------------------------------------------";

			Console.WriteLine();
			Console.WriteLine(strSeparator);
			Console.WriteLine(strMessage);
			Console.WriteLine(strSeparator);
			Console.WriteLine();

			WriteToErrorStream(strMessage);
		}

		private static void ShowErrorMessage(string strTitle, IEnumerable<string> items)
		{
			const string strSeparator = "------------------------------------------------------------------------------";

			Console.WriteLine();
			Console.WriteLine(strSeparator);
			Console.WriteLine(strTitle);
			string strMessage = strTitle + ":";

			foreach (string item in items)
			{
				Console.WriteLine("   " + item);
				strMessage += " " + item;
			}
			Console.WriteLine(strSeparator);
			Console.WriteLine();

			WriteToErrorStream(strMessage);
		}


		private static void ShowProgramHelp()
		{
			string exeName = System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

			try
			{
				Console.WriteLine();
				Console.WriteLine("This program copies MSGF+ index files (suffix array files) from the local folder to the remote share where MSGF+ index files are stored");
				Console.WriteLine("Once the copy is complete, it creates the .MSGFPlusIndexFileInfo file");
				Console.WriteLine("If the suffix array files have already been copied to the remote share, then use switch /X to create the .MSGFPlusIndexFileInfo file without copying any files");
				Console.WriteLine();

				Console.Write("Program syntax #1:" + Environment.NewLine + exeName);
				Console.WriteLine(" FastaFilePath RemoteIndexFileSharePath [/X]");

				Console.WriteLine();
				Console.Write("Program syntax #2:" + Environment.NewLine + exeName);
				Console.WriteLine(" /F:FastaFilePath [/R:RemoteIndexFileSharePath] [/X]");

				Console.WriteLine();
				Console.WriteLine("FastaFilePath specifies the path to the fasta file; MSGF+ index files to be copied should be in the same folder as the fasta file.  If using /X then FastaFilePath does not have to point to an existing file; only the filename will be used");
				Console.WriteLine();
				Console.WriteLine("RemoteIndexFileSharePath specifies the share where the index files are stored, along with the .MSGFPlusIndexFileInfo file.  The default is " + DEFAULT_REMOTE_SHARE);
				Console.WriteLine();
				Console.WriteLine("Use /X to create the .MSGFPlusIndexFileInfo file in the remote share, using the existing files that were previously manually copied to the share");
				Console.WriteLine();
				Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2013");
				Console.WriteLine("Version: " + GetAppVersion());
				Console.WriteLine();

				Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
				Console.WriteLine("Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov or http://www.sysbio.org/resources/staff/");
				Console.WriteLine();

				// Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
				System.Threading.Thread.Sleep(750);

			}
			catch (Exception ex)
			{
				Console.WriteLine("Error displaying the program syntax: " + ex.Message);
			}

		}


		private static void WriteToErrorStream(string strErrorMessage)
		{
			try
			{
				using (var swErrorStream = new System.IO.StreamWriter(Console.OpenStandardError()))
				{
					swErrorStream.WriteLine(strErrorMessage);
				}
			}
			// ReSharper disable once EmptyGeneralCatchClause
			catch
			{
				// Ignore errors here
			}
		}

	}
}
