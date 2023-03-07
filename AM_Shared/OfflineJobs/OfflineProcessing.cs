using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using PRISM.Logging;

namespace AnalysisManagerBase.OfflineJobs
{
    /// <summary>
    /// Utilize for offline job processing
    /// </summary>
    public static class OfflineProcessing
    {
        /// <summary>
        /// Update files in the TaskQueue directory, renaming the .info file to .success or .fail and appending the completion code and evaluation code info
        /// Also removes the .lock file
        /// </summary>
        /// <param name="infoFilePath">Info file path</param>
        /// <param name="managerName">Manager name</param>
        /// <param name="succeeded">True if the job succeeded</param>
        /// <param name="startTime">Time the analysis started (UTC-based)</param>
        /// <param name="compCode">Integer version of enum CloseOutType specifying the completion code</param>
        /// <param name="compMsg">Completion message</param>
        /// <param name="evalCode">Evaluation code</param>
        /// <param name="evalMsg">Evaluation message</param>
        public static void FinalizeJob(
            string infoFilePath, string managerName,
            bool succeeded, DateTime startTime,
            int compCode, string compMsg,
            int evalCode = 0, string evalMsg = "")
        {
            var infoFile = new FileInfo(infoFilePath);
            var lockFile = new FileInfo(Path.ChangeExtension(infoFile.FullName, Global.LOCK_FILE_EXTENSION));

            if (!infoFile.Exists)
            {
                if (lockFile.Exists)
                {
                    LogTools.LogError("Deleting lock file for missing .info file: " + infoFilePath);
                    lockFile.Delete();
                }

                throw new FileNotFoundException(".info file not found: " + infoFilePath);
            }

            try
            {
                string targetFilePath;

                if (succeeded)
                    targetFilePath = Path.ChangeExtension(infoFilePath, ".success");
                else
                    targetFilePath = Path.ChangeExtension(infoFilePath, ".fail");

                compMsg ??= string.Empty;

                evalMsg ??= string.Empty;

                var settingsToAppend = new SortedSet<string>
                {
                    "Started",
                    "Finished",
                    "CompCode",
                    "CompMsg",
                    "EvalCode",
                    "EvalMsg"
                };

                using (var reader = new StreamReader(new FileStream(infoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        var skipLine = false;

                        foreach (var setting in settingsToAppend)
                        {
                            if (dataLine.StartsWith(setting + "=", StringComparison.OrdinalIgnoreCase))
                                skipLine = true;
                        }

                        if (!skipLine)
                            writer.WriteLine(dataLine);
                    }

                    if (startTime > DateTime.MinValue)
                    {
                        writer.WriteLine("Started=" + startTime.ToLocalTime().ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));
                    }

                    writer.WriteLine("Finished=" + DateTime.Now.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));

                    writer.WriteLine("CompCode=" + compCode);
                    writer.WriteLine("CompMsg=" + compMsg);
                    writer.WriteLine("EvalCode=" + evalCode);
                    writer.WriteLine("EvalMsg=" + evalMsg);
                    writer.WriteLine("MgrName=" + managerName);
                }

                infoFile.Delete();
            }
            catch (Exception ex)
            {
                LogTools.LogError("Error in FinalizeJob", ex);

                if (lockFile.Exists)
                {
                    LogTools.LogMessage("Deleting the lock file, then re-throwing the exception");
                    lockFile.Delete();
                }

                throw;
            }

            // No problems; delete the lock file
            if (lockFile.Exists)
                lockFile.Delete();
        }

        /// <summary>
        /// Rename a file by changing its extension
        /// </summary>
        /// <param name="fileToRename">File to update</param>
        /// <param name="newExtension">New extension, including the leading period</param>
        /// <param name="replaceExisting">When true, replace existing files.  If false, and the new file exists, raises an exception</param>
        public static void RenameFileChangeExtension(FileInfo fileToRename, string newExtension, bool replaceExisting)
        {
            var newFilePath = Path.ChangeExtension(fileToRename.FullName, newExtension);
            var targetFile = new FileInfo(newFilePath);

            if (targetFile.Exists)
            {
                if (replaceExisting)
                {
                    targetFile.Delete();
                }
                else
                {
                    throw new Exception(
                        "Cannot rename file " + fileToRename.FullName + " to " +
                        targetFile.Name + " since target file already exists");
                }
            }

            if (fileToRename.Exists)
            {
                fileToRename.MoveTo(newFilePath);
            }
        }
    }
}
