using System;
using System.Collections.Generic;
using System.IO;

namespace AnalysisManagerBase
{
    public class clsOfflineProcessing
    {
        /// <summary>
        /// Update files in the TaskQueue folder, renaming the .info file to .success or .fail and appending the completion code and eval code info
        /// Also removes the .lock file
        /// </summary>
        /// <param name="infoFilePath">Info file path</param>
        /// <param name="succeeded">True if the job succeeded</param>
        /// <param name="compCode">Integer version of enum CloseOutType specifying the completion code</param>
        /// <param name="compMsg">Completion message</param>
        /// <param name="evalCode">Evaluation code</param>
        /// <param name="evalMsg">Evaluation messge</param>
        public static void FinalizeJob(string infoFilePath, bool succeeded, int compCode, string compMsg, int evalCode = 0, string evalMsg = "")
        {
            var infoFile = new FileInfo(infoFilePath);
            if (!infoFile.Exists)
                throw new FileNotFoundException(".info file not found: " + infoFilePath);

            string targetFilePath;
            if (succeeded)
                targetFilePath = Path.ChangeExtension(infoFilePath, ".success");
            else
                targetFilePath = Path.ChangeExtension(infoFilePath, ".fail");

            if (compMsg == null)
                compMsg = string.Empty;

            if (evalMsg == null)
                evalMsg = string.Empty;

            var settingsToAppend = new SortedSet<string> {
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

                writer.WriteLine("CompCode=" + compCode);
                writer.WriteLine("CompMsg=" + compMsg);
                writer.WriteLine("EvalCode=" + evalCode);
                writer.WriteLine("EvalMsg=" + evalMsg);
            }

            var lockFile = new FileInfo(Path.ChangeExtension(infoFile.FullName, clsGlobal.LOCK_FILE_EXTENSION));

            infoFile.Delete();

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
                    targetFile.Delete();
                else
                {
                    throw new Exception(
                        "Cannot rename file " + fileToRename.FullName + " to " +
                        targetFile.Name + " since target file already exists");
                }
            }

            fileToRename.MoveTo(newFilePath);
        }

    }
}
