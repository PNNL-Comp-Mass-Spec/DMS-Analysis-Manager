using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.FileAndDirectoryTools;

namespace AnalysisManager_RepoPkgr_PlugIn
{
    public static class FileUtils
    {
        // Ignore Spelling: gzip, mzid

        public static void CopyFiles(string sourceDirectory, string searchPattern, string destinationDirectory)
        {
            if (!Directory.Exists(destinationDirectory))
            {
                Directory.CreateDirectory(destinationDirectory);
            }
            var dir = new DirectoryInfo(sourceDirectory);

            foreach (var sourceFile in dir.GetFiles(searchPattern))
            {
                sourceFile.CopyTo(Path.Combine(destinationDirectory, sourceFile.Name), true);
            }
        }

        /// <summary>
        /// Delete files according to given filter pattern
        /// from given directory
        /// </summary>
        /// <param name="path">Full path to directory from which files will be deleted</param>
        /// <param name="filter">File matching pattern to select files to delete</param>
        public static void DeleteFiles(string path, string filter)
        {
            var directory = new DirectoryInfo(path);
            directory.GetFiles(filter).ToList().ForEach(f => f.Delete());
        }

        /// <summary>
        /// Look for zipped files in given directory and convert them to gzip.
        /// (conversions are performed in the working directory)
        /// </summary>
        /// <param name="targetDirectoryPath">Full path to directory that contains zipped files to convert</param>
        /// <param name="workDir">Local working directory</param>
        /// <returns>The number of .zip files that were converted to .gz files</returns>
        public static int ConvertZipsToGZips(string targetDirectoryPath, string workDir)
        {
            const int debugLevel = 1;

            // make zipper to work on workDir
            var dotNetZipTools = new DotNetZipTools(debugLevel, workDir);

            var targetDir = new DirectoryInfo(targetDirectoryPath);

            if (!targetDir.Exists)
                return 0;

            // get file handler object to access the workDir
            var workingDirectory = new DirectoryInfo(workDir);

            var filesUpdated = 0;

            // for each zip file in target directory
            foreach (var fileToZip in targetDir.GetFiles("*.zip"))
            {
                // get job prefix from zip file
                var pfx = Regex.Match(fileToZip.Name, @"Job_\d*_").Groups[0].Value;

                // copy zip file to local working directory
                fileToZip.CopyTo(Path.Combine(workDir, fileToZip.Name));

                // unzip it and delete zip
                dotNetZipTools.UnzipFile(Path.Combine(workDir, fileToZip.Name));

                // find the unzipped mzid file
                var mzFiles = workingDirectory.GetFiles("*.mzid");

                if (mzFiles.Length != 1)
                {
                    // oops??
                }

                // gzip the mzid file
                dotNetZipTools.GZipFile(mzFiles[0].FullName, true);

                // get gzip file
                var gzFiles = workingDirectory.GetFiles("*mzid.gz");

                if (gzFiles.Length != 1)
                {
                    // oops??
                }

                // resolve gzip file name
                var gzFileName = gzFiles[0].Name;

                if (!string.IsNullOrEmpty(pfx))
                {
                    gzFileName = pfx + gzFileName;
                    File.Move(gzFiles[0].FullName, Path.Combine(workDir, gzFileName));
                }

                // move the gzip file to target directory
                var targetFilePath = Path.Combine(targetDir.FullName, gzFileName);

                if (File.Exists(targetFilePath))
                    File.Delete(targetFilePath);

                File.Move(Path.Combine(workDir, gzFileName), targetFilePath);

                // get rid of zip file on both sides
                File.Delete(Path.Combine(workDir, fileToZip.Name));
                File.Delete(fileToZip.FullName);

                filesUpdated++;
            }

            return filesUpdated;
        }
    }
}
