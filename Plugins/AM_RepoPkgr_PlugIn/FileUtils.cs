using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace AnalysisManager_RepoPkgr_PlugIn
{
    public static class FileUtils
    {
        /// <summary>
        /// Utility method to clear files and folders from given directory
        /// </summary>
        /// <param name="path"></param>
        private static void ClearDir(string path)
        {
            var directory = new DirectoryInfo(path);
            directory.GetFiles().ToList().ForEach(f => f.Delete());
            directory.GetDirectories().ToList().ForEach(d => d.Delete(true));
        }

        public static void CopyFiles(string sourceFolder, string searchPattern, string destinationFolder)
        {
            if (!Directory.Exists(destinationFolder))
            {
                Directory.CreateDirectory(destinationFolder);
            }
            var dir = new DirectoryInfo(sourceFolder);
            foreach (var fi in dir.GetFiles(searchPattern))
            {
                fi.CopyTo(Path.Combine(destinationFolder, fi.Name), true);
            }
        }

        /// <summary>
        /// Delete files accorting to given filter pattern
        /// from given directory
        /// </summary>
        /// <param name="path">Full path to folder from which files will be deleted</param>
        /// <param name="filter">File matching pattern to select files to delete</param>
        public static void DeleteFiles(string path, string filter)
        {
            var directory = new DirectoryInfo(path);
            directory.GetFiles(filter).ToList().ForEach(f => f.Delete());
        }

        // 
        /// <summary>
        /// Look for zipped files in given folder and convert them to gzip.
        /// (conversions are performed in the working directory)
        /// </summary>
        /// <param name="targetDir">Full path to folder that contains zipped files to convert</param>
        /// <param name="workDir">Local working directory</param>
        /// <returns>The number of .zip files that were converted to .gz files</returns>
        public static int ConvertZipsToGZips(string targetDir, string workDir)
        {
            const int debugLevel = 1;

            //  make zipper to work on workDir
            var dotNetZipTools = new AnalysisManagerBase.clsDotNetZipTools(debugLevel, workDir);

            // get file handler object to access the targetDir
            var diTargetDir = new DirectoryInfo(targetDir);

            if (!diTargetDir.Exists)
                return 0;

            // get file handler object to access the workDir
            var diWorkDir = new DirectoryInfo(workDir);

            var filesUpdated = 0;

            // for each zip file in target folder
            foreach (var tarFi in diTargetDir.GetFiles("*.zip"))
            {

                // get job prefix from zip file
                var pfx = Regex.Match(tarFi.Name, @"Job_\d*_").Groups[0].Value;

                // copy zip file to local work dir
                tarFi.CopyTo(Path.Combine(workDir, tarFi.Name));

                // unzip it and delete zip
                dotNetZipTools.UnzipFile(Path.Combine(workDir, tarFi.Name));

                // find the unzipped mzid file
                var mzFiles = diWorkDir.GetFiles("*.mzid");
                if (mzFiles.Length != 1)
                {
                    // oops??
                }

                // gzip the mzid file
                dotNetZipTools.GZipFile(mzFiles[0].FullName, true);

                // get gzip file
                var gzFiles = diWorkDir.GetFiles("*mzid.gz");
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
                var targetFilePath = Path.Combine(targetDir, gzFileName);
                if (File.Exists(targetFilePath))
                    File.Delete(targetFilePath);

                File.Move(Path.Combine(workDir, gzFileName), targetFilePath);

                // get rid of zip file on both sides		  
                File.Delete(Path.Combine(workDir, tarFi.Name));
                File.Delete(tarFi.FullName);

                filesUpdated++;
            }

            return filesUpdated;
        }

    }
}
