using Microsoft.Extensions.FileSystemGlobbing;
using PRISM;

namespace AnalysisManagerBase.FileAndDirectoryTools
{
    /// <summary>
    /// Extension methods for <see cref="ZipFileTools"/>
    /// </summary>
    public static class ZipFileToolsExtensions
    {
        /// <summary>
        /// Unzip zipFilePath into the specified target directory, applying the specified file filter using file name globbing
        /// </summary>
        /// <param name="zipTools"><see cref="ZipFileTools"/> instance</param>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Directory to place the unzipped files</param>
        /// <param name="fileFilter">Filter to apply when unzipping. Use file name globbing</param>
        /// <returns>True if success, false if an error</returns>
        public static bool UnzipFileGlob(this ZipFileTools zipTools, string zipFilePath, string targetDirectory, string fileFilter)
        {
            if (string.IsNullOrWhiteSpace(fileFilter))
            {
                return zipTools.UnzipFile(zipFilePath, targetDirectory, fileFilter);
            }

            // Microsoft.Extensions.FileSystemGlobbing.Matcher
            Matcher matcher = new();
            matcher.AddInclude(fileFilter);

            return zipTools.UnzipFile(zipFilePath, targetDirectory, name => matcher.Match(name).HasMatches);
        }
    }
}
