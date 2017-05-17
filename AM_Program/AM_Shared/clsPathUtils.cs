using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AnalysisManagerBase
{
    public static class clsPathUtils
    {
        public static string AssureLinuxPath(string pathSpec)
        {
            return pathSpec.Replace('\\', '/');
        }

        public static string AssureWindowsPath(string pathSpec)
        {
            return pathSpec.Replace('/', '\\');
        }

        public static string CombinePathsLocalSepChar(string path1, string path2)
        {
            return CombinePaths(path1, path2, Path.DirectorySeparatorChar);
        }

        public static string CombineLinuxPaths(string path1, string path2)
        {
            return CombinePaths(path1, path2, '/');
        }

        public static string CombineWindowsPaths(string path1, string path2)
        {
            return CombinePaths(path1, path2, '\\');
        }

        public static string CombinePaths(string path1, string path2, char directorySepChar)
        {
            if (path1 == null || path2 == null)
                throw new ArgumentNullException((path1 == null) ? "path1" : "path2");

            if (string.IsNullOrWhiteSpace(path2))
                return path1;

            if (string.IsNullOrWhiteSpace(path1))
                return path2;

            if (Path.IsPathRooted(path2))
                return path2;

            var ch = path1[path1.Length - 1];
            if (ch != Path.DirectorySeparatorChar && ch != Path.AltDirectorySeparatorChar && ch != Path.VolumeSeparatorChar)
                return path1 + directorySepChar + path2;

            return path1 + path2;

        }

        /// <summary>
        /// Check a filename against a filemask (like * or *.txt or MSGF*)
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="fileMask"></param>
        /// <returns>True if a match, otherwise false</returns>
        /// <remarks>From "http://stackoverflow.com/questions/725341/how-to-determine-if-a-file-matches-a-file-mask/19655824#19655824"</remarks>
        public static bool FitsMask(string fileName, string fileMask)
        {
            var convertedMask = "^" + Regex.Escape(fileMask).Replace(@"\*", ".*").Replace(@"\?", ".") + "$";
            var regexMask = new Regex(convertedMask, RegexOptions.IgnoreCase);
            return regexMask.IsMatch(fileName);
        }

        /// <summary>
        /// Examines strPath to look for spaces
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns>filePath as-is if no spaces, otherwise filePath surrounded by double quotes </returns>
        /// <remarks></remarks>
        public static string PossiblyQuotePath(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
            {
                return string.Empty;

            }

            if (!filePath.Contains(" "))
                return filePath;

            if (!filePath.StartsWith("\""))
            {
                filePath = "\"" + filePath;
            }

            if (!filePath.EndsWith("\""))
            {
                filePath += "\"";
            }

            return filePath;
        }

        public static string ReplaceFilenameInPath(string existingFilePath, string newFileName)
        {
            if (string.IsNullOrWhiteSpace(existingFilePath))
                return newFileName;

            var existingFile = new FileInfo(existingFilePath);
            if (existingFile.DirectoryName == null)
                return newFileName;

            return Path.Combine(existingFile.DirectoryName, newFileName);
        }
    }
}
