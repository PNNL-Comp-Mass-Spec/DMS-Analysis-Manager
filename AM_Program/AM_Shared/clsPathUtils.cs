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
        /// Return the parent directory of directoryPath
        /// Supports both Windows paths and Linux paths
        /// </summary>
        /// <param name="directoryPath">Directory path to examine</param>
        /// <param name="directoryName">Name of the directory in directoryPath but without the parent path</param>
        /// <returns>Parent directory path, or an empty string if no parent</returns>
        /// <remarks>Returns \ or / if the path is rooted and the parent is a path</remarks>
        public static string GetParentDirectoryPath(string directoryPath, out string directoryName)
        {
            if (directoryPath.Contains(Path.DirectorySeparatorChar) && Path.IsPathRooted(directoryPath))
            {
                var directory = new DirectoryInfo(directoryPath);
                directoryName = directory.Name;

                var parent = directory.Parent;
                return parent?.FullName ?? string.Empty;
            }

            char sepChar;
            if (directoryPath.Contains(Path.DirectorySeparatorChar))
                sepChar = Path.DirectorySeparatorChar;
            else
            {
                sepChar = Path.DirectorySeparatorChar == '\\' ? '/' : '\\';
            }

            if (sepChar == '\\')
            {
                // Check for a windows server without a share name
                if (Regex.IsMatch(directoryPath, @"^\\\\[^\\]+\\?$") ||
                    Regex.IsMatch(directoryPath, @"^[a-z]:\\?$"))
                {
                    directoryName = "";
                    return "";
                }
            }
            else
            {
                // sepChar is /
                if (directoryPath == "/")
                {
                    directoryName = "";
                    return "";
                }
            }

            if (directoryPath.EndsWith(sepChar.ToString()))
                directoryPath = directoryPath.TrimEnd(sepChar);

            bool rootedLinuxPath;
            string[] pathParts;
            if (directoryPath.StartsWith(@"/"))
            {
                rootedLinuxPath = true;
                pathParts = directoryPath.Substring(1).Split(sepChar);
            }
            else
            {
                rootedLinuxPath = false;
                pathParts = directoryPath.Split(sepChar);
            }

            if (pathParts.Length == 1)
            {
                directoryName = pathParts[0];
                return rootedLinuxPath ? "/" : "";
            }

            directoryName = pathParts[pathParts.Length - 1];

            var parentPath = directoryPath.Substring(0, directoryPath.Length - directoryName.Length - 1);
            if (rootedLinuxPath && !parentPath.StartsWith("/"))
                return "/" + parentPath;

            return parentPath;

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
