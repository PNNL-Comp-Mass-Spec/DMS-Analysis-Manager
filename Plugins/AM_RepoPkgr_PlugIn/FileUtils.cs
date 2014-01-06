using System.IO;
using System.Linq;

namespace AnalysisManager_RepoPkgr_PlugIn
{
  static class FileUtils
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
      if (!Directory.Exists(destinationFolder)) {
        Directory.CreateDirectory(destinationFolder);
      }
      var dir = new DirectoryInfo(sourceFolder);
      foreach (var fi in dir.GetFiles(searchPattern)) {
        fi.CopyTo(Path.Combine(destinationFolder, fi.Name));
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

    /// <summary>
    /// ensure an empty fasta working directory (create or clear)
    /// </summary>
    /// <param name="orgWDir"> </param>
    /// <returns>Path to fasta working directory</returns>
    public static string SetupWorkDir(string orgWDir)
    {
      if (!Directory.Exists(orgWDir)) {
        Directory.CreateDirectory(orgWDir);
      } else {
        ClearDir(orgWDir);
      }
      return orgWDir;
    }
  }
}
