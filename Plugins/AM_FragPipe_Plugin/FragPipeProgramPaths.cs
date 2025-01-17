using System.IO;

namespace AnalysisManagerFragPipePlugin
{
    internal class FragPipeProgramPaths
    {
        // Ignore Spelling: Frag

        /// <summary>
        /// Path to DiaNN.exe
        /// </summary>
        /// <remarks>
        /// C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\diann\1.8.2_beta_8\win\DiaNN.exe
        /// </remarks>
        public FileInfo DiannExe { get; set; } = new("DiaNN.exe");

        /// <summary>
        /// Path to python.exe
        /// </summary>
        /// <remarks>
        /// C:\Python3\python.exe
        /// </remarks>
        public FileInfo PythonExe { get; set; } = new("python.exe");

        /// <summary>
        /// Path to the tools directory below the FragPipe instance directory
        /// </summary>
        /// <remarks>
        /// C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools
        /// </remarks>
        public DirectoryInfo ToolsDirectory { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="toolsDirectory">Path to the tools directory below the FragPipe instance directory</param>
        public FragPipeProgramPaths(DirectoryInfo toolsDirectory)
        {
            ToolsDirectory = toolsDirectory;
        }
    }
}
