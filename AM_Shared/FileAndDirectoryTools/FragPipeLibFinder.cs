using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISM;

// ReSharper disable CommentTypo
// ReSharper disable IdentifierTypo
// ReSharper disable UnusedMember.Global

namespace AnalysisManagerBase.FileAndDirectoryTools
{
    /// <summary>
    /// This class is used to determine paths to FragPipe related directories and .jar files
    /// It also includes methods to find the newest version of Python 3
    /// </summary>
    public class FragPipeLibFinder : EventNotifier
    {
        // Ignore Spelling: batmass-io, bruker, cd, cpp, crystalc, diann, frag, fragpipe, grppr, ionquant
        // Ignore Spelling: javacpp, jfreechart, msbooster, MSFragger, PTM, ptmshepherd, quant, tmt, usr
        // Ignore Spelling: \batmass, \bruker, \fragpipe, \thermo, \tools

        /// <summary>
        /// Name of the batmass-io .jar file
        /// </summary>
        private const string BATMASS_JAR_NAME = "batmass-io-1.30.0.jar";

        /// <summary>
        /// Name of the Crystal-C .jar file
        /// </summary>
        private const string CRYSTALC_JAR_NAME = "original-crystalc-1.5.2.jar";

        /// <summary>
        /// Relative path to the Data-Independent Acquisition by Neural Networks (DIA-NN) executable
        /// (below the fragpipe directory, which should be at C:\DMS_Programs\MSFragger\fragpipe)
        /// </summary>
        public const string DIANN_RELATIVE_PATH = @"fragpipe\tools\diann\1.8.2_beta_8\win\DiaNN.exe";

        /// <summary>
        /// Name of the FragPipe .jar file
        /// </summary>
        private const string FRAGPIPE_JAR_NAME = "fragpipe-21.1.jar";

        /// <summary>
        /// Name of the grppr .jar file
        /// </summary>
        private const string GRPPR_JAR_NAME = "grppr-0.3.23.jar";

        /// <summary>
        /// Name of the IonQuant jar file
        /// </summary>
        private const string IONQUANT_JAR_NAME = "IonQuant-1.10.27.jar";

        /// <summary>
        /// Name of the Java C++ presets platform directory
        /// </summary>
        [Obsolete("Deprecated with FragPipe v18")]
        private const string JAVA_CPP_PRESETS_DIRECTORY_NAME = "javacpp-presets-platform-1.5.6-bin";

        /// <summary>
        /// Name of the JFreeChart jar file
        /// </summary>
        private const string JFREECHART_JAR_NAME = "jfreechart-1.5.3.jar";

        /// <summary>
        /// Name of the MSBooster jar file
        /// </summary>
        private const string MSBOOSTER_JAR_NAME = "msbooster-1.1.28.jar";

        /// <summary>
        /// Relative path to the directory with the MSFragger .jar file
        /// </summary>
        public const string MSFRAGGER_JAR_DIRECTORY_RELATIVE_PATH = @"fragpipe\tools\MSFragger-4.0";

        /// <summary>
        /// Name of the MSFragger .jar file
        /// </summary>
        public const string MSFRAGGER_JAR_NAME = "MSFragger-4.0.jar";

        /// <summary>
        /// Relative path to philosopher.exe (below the fragpipe directory, which should be at C:\DMS_Programs\MSFragger\fragpipe)
        /// </summary>
        public const string PHILOSOPHER_RELATIVE_PATH = @"fragpipe\tools\philosopher\philosopher.exe";

        /// <summary>
        /// Relative path to the percolator .exe (below the fragpipe directory, which should be at C:\DMS_Programs\MSFragger\fragpipe)
        /// </summary>
        public const string PERCOLATOR_RELATIVE_PATH = @"fragpipe\tools\percolator_3_6_4\windows\percolator.exe";

        /// <summary>
        /// Relative path to the PTM Prophet .exe (below the fragpipe directory, which should be at C:\DMS_Programs\MSFragger\fragpipe)
        /// </summary>
        public const string PTM_PROPHET_RELATIVE_PATH = @"fragpipe\tools\PTMProphet\PTMProphetParser.exe";

        /// <summary>
        /// Name of the PTM Shepherd jar file
        /// </summary>
        private const string PTMSHEPHERD_JAR_NAME = "ptmshepherd-2.0.6.jar";

        /// <summary>
        /// Name of the smile-core library
        /// </summary>
        [Obsolete("Deprecated with FragPipe v18")]
        private const string SMILE_CORE_JAR_NAME = "smile-core-2.6.0.jar";

        /// <summary>
        /// Name of the smile-math library
        /// </summary>
        [Obsolete("Deprecated with FragPipe v18")]
        private const string SMILE_MATH_JAR_NAME = "smile-math-2.6.0.jar";

        /// <summary>
        /// Relative path to the TMT integrator jar file
        /// </summary>
        public const string TMT_INTEGRATOR_JAR_RELATIVE_PATH = @"fragpipe\tools\tmt-integrator-5.0.7.jar";

        private DirectoryInfo mFragPipeLibDirectory;

        private DirectoryInfo mFragPipeToolsDirectory;

        /// <summary>
        /// Path to philosopher.exe
        /// </summary>
        public FileInfo PhilosopherExe { get; }

        /// <summary>
        /// Path to the python executable
        /// </summary>
        public static string PythonPath { get; private set; }

        /// <summary>
        /// True if the Python .exe could be found, otherwise false
        /// </summary>
        public static bool PythonInstalled => FindPython();

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="philosopherExe">Path to philosopher.exe</param>
        public FragPipeLibFinder(FileInfo philosopherExe)
        {
            PhilosopherExe = philosopherExe;
        }

        /// <summary>
        /// Find the FragPipe lib directory
        /// </summary>
        /// <remarks>
        /// Typically at C:\DMS_Programs\MSFragger\fragpipe\lib
        /// </remarks>
        /// <param name="libDirectory"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindFragPipeLibDirectory(out DirectoryInfo libDirectory)
        {
            if (mFragPipeLibDirectory != null)
            {
                libDirectory = mFragPipeLibDirectory;
                return true;
            }

            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                libDirectory = null;
                return false;
            }

            var fragPipeDirectory = toolsDirectory.Parent;

            if (fragPipeDirectory == null)
            {
                OnErrorEvent("Unable to determine the parent directory of " + toolsDirectory.FullName);
                libDirectory = null;
                return false;
            }

            libDirectory = new DirectoryInfo(Path.Combine(fragPipeDirectory.FullName, "lib"));

            if (libDirectory.Exists)
            {
                mFragPipeLibDirectory = libDirectory;
                return true;
            }

            OnErrorEvent("FragPipe lib directory not found: " + libDirectory.FullName);
            return false;
        }

        /// <summary>
        /// Find the FragPipe tools directory
        /// </summary>
        /// <remarks>
        /// Typically at C:\DMS_Programs\MSFragger\fragpipe\tools
        /// </remarks>
        /// <param name="toolsDirectory"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindFragPipeToolsDirectory(out DirectoryInfo toolsDirectory)
        {
            if (mFragPipeToolsDirectory != null)
            {
                toolsDirectory = mFragPipeToolsDirectory;
                return true;
            }

            // PhilosopherExe tracks file philosopher.exe, for example
            // C:\DMS_Programs\MSFragger\fragpipe\tools\philosopher\philosopher.exe

            if (PhilosopherExe.Directory == null)
            {
                OnErrorEvent("Unable to determine the parent directory of " + PhilosopherExe.FullName);
                toolsDirectory = null;
                return false;
            }

            toolsDirectory = PhilosopherExe.Directory.Parent;

            if (toolsDirectory == null)
            {
                OnErrorEvent("Unable to determine the parent directory of " + PhilosopherExe.Directory.FullName);
                toolsDirectory = null;
                return false;
            }

            if (toolsDirectory.Exists)
            {
                mFragPipeToolsDirectory = toolsDirectory;
                return true;
            }

            OnErrorEvent("FragPipe tools directory not found: " + toolsDirectory.FullName);
            return false;
        }

        /// <summary>
        /// Find the Batmass IO .jar file
        /// </summary>
        /// <remarks>
        /// Typically at C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.30.0.jar
        /// </remarks>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileBatmassIO(out FileInfo jarFile)
        {
            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, BATMASS_JAR_NAME));

            if (jarFile.Exists)
                return true;

            // ReSharper disable once StringLiteralTypo
            OnErrorEvent("Batmass IO .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Commons-Math3 .jar file
        /// </summary>
        /// <remarks>
        /// Typically at C:\DMS_Programs\MSFragger\fragpipe\tools\commons-math3-3.6.1.jar
        /// </remarks>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileCommonsMath(out FileInfo jarFile)
        {
            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, "commons-math3-3.6.1.jar"));

            if (jarFile.Exists)
                return true;

            OnErrorEvent("Commons-Math3 .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the Crystal-C .jar file
        /// </summary>
        /// <remarks>
        /// Typically at C:\DMS_Programs\MSFragger\fragpipe\tools\original-crystalc-1.4.2.jar
        /// </remarks>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileCrystalC(out FileInfo jarFile)
        {
            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, CRYSTALC_JAR_NAME));

            if (jarFile.Exists)
                return true;

            OnErrorEvent("Crystal-C .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the fragpipe .jar file
        /// </summary>
        /// <remarks>
        /// Typically at C:\DMS_Programs\MSFragger\fragpipe\lib\fragpipe-21.1.jar
        /// </remarks>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileFragPipe(out FileInfo jarFile)
        {
            if (!FindFragPipeLibDirectory(out var libDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(libDirectory.FullName, FRAGPIPE_JAR_NAME));

            if (jarFile.Exists)
                return true;

            // ReSharper disable once StringLiteralTypo
            OnErrorEvent("Fragpipe .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the Grppr .jar file
        /// </summary>
        /// <remarks>
        /// Typically at C:\DMS_Programs\MSFragger\fragpipe\tools\grppr-0.3.23.jar
        /// </remarks>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileGrppr(out FileInfo jarFile)
        {
            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, GRPPR_JAR_NAME));

            if (jarFile.Exists)
                return true;

            // ReSharper disable once StringLiteralTypo
            OnErrorEvent("Grppr .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the IonQuant .jar file
        /// </summary>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileIonQuant(out FileInfo jarFile)
        {
            // Typically C:\DMS_Programs\MSFragger\fragpipe\tools\IonQuant-1.10.27.jar

            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, IONQUANT_JAR_NAME));

            if (jarFile.Exists)
                return true;

            OnErrorEvent("IonQuant .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the JFreeChart .jar file
        /// </summary>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileJFreeChart(out FileInfo jarFile)
        {
            // Typically C:\DMS_Programs\MSFragger\fragpipe\tools\jfreechart-1.5.3.jar

            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, JFREECHART_JAR_NAME));

            if (jarFile.Exists)
                return true;

            OnErrorEvent("JFreeChart .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the MSBooster .jar file
        /// </summary>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileMSBooster(out FileInfo jarFile)
        {
            // Typically C:\DMS_Programs\MSFragger\fragpipe\tools\msbooster-1.1.28.jar

            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, MSBOOSTER_JAR_NAME));

            if (jarFile.Exists)
                return true;

            OnErrorEvent("MSBooster .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the PtmShepherd .jar file
        /// </summary>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFilePtmShepherd(out FileInfo jarFile)
        {
            // Typically C:\DMS_Programs\MSFragger\fragpipe\tools\ptmshepherd-1.2.6.jar

            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, PTMSHEPHERD_JAR_NAME));

            if (jarFile.Exists)
                return true;

            OnErrorEvent("PtmShepherd .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the smile-core .jar file
        /// </summary>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        [Obsolete("Deprecated with FragPipe v18")]
        public bool FindJarFileSmileCore(out FileInfo jarFile)
        {
            // Typically C:\DMS_Programs\MSFragger\fragpipe\tools\smile-core-2.6.0.jar

            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, SMILE_CORE_JAR_NAME));

            if (jarFile.Exists)
                return true;

            OnErrorEvent("smile-core .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the smile-math .jar file
        /// </summary>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        [Obsolete("Deprecated with FragPipe v18")]
        public bool FindJarFileSmileMath(out FileInfo jarFile)
        {
            // Typically C:\DMS_Programs\MSFragger\fragpipe\tools\smile-math-2.6.0.jar

            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, SMILE_MATH_JAR_NAME));

            if (jarFile.Exists)
                return true;

            OnErrorEvent("smile-math .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the Java C++ presets platform directory
        /// </summary>
        /// <param name="cppPresetsDirectory"></param>
        /// <returns>True if found, otherwise false</returns>
        [Obsolete("Deprecated with FragPipe v18")]
        public bool FindCppPresetsPlatformDirectory(out DirectoryInfo cppPresetsDirectory)
        {
            // Typically C:\DMS_Programs\MSFragger\fragpipe\tools\javacpp-presets-platform-1.5.6-bin

            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                cppPresetsDirectory = null;
                return false;
            }

            cppPresetsDirectory = new DirectoryInfo(Path.Combine(toolsDirectory.FullName, JAVA_CPP_PRESETS_DIRECTORY_NAME));

            if (cppPresetsDirectory.Exists)
                return true;

            OnErrorEvent("Java C++ presets platform directory not found: " + cppPresetsDirectory.FullName);
            return false;
        }

        /// <summary>
        /// Find the best candidate directory with Python 3.x
        /// </summary>
        /// <returns>True if Python could be found, otherwise false</returns>
        private static bool FindPython()
        {
            if (!string.IsNullOrWhiteSpace(PythonPath))
                return true;

            if (SystemInfo.IsLinux)
            {
                PythonPath = "/usr/bin/python3";
                ConsoleMsgUtils.ShowDebug("Assuming Python 3 is at {0}", PythonPath);
                return true;
            }

            foreach (var directoryPath in PythonPathsToCheck())
            {
                var exePath = FindPythonExe(directoryPath);

                if (string.IsNullOrWhiteSpace(exePath))
                    continue;

                PythonPath = exePath;
                break;
            }

            return !string.IsNullOrWhiteSpace(PythonPath);
        }

        /// <summary>
        /// Find the best candidate directory with Python 3.x
        /// </summary>
        /// <returns>Path to the python executable, otherwise an empty string</returns>
        private static string FindPythonExe(string directoryPath)
        {
            var directory = new DirectoryInfo(directoryPath);

            if (!directory.Exists)
                return string.Empty;

            var subDirectories = directory.GetDirectories("Python3*").ToList();
            subDirectories.AddRange(directory.GetDirectories("Python 3*"));
            subDirectories.Add(directory);

            var candidates = new List<FileInfo>();

            foreach (var subDirectory in subDirectories)
            {
                var files = subDirectory.GetFiles("python.exe");

                if (files.Length == 0)
                    continue;

                candidates.Add(files.First());
            }

            if (candidates.Count == 0)
                return string.Empty;

            // Find the newest .exe
            var query = (from item in candidates orderby item.LastWriteTime select item.FullName);

            return query.First();
        }

        /// <summary>
        /// Find the vendor lib directory
        /// </summary>
        /// <remarks>
        /// Typically at
        /// C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.0\ext\bruker
        /// and
        /// C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.0\ext\thermo
        /// </remarks>
        /// <param name="vendorName">Vendor name: either bruker or thermo</param>
        /// <param name="vendorLibDirectory">Output: directory info, if found</param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindVendorLibDirectory(string vendorName, out DirectoryInfo vendorLibDirectory)
        {
            // Look for the FragPipe tools directory, e.g.
            // C:\DMS_Programs\MSFragger\fragpipe\tools\

            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                vendorLibDirectory = null;
                return false;
            }

            // Look for the MSFragger external library directory, e.g.
            // C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.0\ext\

            var msfraggerDirectoryName = Path.GetFileName(MSFRAGGER_JAR_DIRECTORY_RELATIVE_PATH);

            var msFraggerExtDirectory = new DirectoryInfo(Path.Combine(toolsDirectory.FullName, msfraggerDirectoryName, "ext"));

            if (!msFraggerExtDirectory.Exists)
            {
                OnErrorEvent("MSFragger external library directory not found: " + msFraggerExtDirectory.FullName);
                vendorLibDirectory = null;
                return false;
            }

            // Append the vendor name
            vendorLibDirectory = new DirectoryInfo(Path.Combine(msFraggerExtDirectory.FullName, vendorName));

            if (vendorLibDirectory.Exists)
                return true;

            OnErrorEvent("MSFragger vendor lib directory not found: " + vendorLibDirectory.FullName);
            return false;
        }

        /// <summary>
        /// Look for a subdirectory below the site-packages directory for the given Python package
        /// </summary>
        /// <param name="packageName"></param>
        /// <param name="errorMessage"></param>
        /// <returns>True if the package was found, otherwise false</returns>
        public static bool PythonPackageInstalled(string packageName, out string errorMessage)
        {
            var pythonExe = new FileInfo(PythonPath);

            if (!pythonExe.Exists)
            {
                errorMessage = "Python executable not found: " + PythonPath;
                return false;
            }

            if (pythonExe.Directory == null)
            {
                errorMessage = "Unable to determine the parent directory of the Python executable: " + PythonPath;
                return false;
            }

            var sitePackagesDirectory = new DirectoryInfo(Path.Combine(pythonExe.Directory.FullName, "Lib", "site-packages"));

            if (!sitePackagesDirectory.Exists)
            {
                errorMessage = "Python site-packages directory not found: " + sitePackagesDirectory.FullName;
                return false;
            }

            var packageDirectory = new DirectoryInfo(Path.Combine(sitePackagesDirectory.FullName, packageName));

            if (!packageDirectory.Exists)
            {
                errorMessage = string.Format(
                    "Python package {0} is not installed; install using \"cd {1} \" then \"pip install {0}\"",
                    packageName, pythonExe.Directory.FullName);

                return false;
            }

            errorMessage = string.Empty;
            return true;
        }

        /// <summary>
        /// Obtain a list of directories to search for a Python subdirectory
        /// </summary>
        private static IEnumerable<string> PythonPathsToCheck()
        {
            return new List<string>
            {
                Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles),
                Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86),
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "Programs"),
                @"C:\ProgramData\Anaconda3",
                @"C:\"
            };
        }
    }
}
