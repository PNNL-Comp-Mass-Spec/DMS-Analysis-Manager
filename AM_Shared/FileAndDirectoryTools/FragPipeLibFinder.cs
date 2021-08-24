using System.IO;
using PRISM;

// ReSharper disable CommentTypo
// ReSharper disable IdentifierTypo
// ReSharper disable UnusedMember.Global

namespace AnalysisManagerBase.FileAndDirectoryTools
{
    /// <summary>
    /// This class is used to determine paths to FragPipe related directories and .jar files
    /// </summary>
    public class FragPipeLibFinder : EventNotifier
    {
        // Ignore Spelling: batmass-io, bruker, crystalc, fragpipe, grppr, ionquant
        // Ignore Spelling: \batmass, \bruker, \fragpipe, \thermo, \tools

        /// <summary>
        /// Relative path to the directory with the MSFragger .jar file
        /// </summary>
        public const string MSFRAGGER_JAR_DIRECTORY_RELATIVE_PATH = @"fragpipe\tools\MSFragger-3.3";

        /// <summary>
        /// Name of the batmass-io .jar file
        /// </summary>
        private const string BATMASS_JAR_NAME = "batmass-io-1.23.4.jar";

        /// <summary>
        /// Name of the Crystal-C .jar file
        /// </summary>
        private const string CRYSTALC_JAR_NAME = "original-crystalc-1.4.2.jar";

        /// <summary>
        /// Name of the grppr .jar file
        /// </summary>
        private const string GRPPR_JAR_NAME = "grppr-0.3.23.jar";

        /// <summary>
        /// Name of the MSFragger .jar file
        /// </summary>
        public const string MSFRAGGER_JAR_NAME = "MSFragger-3.3.jar";

        /// <summary>
        /// Relative path to philosopher.exe (below the fragpipe directory, which should be at C:\DMS_Programs\MSFragger\fragpipe)
        /// </summary>
        public const string PHILOSOPHER_RELATIVE_PATH = @"fragpipe\tools\philosopher\philosopher.exe";

        /// <summary>
        /// Relative path to the percolator .exe (below the fragpipe directory, which should be at C:\DMS_Programs\MSFragger\fragpipe)
        /// </summary>
        public const string PERCOLATOR_RELATIVE_PATH = @"fragpipe\tools\percolator-v3-05.exe";

        /// <summary>
        /// Path to philosopher.exe
        /// </summary>
        public FileInfo PhilosopherExe { get; }

        private DirectoryInfo mFragPipeLibDirectory;

        private DirectoryInfo mFragPipeToolsDirectory;

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
        /// <param name="libDirectory"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindFragPipeLibDirectory(out DirectoryInfo libDirectory)
        {
            // Typically at C:\DMS_Programs\MSFragger\fragpipe\lib

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
        /// Typically at C:\DMS_Programs\MSFragger\fragpipe\tools\
        /// </remarks>
        /// <param name="toolsDirectory"></param>
        /// <returns>True if found, otherwise false</returns>
        private bool FindFragPipeToolsDirectory(out DirectoryInfo toolsDirectory)
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
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileBatmassIO(out FileInfo jarFile)
        {
            // Typically at C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.4.jar

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
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileCommonsMath(out FileInfo jarFile)
        {
            // Typically at C:\DMS_Programs\MSFragger\fragpipe\tools\commons-math3-3.6.1.jar

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
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileCrystalC(out FileInfo jarFile)
        {
            // Typically at C:\DMS_Programs\MSFragger\fragpipe\tools\original-crystalc-1.4.2.jar

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
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileFragPipe(out FileInfo jarFile)
        {
            // Typically at c:\dms_programs\MSFragger\fragpipe\lib\fragpipe-16.0.jar

            if (!FindFragPipeLibDirectory(out var libDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(libDirectory.FullName, "fragpipe-16.0.jar"));
            if (jarFile.Exists)
                return true;

            // ReSharper disable once StringLiteralTypo
            OnErrorEvent("Fragpipe .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the Grppr .jar file
        /// </summary>
        /// <param name="jarFile"></param>
        /// <returns>True if found, otherwise false</returns>
        public bool FindJarFileGrppr(out FileInfo jarFile)
        {
            // Typically at C:\DMS_Programs\MSFragger\fragpipe\tools\grppr-0.3.23.jar

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
            // Typically C:\DMS_Programs\MSFragger\fragpipe\tools\ionquant-1.5.5.jar

            if (!FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                jarFile = null;
                return false;
            }

            jarFile = new FileInfo(Path.Combine(toolsDirectory.FullName, "ionquant-1.5.5.jar"));
            if (jarFile.Exists)
                return true;

            OnErrorEvent("IonQuant .jar file not found: " + jarFile.FullName);
            return false;
        }

        /// <summary>
        /// Find the vendor lib directory
        /// </summary>
        /// <remarks>
        /// Typically at
        /// C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\bruker
        /// and
        /// C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\thermo
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
            // C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\

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
    }
}
