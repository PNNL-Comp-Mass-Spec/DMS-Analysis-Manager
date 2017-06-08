//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 05/12/2015
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using AnalysisManagerBase;

namespace AnalysisManagerMODPlusPlugin
{
    /// <summary>
    /// Class for running MODPlus
    /// </summary>
    /// <remarks></remarks>
    public class clsAnalysisToolRunnerMODPlus : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        protected const string MODPlus_CONSOLE_OUTPUT = "MODPlus_ConsoleOutput.txt";
        protected const string MODPlus_JAR_NAME = "modp_pnnl.jar";
        protected const string TDA_PLUS_JAR_NAME = "tda_plus.jar";

        protected const float PROGRESS_PCT_CONVERTING_MSXML_TO_MGF = 1;
        protected const float PROGRESS_PCT_SPLITTING_MGF = 3;
        protected const float PROGRESS_PCT_MODPLUS_STARTING = 5;
        protected const float PROGRESS_PCT_MODPLUS_COMPLETE = 95;
        protected const float PROGRESS_PCT_COMPUTING_FDR = 96;

        #endregion

        #region "Module Variables"

        protected bool mToolVersionWritten;
        protected string mMODPlusVersion;

        protected string mMODPlusProgLoc;
        protected string mConsoleOutputErrorMsg;

        /// <summary>
        /// Dictionary of ModPlus instances
        /// </summary>
        /// <remarks>Key is core number (1 through NumCores), value is the instance</remarks>
        protected Dictionary<int, clsMODPlusRunner> mMODPlusRunners;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MODPlus
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerMODPlus.RunTool(): Enter");
                }

                // Verify that program files exist

                // JavaProgLoc will typically be "C:\Program Files\Java\jre8\bin\java.exe"
                var javaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrEmpty(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MODPlus program
                mMODPlusProgLoc = DetermineProgramLocation("MODPlusProgLoc", MODPlus_JAR_NAME);

                if (string.IsNullOrWhiteSpace(mMODPlusProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!InitializeFastaFile())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                Dictionary<int, string> paramFileList = null;

                // Run MODPlus (using multiple threads)
                var processingSuccess = StartMODPlus(javaProgLoc, out paramFileList);

                if (processingSuccess)
                {
                    // Look for the results file(s)
                    var postProcessSuccess = PostProcessMODPlusResults(paramFileList);
                    if (!postProcessSuccess)
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            LogError("Unknown error post-processing the MODPlus results");
                        }
                        processingSuccess = false;
                    }
                }

                m_progress = PROGRESS_PCT_MODPLUS_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                Thread.Sleep(500);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                LogError("Error in MODPlusPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Add a node given a simple xpath expression, for example "search/database" or "search/parameters/fragment_ion_tol"
        /// </summary>
        /// <param name="doc"></param>
        /// <param name="xpath"></param>
        /// <param name="attributeName"></param>
        /// <param name="attributeValue"></param>
        /// <remarks></remarks>
        private void AddXMLElement(XmlDocument doc, string xpath, string attributeName, string attributeValue)
        {
            var attributes = new Dictionary<string, string>();
            attributes.Add(attributeName, attributeValue);

            AddXMLElement(doc, xpath, attributes);
        }

        /// <summary>
        /// Add a node given a simple xpath expression, for example "search/database" or "search/parameters/fragment_ion_tol"
        /// </summary>
        /// <param name="doc"></param>
        /// <param name="xpath"></param>
        /// <param name="attributes"></param>
        /// <remarks></remarks>
        private void AddXMLElement(XmlDocument doc, string xpath, Dictionary<string, string> attributes)
        {
            MakeXPath(doc, xpath, attributes);
        }

        /// <summary>
        /// Use MSConvert to convert the .mzXML or .mzML file to a .mgf file
        /// </summary>
        /// <param name="fiSpectrumFile"></param>
        /// <param name="fiMgfFile"></param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool ConvertMsXmlToMGF(FileInfo fiSpectrumFile, FileInfo fiMgfFile)
        {
            // Set up and execute a program runner to run MSConvert

            var msConvertProgLoc = DetermineProgramLocation("ProteoWizardDir", "msconvert.exe");
            if (string.IsNullOrWhiteSpace(msConvertProgLoc))
            {
                if (string.IsNullOrWhiteSpace(m_message))
                {
                    LogError("Manager parameter ProteoWizardDir was not found; cannot run MSConvert.exe");
                }
                return false;
            }

            var msConvertConsoleOutput = Path.Combine(m_WorkDir, "MSConvert_ConsoleOutput.txt");
            m_jobParams.AddResultFileToSkip(msConvertConsoleOutput);

            var cmdStr = " --mgf";
            cmdStr += " --outfile " + fiMgfFile.FullName;
            cmdStr += " " + PossiblyQuotePath(fiSpectrumFile.FullName);

            if (m_DebugLevel >= 1)
            {
                // C:\DMS_Programs\ProteoWizard\msconvert.exe --mgf --outfile Dataset.mgf Dataset.mzML
                LogDebug(msConvertProgLoc + " " + cmdStr);
            }

            var msConvertRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(msConvertRunner);
            msConvertRunner.LoopWaiting += MSConvert_CmdRunner_LoopWaiting;

            msConvertRunner.CreateNoWindow = true;
            msConvertRunner.CacheStandardOutput = false;
            msConvertRunner.EchoOutputToConsole = true;

            msConvertRunner.WriteConsoleOutputToFile = true;
            msConvertRunner.ConsoleOutputFilePath = msConvertConsoleOutput;

            m_progress = PROGRESS_PCT_CONVERTING_MSXML_TO_MGF;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var success = msConvertRunner.RunProgram(msConvertProgLoc, cmdStr, "MSConvert", true);

            if (success)
            {
                if (m_DebugLevel >= 2)
                {
                    LogDebug("MSConvert.exe successfully created " + fiMgfFile.Name);
                }
                return true;
            }

            LogError("Error running MSConvert");

            if (msConvertRunner.ExitCode != 0)
            {
                LogWarning("MSConvert returned a non-zero exit code: " + msConvertRunner.ExitCode);
            }
            else
            {
                LogWarning("Call to MSConvert failed (but exit code is 0)");
            }

            return false;
        }

        public override void CopyFailedResultsToArchiveFolder()
        {
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION);

            base.CopyFailedResultsToArchiveFolder();
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="paramFileName"></param>
        /// <param name="fastaFilePath"></param>
        /// <param name="mgfFiles"></param>
        /// <returns>Dictionary where key is the thread number and value is the parameter file path</returns>
        /// <remarks></remarks>
        private Dictionary<int, string> CreateParameterFiles(string paramFileName, string fastaFilePath, IEnumerable<FileInfo> mgfFiles)
        {
            try
            {
                var fiParamFile = new FileInfo(Path.Combine(m_WorkDir, paramFileName));
                if (!fiParamFile.Exists)
                {
                    LogError("Parameter file not found by CreateParameterFiles");
                    return new Dictionary<int, string>();
                }

                var doc = new XmlDocument();
                doc.Load(fiParamFile.FullName);

                DefineParamfileDatasetAndFasta(doc, fastaFilePath);

                DefineParamMassResolutionSettings(doc);

                var paramFileList = CreateThreadParamFiles(fiParamFile, doc, mgfFiles);

                return paramFileList;
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateParameterFiles", ex);
                return new Dictionary<int, string>();
            }
        }

        private Dictionary<int, string> CreateThreadParamFiles(FileInfo fiMasterParamFile, XmlDocument doc, IEnumerable<FileInfo> mgfFiles)
        {
            var reThreadNumber = new Regex(@"_Part(\d+)\.mgf", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var paramFileList = new Dictionary<int, string>();

            foreach (var fiMgfFile in mgfFiles)
            {
                var reMatch = reThreadNumber.Match(fiMgfFile.Name);
                if (!reMatch.Success)
                {
                    LogError("RegEx failed to extract the thread number from the MGF file name: " + fiMgfFile.Name);
                    return new Dictionary<int, string>();
                }

                var threadNumber = 0;
                if (!int.TryParse(reMatch.Groups[1].Value, out threadNumber))
                {
                    LogError("RegEx logic error extracting the thread number from the MGF file name: " + fiMgfFile.Name);
                    return new Dictionary<int, string>();
                }

                if (paramFileList.ContainsKey(threadNumber))
                {
                    LogError("MGFSplitter logic error; duplicate thread number encountered for " + fiMgfFile.Name);
                    return new Dictionary<int, string>();
                }

                var nodeList = doc.SelectNodes("/search/dataset");
                if (nodeList.Count > 0)
                {
                    nodeList[0].Attributes["local_path"].Value = fiMgfFile.FullName;
                    nodeList[0].Attributes["format"].Value = "mgf";
                }

                var paramFileName = Path.GetFileNameWithoutExtension(fiMasterParamFile.Name) + "_Part" + threadNumber + ".xml";
                var paramFilePath = Path.Combine(fiMasterParamFile.DirectoryName, paramFileName);

                using (var objXmlWriter = new XmlTextWriter(new FileStream(paramFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), new UTF8Encoding(false)))
                {
                    objXmlWriter.Formatting = Formatting.Indented;
                    objXmlWriter.Indentation = 4;

                    doc.WriteTo(objXmlWriter);
                }

                paramFileList.Add(threadNumber, paramFilePath);

                m_jobParams.AddResultFileToSkip(paramFilePath);
            }

            return paramFileList;
        }

        private void DefineParamfileDatasetAndFasta(XmlDocument doc, string fastaFilePath)
        {
            // Define the path to the dataset file
            var nodeList = doc.SelectNodes("/search/dataset");
            if (nodeList.Count > 0)
            {
                // This value will get updated to the correct name later in this function
                nodeList[0].Attributes["local_path"].Value = "Dataset_PartX.mgf";
                nodeList[0].Attributes["format"].Value = "mgf";
            }
            else
            {
                // Match not found; add it
                var attributes = new Dictionary<string, string>();
                attributes.Add("local_path", "Dataset_PartX.mgf");
                attributes.Add("format", "mgf");
                AddXMLElement(doc, "/search/dataset", attributes);
            }

            // Define the path to the fasta file
            nodeList = doc.SelectNodes("/search/database");
            if (nodeList.Count > 0)
            {
                nodeList[0].Attributes["local_path"].Value = fastaFilePath;
            }
            else
            {
                // Match not found; add it
                AddXMLElement(doc, "/search/database", "local_path", fastaFilePath);
            }
        }

        private void DefineParamMassResolutionSettings(XmlDocument doc)
        {
            const string LOW_RES_FLAG = "low";
            const string HIGH_RES_FLAG = "high";

            const float MIN_FRAG_TOL_LOW_RES = 0.3f;
            const string DEFAULT_FRAG_TOL_LOW_RES = "0.5";
            const string DEFAULT_FRAG_TOL_HIGH_RES = "0.05";

            // Validate the setting for instrument_resolution and fragment_ion_tol

            var strDatasetType = m_jobParams.GetParam("JobParameters", "DatasetType");
            var instrumentResolutionMsMs = LOW_RES_FLAG;

            if (strDatasetType.ToLower().EndsWith("hmsn"))
            {
                instrumentResolutionMsMs = HIGH_RES_FLAG;
            }

            var nodeList = doc.SelectNodes("/search/instrument_resolution");
            if (nodeList.Count > 0)
            {
                if (nodeList[0].Attributes["msms"].Value == HIGH_RES_FLAG && instrumentResolutionMsMs == "low")
                {
                    // Parameter file lists the resolution as high, but it's actually low
                    // Auto-change it
                    nodeList[0].Attributes["msms"].Value = instrumentResolutionMsMs;
                    m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, "Auto-switched to low resolution mode for MS/MS data");
                    LogWarning(m_EvalMessage);
                }
            }
            else
            {
                // Match not found; add it
                var attributes = new Dictionary<string, string>();
                attributes.Add("ms", HIGH_RES_FLAG);
                attributes.Add("msms", instrumentResolutionMsMs);
                AddXMLElement(doc, "/search/instrument_resolution", attributes);
            }

            nodeList = doc.SelectNodes("/search/parameters/fragment_ion_tol");
            if (nodeList.Count > 0)
            {
                if (instrumentResolutionMsMs == LOW_RES_FLAG)
                {
                    double massTolDa = 0;
                    if (double.TryParse(nodeList[0].Attributes["value"].Value, out massTolDa))
                    {
                        var massUnits = nodeList[0].Attributes["unit"].Value;

                        if (massUnits == "ppm")
                        {
                            // Convert from ppm to Da
                            massTolDa = massTolDa * 1000 / 1000000;
                        }

                        if (massTolDa < MIN_FRAG_TOL_LOW_RES)
                        {
                            m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, "Auto-changed fragment_ion_tol to " + DEFAULT_FRAG_TOL_LOW_RES + " Da since low resolution MS/MS");
                            nodeList[0].Attributes["value"].Value = DEFAULT_FRAG_TOL_LOW_RES;
                            nodeList[0].Attributes["unit"].Value = "da";
                        }
                    }
                }
            }
            else
            {
                // Match not found; add it
                var attributes = new Dictionary<string, string>();

                if (instrumentResolutionMsMs == HIGH_RES_FLAG)
                {
                    attributes.Add("value", DEFAULT_FRAG_TOL_HIGH_RES);
                }
                else
                {
                    attributes.Add("value", DEFAULT_FRAG_TOL_LOW_RES);
                }

                attributes.Add("unit", "da");
                AddXMLElement(doc, "/search/parameters/fragment_ion_tol", attributes);
            }
        }

        private bool InitializeFastaFile()
        {
            // Define the path to the fasta file
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            var fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"));

            var fiFastaFile = new FileInfo(fastaFilePath);

            if (!fiFastaFile.Exists)
            {
                // Fasta file not found
                LogError("Fasta file not found: " + fiFastaFile.Name, "Fasta file not found: " + fiFastaFile.FullName);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Add a node given a simple xpath expression, for example "search/database" or "search/parameters/fragment_ion_tol"
        /// </summary>
        /// <param name="doc"></param>
        /// <param name="xpath"></param>
        /// <param name="attributes"></param>
        /// <returns></returns>
        /// <remarks>Code adapted from "http://stackoverflow.com/questions/508390/create-xml-nodes-based-on-xpath"</remarks>
        private XmlNode MakeXPath(XmlDocument doc, string xpath, Dictionary<string, string> attributes)
        {
            return MakeXPath(doc, doc as XmlNode, xpath, attributes);
        }

        private XmlNode MakeXPath(XmlDocument doc, XmlNode parent, string xpath, Dictionary<string, string> attributes)
        {
            // Grab the next node name in the xpath; or return parent if empty
            var partsOfXPath = xpath.Trim('/').Split('/');
            var nextNodeInXPath = partsOfXPath.First();
            if (string.IsNullOrEmpty(nextNodeInXPath))
            {
                return parent;
            }

            // Get or create the node from the name
            var node = parent.SelectSingleNode(nextNodeInXPath);
            if (node == null)
            {
                var newNode = doc.CreateElement(nextNodeInXPath);

                if (partsOfXPath.Length == 1)
                {
                    // Right-most node in the xpath
                    // Add the attributes
                    foreach (var attrib in attributes)
                    {
                        var newAttr = doc.CreateAttribute(attrib.Key);
                        newAttr.Value = attrib.Value;
                        newNode.Attributes.Append(newAttr);
                    }
                }

                node = parent.AppendChild(newNode);
            }

            if (partsOfXPath.Length == 1)
            {
                return node;
            }
            else
            {
                // Rejoin the remainder of the array as an xpath expression and recurse
                var rest = string.Join("/", partsOfXPath.Skip(1).ToArray());
                return MakeXPath(doc, node, rest, attributes);
            }
        }

        private bool PostProcessMODPlusResults(Dictionary<int, string> paramFileList)
        {
            var successOverall = true;

            try
            {
                // Keys in this list are scan numbers with charge state encoded as Charge / 100
                // For example, if scan 1000 and charge 2, then the key will be 1000.02
                // Values are a list of readers that have that given ScanPlusCharge combo
                var lstNextAvailableScan = new SortedList<double, List<clsMODPlusResultsReader>>();

                // Combine the result files using a Merge Sort (we assume the results are sorted by scan in each result file)

                if (m_DebugLevel >= 1)
                {
                    LogMessage("Merging the results files");
                }

                foreach (var modPlusRunner in mMODPlusRunners)
                {
                    if (string.IsNullOrWhiteSpace(modPlusRunner.Value.OutputFilePath))
                    {
                        continue;
                    }

                    var fiResultFile = new FileInfo(modPlusRunner.Value.OutputFilePath);

                    if (!fiResultFile.Exists)
                    {
                        // Result file not found for the current thread
                        // Log an error, but continue to combine the files
                        LogError("Result file not found for thread " + modPlusRunner.Key);
                        successOverall = false;
                        continue;
                    }

                    if (fiResultFile.Length == 0)
                    {
                        // 0-byte result file
                        // Log an error, but continue to combine the files
                        LogError("Result file is empty for thread " + modPlusRunner.Key);
                        successOverall = false;
                        continue;
                    }

                    var reader = new clsMODPlusResultsReader(m_Dataset, fiResultFile);
                    if (reader.SpectrumAvailable)
                    {
                        PushReader(lstNextAvailableScan, reader);
                    }
                }

                // The final results file is named Dataset_modp.txt
                var combinedResultsFilePath = Path.Combine(m_WorkDir, m_Dataset + clsMODPlusRunner.RESULTS_FILE_SUFFIX);
                var fiCombinedResults = new FileInfo(combinedResultsFilePath);

                using (var swCombinedResults = new StreamWriter(new FileStream(fiCombinedResults.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (lstNextAvailableScan.Count > 0)
                    {
                        var nextScan = lstNextAvailableScan.First();

                        lstNextAvailableScan.Remove(nextScan.Key);

                        foreach (var reader in nextScan.Value)
                        {
                            foreach (var dataLine in reader.CurrentScanData)
                            {
                                swCombinedResults.WriteLine(dataLine);
                            }

                            // Add a blank line
                            swCombinedResults.WriteLine();

                            if (reader.ReadNextSpectrum())
                            {
                                PushReader(lstNextAvailableScan, reader);
                            }
                        }
                    }
                }

                foreach (var modPlusRunner in mMODPlusRunners)
                {
                    m_jobParams.AddResultFileToSkip(modPlusRunner.Value.OutputFilePath);
                }

                // Zip the output file along with the ConsoleOutput files
                var diZipFolder = new DirectoryInfo(Path.Combine(m_WorkDir, "Temp_ZipScratch"));
                if (!diZipFolder.Exists)
                    diZipFolder.Create();

                var filesToMove = new List<FileInfo>();
                filesToMove.Add(fiCombinedResults);

                var diWorkDir = new DirectoryInfo(m_WorkDir);
                filesToMove.AddRange(diWorkDir.GetFiles("*ConsoleOutput*.txt"));

                foreach (var paramFile in paramFileList)
                {
                    filesToMove.Add(new FileInfo(paramFile.Value));
                }

                foreach (var fiFile in filesToMove)
                {
                    if (fiFile.Exists)
                    {
                        fiFile.MoveTo(Path.Combine(diZipFolder.FullName, fiFile.Name));
                    }
                }

                var zippedResultsFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(fiCombinedResults.Name) + ".zip");
                var blnSuccess = m_IonicZipTools.ZipDirectory(diZipFolder.FullName, zippedResultsFilePath);

                if (blnSuccess)
                {
                    m_jobParams.AddResultFileToSkip(fiCombinedResults.Name);
                }
                else if (string.IsNullOrEmpty(m_message))
                {
                    LogError("Unknown error zipping the MODPlus results and console output files");
                    return false;
                }

                if (successOverall)
                {
                    m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MGF_EXTENSION);
                }

                return successOverall;
            }
            catch (Exception ex)
            {
                LogError("Exception preparing the MODPlus results for zipping: " + ex.Message);
                return false;
            }
        }

        protected void PushReader(SortedList<double, List<clsMODPlusResultsReader>> lstNextAvailableScan, clsMODPlusResultsReader reader)
        {
            List<clsMODPlusResultsReader> readersForValue = null;

            if (lstNextAvailableScan.TryGetValue(reader.CurrentScanChargeCombo, out readersForValue))
            {
                readersForValue.Add(reader);
            }
            else
            {
                readersForValue = new List<clsMODPlusResultsReader>();
                readersForValue.Add(reader);

                lstNextAvailableScan.Add(reader.CurrentScanChargeCombo, readersForValue);
            }
        }

        /// <summary>
        /// Split the .mgf file into multiple parts
        /// </summary>
        /// <param name="fiMgfFile"></param>
        /// <param name="threadCount"></param>
        /// <returns>List of newly created .mgf files</returns>
        /// <remarks>Yses a round-robin splitting</remarks>
        private List<FileInfo> SplitMGFFiles(FileInfo fiMgfFile, int threadCount)
        {
            if (m_DebugLevel >= 1)
            {
                LogDebug("Splitting mgf file into " + threadCount + " parts: " + fiMgfFile.Name);
            }

            // Cache the current state of m_message
            var cachedStatusMessage = string.Copy(m_message);
            m_message = string.Empty;

            var splitter = new clsSplitMGFFile();
            RegisterEvents(splitter);

            // Split the .mgf file
            // If an error occurs, m_message will be updated because ErrorEventHandler calls LogError when event ErrorEvent is raised by clsSplitMGFFile
            var mgfFiles = splitter.SplitMgfFile(fiMgfFile.FullName, threadCount, "_Part");

            if (mgfFiles.Count == 0)
            {
                if (string.IsNullOrWhiteSpace(m_message))
                {
                    LogError("SplitMgfFile returned an empty list of files");
                }

                return new List<FileInfo>();
            }

            // Restore m_message
            m_message = cachedStatusMessage;

            m_jobParams.AddResultFileToSkip(fiMgfFile.FullName);

            return mgfFiles;
        }

        /// <summary>
        /// Run MODPlus
        /// </summary>
        /// <param name="javaProgLoc">Path to java.exe</param>
        /// <param name="paramFileList">Output: Dictionary where key is the thread number and value is the parameter file path</param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool StartMODPlus(string javaProgLoc, out Dictionary<int, string> paramFileList)
        {
            var currentTask = "Initializing";

            paramFileList = new Dictionary<int, string>();

            try
            {
                // We will store the MODPlus version info in the database after the header block is written to file MODPlus_ConsoleOutput.txt

                mToolVersionWritten = false;
                mMODPlusVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                currentTask = "Determine thread count";

                var javaMemorySizeMB = m_jobParams.GetJobParameter("MODPlusJavaMemorySize", 3000);
                var maxThreadsToAllow = ComputeMaxThreadsGivenMemoryPerThread(javaMemorySizeMB);

                // Determine the number of threads
                var threadCountText = m_jobParams.GetJobParameter("MODPlusThreads", "90%");
                var threadCount = ParseThreadCount(threadCountText, maxThreadsToAllow);

                // Convert the .mzXML or .mzML file to the MGF format
                var spectrumFileName = m_Dataset;

                var msXmlOutputType = m_jobParams.GetJobParameter("MSXMLOutputType", string.Empty);
                if (msXmlOutputType.ToLower() == "mzxml")
                {
                    spectrumFileName += clsAnalysisResources.DOT_MZXML_EXTENSION;
                }
                else
                {
                    spectrumFileName += clsAnalysisResources.DOT_MZML_EXTENSION;
                }

                currentTask = "Convert .mzML file to MGF";

                var fiSpectrumFile = new FileInfo(Path.Combine(m_WorkDir, spectrumFileName));
                if (!fiSpectrumFile.Exists)
                {
                    LogError("Spectrum file not found: " + fiSpectrumFile.Name);
                    return false;
                }

                var fiMgfFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MGF_EXTENSION));

                if (fiMgfFile.Exists)
                {
                    // The .MGF file already exists
                    // This will typically only be true while debugging
                }
                else
                {
                    var success = ConvertMsXmlToMGF(fiSpectrumFile, fiMgfFile);
                    if (!success)
                    {
                        return false;
                    }
                }

                currentTask = "Split the MGF file";

                // Create one MGF file for each thread
                var mgfFiles = SplitMGFFiles(fiMgfFile, threadCount);
                if (mgfFiles.Count == 0)
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                    {
                        LogError("Unknown error calling SplitMGFFiles");
                    }
                    return false;
                }

                currentTask = "Lookup job parameters";

                // Define the path to the fasta file
                // Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
                var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
                var dbFilename = m_jobParams.GetParam("PeptideSearch", "generatedFastaName");
                var fastaFilePath = Path.Combine(localOrgDbFolder, dbFilename);

                var paramFileName = m_jobParams.GetParam("ParmFileName");

                currentTask = "Create a parameter file for each thread";

                paramFileList = CreateParameterFiles(paramFileName, fastaFilePath, mgfFiles);

                if (paramFileList.Count == 0)
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                    {
                        LogError("CreateParameterFile returned an empty list in StartMODPlus");
                    }
                    return false;
                }

                currentTask = " Set up and execute a program runner to run each MODPlus instance";

                LogMessage("Running MODPlus using " + paramFileList.Count + " threads");

                m_progress = PROGRESS_PCT_MODPLUS_STARTING;
                ResetProgRunnerCpuUsage();

                mMODPlusRunners = new Dictionary<int, clsMODPlusRunner>();
                var lstThreads = new List<Thread>();

                foreach (var paramFile in paramFileList)
                {
                    var threadNum = paramFile.Key;

                    currentTask = "LaunchingModPlus, thread " + threadNum;

                    LogDebug(currentTask);

                    var modPlusRunner = new clsMODPlusRunner(m_Dataset, threadNum, m_WorkDir, paramFile.Value, javaProgLoc, mMODPlusProgLoc);

                    modPlusRunner.JavaMemorySizeMB = javaMemorySizeMB;

                    mMODPlusRunners.Add(threadNum, modPlusRunner);

                    var newThread = new Thread(new ThreadStart(modPlusRunner.StartAnalysis));
                    newThread.Priority = ThreadPriority.BelowNormal;
                    newThread.Start();
                    lstThreads.Add(newThread);
                }

                // Wait for all of the threads to exit
                // Run for a maximum of 14 days

                currentTask = "Waiting for all of the threads to exit";

                var dtStartTime = DateTime.UtcNow;
                var completedThreads = new SortedSet<int>();

                const int SECONDS_BETWEEN_UPDATES = 15;
                var dtLastStatusUpdate = DateTime.UtcNow;

                while (true)
                {
                    // Poll the status of each of the threads

                    var stepsComplete = 0;
                    double progressSum = 0;

                    var processIDs = new List<int>();
                    float coreUsageOverall = 0;

                    foreach (var modPlusRunner in mMODPlusRunners)
                    {
                        var eStatus = modPlusRunner.Value.Status;
                        if (eStatus >= clsMODPlusRunner.MODPlusRunnerStatusCodes.Success)
                        {
                            // Analysis completed (or failed)
                            stepsComplete += 1;

                            if (!completedThreads.Contains(modPlusRunner.Key))
                            {
                                completedThreads.Add(modPlusRunner.Key);
                                LogDebug("MODPlus thread " + modPlusRunner.Key + " is now complete");
                            }
                        }

                        processIDs.Add(modPlusRunner.Value.ProcessID);

                        progressSum += modPlusRunner.Value.Progress;
                        coreUsageOverall += modPlusRunner.Value.CoreUsage;

                        if (m_DebugLevel >= 1)
                        {
                            if (!modPlusRunner.Value.CommandLineArgsLogged && !string.IsNullOrWhiteSpace(modPlusRunner.Value.CommandLineArgs))
                            {
                                modPlusRunner.Value.CommandLineArgsLogged = true;

                                // "C:\Program Files\Java\jre8\bin\java.exe" -Xmx3G -jar C:\DMS_Programs\MODPlus\modp_pnnl.jar
                                //   -i MODPlus_Params_Part1.xml -o E:\DMS_WorkDir2\Dataset_Part1_modp.txt > MODPlus_ConsoleOutput_Part1.txt
                                LogDebug(
                                    javaProgLoc + " " + modPlusRunner.Value.CommandLineArgs);
                            }
                        }

                        if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(modPlusRunner.Value.ReleaseDate))
                        {
                            mMODPlusVersion = modPlusRunner.Value.ReleaseDate;
                            mToolVersionWritten = StoreToolVersionInfo(mMODPlusProgLoc);
                        }
                    }

                    var subTaskProgress = progressSum / mMODPlusRunners.Count;
                    var updatedProgress = ComputeIncrementalProgress(PROGRESS_PCT_MODPLUS_STARTING, PROGRESS_PCT_MODPLUS_COMPLETE, (float)subTaskProgress);
                    if (updatedProgress > m_progress)
                    {
                        // This progress will get written to the status file and sent to the messaging queue by UpdateStatusFile()
                        m_progress = updatedProgress;
                    }

                    if (stepsComplete >= mMODPlusRunners.Count)
                    {
                        // All threads are done
                        break;
                    }

                    while (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds < SECONDS_BETWEEN_UPDATES)
                    {
                        Thread.Sleep(250);
                    }

                    dtLastStatusUpdate = DateTime.UtcNow;

                    CmdRunner_LoopWaiting(processIDs, coreUsageOverall, SECONDS_BETWEEN_UPDATES);

                    if (DateTime.UtcNow.Subtract(dtStartTime).TotalDays > 14)
                    {
                        LogError("MODPlus ran for over 14 days; aborting");

                        foreach (var modPlusRunner in mMODPlusRunners)
                        {
                            modPlusRunner.Value.AbortProcessingNow();
                        }

                        return false;
                    }
                }

                var blnSuccess = true;
                var exitCode = 0;

                currentTask = "Looking for console output error messages";

                // Look for any console output error messages

                foreach (var modPlusRunner in mMODPlusRunners)
                {
                    // One last check for the ToolVersion info being written to the database
                    if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(modPlusRunner.Value.ReleaseDate))
                    {
                        mMODPlusVersion = modPlusRunner.Value.ReleaseDate;
                        mToolVersionWritten = StoreToolVersionInfo(mMODPlusProgLoc);
                    }

                    var progRunner = modPlusRunner.Value.ProgRunner;

                    if (progRunner == null)
                    {
                        blnSuccess = false;
                        if (string.IsNullOrWhiteSpace(m_message))
                        {
                            m_message = "progRunner object is null for thread " + modPlusRunner.Key;
                        }
                        continue;
                    }

                    if (!string.IsNullOrWhiteSpace(progRunner.CachedConsoleErrors))
                    {
                        // Note that clsProgRunner will have already included these errors in the ConsoleOutput.txt file
                        var consoleError = "Console error for thread " + modPlusRunner.Key + ": " +
                                           progRunner.CachedConsoleErrors.Replace(Environment.NewLine, "; ");
                        LogError(consoleError);
                        blnSuccess = false;
                    }

                    if (progRunner.ExitCode != 0 && exitCode == 0)
                    {
                        blnSuccess = false;
                        exitCode = progRunner.ExitCode;
                    }
                }

                if (!blnSuccess)
                {

                    LogError("Error running MODPlus");

                    if (exitCode != 0)
                    {
                        LogWarning("MODPlus returned a non-zero exit code: " + exitCode);
                    }
                    else
                    {
                        LogWarning("Call to MODPlus failed (but exit code is 0)");
                    }

                    return false;
                }

                m_progress = PROGRESS_PCT_MODPLUS_COMPLETE;

                m_StatusTools.UpdateAndWrite(m_progress);
                if (m_DebugLevel >= 3)
                {
                    LogDebug("MODPlus Analysis Complete");
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in StartMODPlus at " + currentTask, ex);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string strProgLoc)
        {
            string strToolVersionInfo = null;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            strToolVersionInfo = string.Copy(mMODPlusVersion);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo>();
            var fiMODPlusProg = new FileInfo(mMODPlusProgLoc);
            ioToolFiles.Add(fiMODPlusProg);

            ioToolFiles.Add(new FileInfo(Path.Combine(fiMODPlusProg.DirectoryName, "tda_plus.jar")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: true);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        #endregion

        #region "Event Handlers"

        private void MSConvert_CmdRunner_LoopWaiting()
        {
            var processIDs = new List<int>();
            processIDs.Add(0);

            // Pass -1 for coreUsageOverall so that mCoreUsageHistory does not get updated
            CmdRunner_LoopWaiting(processIDs, coreUsageOverall: -1, secondsBetweenUpdates: 30);
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting(List<int> processIDs, float coreUsageOverall, int secondsBetweenUpdates)
        {
            UpdateStatusFile(m_progress);

            UpdateProgRunnerCpuUsage(processIDs.FirstOrDefault(), coreUsageOverall, secondsBetweenUpdates);

            LogProgress("MODPlus");
        }

        #endregion
    }
}
