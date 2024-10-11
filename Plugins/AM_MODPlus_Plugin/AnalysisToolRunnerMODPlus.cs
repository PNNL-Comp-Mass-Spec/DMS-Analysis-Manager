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
using System.Xml;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMODPlusPlugin
{
    /// <summary>
    /// Class for running MODPlus
    /// </summary>
    public class AnalysisToolRunnerMODPlus : AnalysisToolRunnerBase
    {
        // Ignore Spelling: Da, hmsn, mgf, mzml, mzxml, msms, outfile, xpath

        private const string MODPlus_JAR_NAME = "modp_pnnl.jar";

        private const int PROGRESS_PCT_CONVERTING_MSXML_TO_MGF = 1;
        private const int PROGRESS_PCT_MODPLUS_STARTING = 5;
        private const int PROGRESS_PCT_MODPLUS_COMPLETE = 95;

        private bool mToolVersionWritten;
        private string mMODPlusVersion;

        private string mMODPlusProgLoc;

        /// <summary>
        /// Dictionary of ModPlus instances
        /// </summary>
        /// <remarks>Key is core number (1 through NumCores), value is the instance</remarks>
        private Dictionary<int, MODPlusRunner> mMODPlusRunners;

        /// <summary>
        /// Runs MODPlus
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerMODPlus.RunTool(): Enter");
                }

                // Verify that program files exist

                // JavaProgLoc will typically be "C:\Program Files\Java\jre11\bin\java.exe"
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

                // Run MODPlus (using multiple threads)
                var processingSuccess = StartMODPlus(javaProgLoc, out var paramFileList);

                if (processingSuccess)
                {
                    // Look for the results file(s)
                    var postProcessSuccess = PostProcessMODPlusResults(paramFileList);

                    if (!postProcessSuccess)
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            LogError("Unknown error post-processing the MODPlus results");
                        }
                        processingSuccess = false;
                    }
                }

                mProgress = PROGRESS_PCT_MODPLUS_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
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
        }

        /// <summary>
        /// Add a node given a simple xpath expression, for example "search/database" or "search/parameters/fragment_ion_tol"
        /// </summary>
        /// <param name="doc"></param>
        /// <param name="xpath"></param>
        /// <param name="attributeName"></param>
        /// <param name="attributeValue"></param>
        private void AddXMLElement(XmlDocument doc, string xpath, string attributeName, string attributeValue)
        {
            var attributes = new Dictionary<string, string> {
                { attributeName, attributeValue}
            };

            AddXMLElement(doc, xpath, attributes);
        }

        /// <summary>
        /// Add a node given a simple xpath expression, for example "search/database" or "search/parameters/fragment_ion_tol"
        /// </summary>
        /// <param name="doc"></param>
        /// <param name="xpath"></param>
        /// <param name="attributes"></param>
        private void AddXMLElement(XmlDocument doc, string xpath, Dictionary<string, string> attributes)
        {
            MakeXPath(doc, xpath, attributes);
        }

        /// <summary>
        /// Use MSConvert to convert the .mzXML or .mzML file to a .mgf file
        /// </summary>
        /// <param name="spectrumFile"></param>
        /// <param name="mgfFile"></param>
        /// <returns>True if success, false if an error</returns>
        private bool ConvertMsXmlToMGF(FileSystemInfo spectrumFile, FileSystemInfo mgfFile)
        {
            // Set up and execute a program runner to run MSConvert

            var msConvertProgLoc = DetermineProgramLocation("ProteoWizardDir", "msconvert.exe");

            if (string.IsNullOrWhiteSpace(msConvertProgLoc))
            {
                if (string.IsNullOrWhiteSpace(mMessage))
                {
                    LogError("Manager parameter ProteoWizardDir was not found; cannot run MSConvert.exe");
                }
                return false;
            }

            var msConvertConsoleOutput = Path.Combine(mWorkDir, "MSConvert_ConsoleOutput.txt");
            mJobParams.AddResultFileToSkip(msConvertConsoleOutput);

            var arguments = " --mgf" +
                            " --outfile " + mgfFile.FullName +
                            " " + PossiblyQuotePath(spectrumFile.FullName);

            if (mDebugLevel >= 1)
            {
                // C:\DMS_Programs\ProteoWizard\msconvert.exe --mgf --outfile Dataset.mgf Dataset.mzML
                LogDebug(msConvertProgLoc + " " + arguments);
            }

            var msConvertRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(msConvertRunner);
            msConvertRunner.LoopWaiting += MSConvert_CmdRunner_LoopWaiting;

            msConvertRunner.CreateNoWindow = true;
            msConvertRunner.CacheStandardOutput = false;
            msConvertRunner.EchoOutputToConsole = true;

            msConvertRunner.WriteConsoleOutputToFile = true;
            msConvertRunner.ConsoleOutputFilePath = msConvertConsoleOutput;

            mProgress = PROGRESS_PCT_CONVERTING_MSXML_TO_MGF;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var success = msConvertRunner.RunProgram(msConvertProgLoc, arguments, "MSConvert", true);

            if (success)
            {
                if (mDebugLevel >= 2)
                {
                    LogDebug("MSConvert.exe successfully created " + mgfFile.Name);
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

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZXML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        /// <summary>
        /// Create the parameter files
        /// </summary>
        /// <param name="paramFileName"></param>
        /// <param name="fastaFilePath"></param>
        /// <param name="mgfFiles"></param>
        /// <returns>Dictionary where key is the thread number and value is the parameter file path</returns>
        private Dictionary<int, string> CreateParameterFiles(string paramFileName, string fastaFilePath, IEnumerable<FileInfo> mgfFiles)
        {
            try
            {
                var paramFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));

                if (!paramFile.Exists)
                {
                    LogError("Parameter file not found by CreateParameterFiles");
                    return new Dictionary<int, string>();
                }

                var doc = new XmlDocument();
                doc.Load(paramFile.FullName);

                DefineParamFileDatasetAndFasta(doc, fastaFilePath);

                DefineParamMassResolutionSettings(doc);

                var paramFileList = CreateThreadParamFiles(paramFile, doc, mgfFiles);

                return paramFileList;
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateParameterFiles", ex);
                return new Dictionary<int, string>();
            }
        }

        private Dictionary<int, string> CreateThreadParamFiles(FileInfo masterParamFile, XmlNode doc, IEnumerable<FileInfo> mgfFiles)
        {
            var reThreadNumber = new Regex(@"_Part(\d+)\.mgf", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var paramFileList = new Dictionary<int, string>();

            if (masterParamFile.Directory == null)
            {
                LogError("Unable to determine the parent directory of parameter file " + masterParamFile.FullName);
                return paramFileList;
            }

            foreach (var mgfFile in mgfFiles)
            {
                var reMatch = reThreadNumber.Match(mgfFile.Name);

                if (!reMatch.Success)
                {
                    LogError("RegEx failed to extract the thread number from the MGF file name: " + mgfFile.Name);
                    return new Dictionary<int, string>();
                }

                if (!int.TryParse(reMatch.Groups[1].Value, out var threadNumber))
                {
                    LogError("RegEx logic error extracting the thread number from the MGF file name: " + mgfFile.Name);
                    return new Dictionary<int, string>();
                }

                if (paramFileList.ContainsKey(threadNumber))
                {
                    LogError("MGFSplitter logic error; duplicate thread number encountered for " + mgfFile.Name);
                    return new Dictionary<int, string>();
                }

                var nodeList = doc.SelectNodes("/search/dataset");

                if (nodeList?.Count > 0)
                {
                    var xmlAttributeCollection = nodeList[0].Attributes;

                    if (xmlAttributeCollection != null)
                    {
                        xmlAttributeCollection["local_path"].Value = mgfFile.FullName;
                        xmlAttributeCollection["format"].Value = "mgf";
                    }
                }

                var paramFileName = Path.GetFileNameWithoutExtension(masterParamFile.Name) + "_Part" + threadNumber + ".xml";
                var paramFilePath = Path.Combine(masterParamFile.Directory.FullName, paramFileName);

                using (var writer = new XmlTextWriter(new FileStream(paramFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), new UTF8Encoding(false)))
                {
                    writer.Formatting = Formatting.Indented;
                    writer.Indentation = 4;

                    doc.WriteTo(writer);
                }

                paramFileList.Add(threadNumber, paramFilePath);

                mJobParams.AddResultFileToSkip(paramFilePath);
            }

            return paramFileList;
        }

        private void DefineParamFileDatasetAndFasta(XmlDocument doc, string fastaFilePath)
        {
            var defaultAttributes = new Dictionary<string, string> {
                { "local_path", "Dataset_PartX.mgf"},
                { "format", "mgf"}
            };

            // Define the path to the dataset file
            var datasetNodes = doc.SelectNodes("/search/dataset");

            if (datasetNodes?.Count > 0)
            {
                // This value will get updated to the correct name later in this method
                var xmlAttributeCollection = datasetNodes[0].Attributes;

                if (xmlAttributeCollection?["local_path"] == null)
                {
                    // Match not found; add it
                    foreach (var attrib in defaultAttributes)
                    {
                        var newAttr = doc.CreateAttribute(attrib.Key);
                        newAttr.Value = attrib.Value;
                        var attributeCollection = datasetNodes[0].Attributes;
                        attributeCollection?.Append(newAttr);
                    }
                }
                else
                {
                    xmlAttributeCollection["local_path"].Value = "Dataset_PartX.mgf";
                    xmlAttributeCollection["format"].Value = "mgf";
                }
            }
            else
            {
                // Match not found; add it
                AddXMLElement(doc, "/search/dataset", defaultAttributes);
            }

            // Define the path to the FASTA file
            var databaseNodes = doc.SelectNodes("/search/database");

            if (databaseNodes?.Count > 0)
            {
                var xmlAttributeCollection = databaseNodes[0].Attributes;

                if (xmlAttributeCollection?["local_path"] == null)
                {
                    // Match not found; add it
                    var newAttr = doc.CreateAttribute("local_path");
                    newAttr.Value = fastaFilePath;
                    var attributeCollection = databaseNodes[0].Attributes;
                    attributeCollection?.Append(newAttr);
                }
                else
                {
                    xmlAttributeCollection["local_path"].Value = fastaFilePath;
                }
            }
            else
            {
                // Node not found; add it
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

            var datasetType = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetType");
            var instrumentResolutionMsMs = LOW_RES_FLAG;

            if (datasetType.EndsWith("hmsn", StringComparison.OrdinalIgnoreCase))
            {
                instrumentResolutionMsMs = HIGH_RES_FLAG;
            }

            var instrumentResolutionNodes = doc.SelectNodes("/search/instrument_resolution");

            if (instrumentResolutionNodes?.Count > 0)
            {
                var xmlAttributeCollection = instrumentResolutionNodes[0].Attributes;

                if (xmlAttributeCollection != null && (xmlAttributeCollection["msms"].Value == HIGH_RES_FLAG && instrumentResolutionMsMs == "low"))
                {
                    // Parameter file lists the resolution as high, but it's actually low
                    // Auto-change it
                    var attributeCollection = instrumentResolutionNodes[0].Attributes;

                    if (attributeCollection?["msms"] != null)
                    {
                        attributeCollection["msms"].Value = instrumentResolutionMsMs;
                        LogWarning("Auto-switched to low resolution mode for MS/MS data", true);
                    }
                    else
                    {
                        LogWarning("Unable to auto-switch to low resolution mode for MS/MS data; attribute not found", true);
                    }
                }
            }
            else
            {
                // Node not found; add it
                var attributes = new Dictionary<string, string> {
                    { "ms", HIGH_RES_FLAG}, {"msms", instrumentResolutionMsMs}
                };

                AddXMLElement(doc, "/search/instrument_resolution", attributes);
            }

            var fragIonTolNodes = doc.SelectNodes("/search/parameters/fragment_ion_tol");

            if (fragIonTolNodes?.Count > 0)
            {
                if (instrumentResolutionMsMs != LOW_RES_FLAG)
                    return;

                var xmlAttributeCollection = fragIonTolNodes[0].Attributes;

                if (xmlAttributeCollection?["value"] == null || xmlAttributeCollection["unit"] == null)
                {
                    LogWarning("The fragment_ion_tol node is missing attributes value and/or unit");
                    return;
                }

                if (!double.TryParse(xmlAttributeCollection["value"].Value, out var massTolDa))
                    return;

                var massUnits = xmlAttributeCollection["unit"].Value;

                if (massUnits == "ppm")
                {
                    // Convert from ppm to Da
                    massTolDa = massTolDa * 1000 / 1000000;
                }

                if (massTolDa < MIN_FRAG_TOL_LOW_RES)
                {
                    mEvalMessage = Global.AppendToComment(mEvalMessage, "Auto-changed fragment_ion_tol to " + DEFAULT_FRAG_TOL_LOW_RES + " Da since low resolution MS/MS");
                    xmlAttributeCollection["value"].Value = DEFAULT_FRAG_TOL_LOW_RES;
                    xmlAttributeCollection["unit"].Value = "da";
                }
            }
            else
            {
                // Node not found; add it
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
            // Define the path to the FASTA file
            var localOrgDbFolder = mMgrParams.GetParam("OrgDbDir");
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName"));

            var fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // FASTA file not found
                LogError("FASTA file not found: " + fastaFile.Name, "FASTA file not found: " + fastaFile.FullName);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Add a node given a simple xpath expression, for example "search/database" or "search/parameters/fragment_ion_tol"
        /// </summary>
        /// <remarks>Code adapted from "http://stackoverflow.com/questions/508390/create-xml-nodes-based-on-xpath"</remarks>
        /// <param name="doc"></param>
        /// <param name="xpath"></param>
        /// <param name="attributes"></param>
        private void MakeXPath(XmlDocument doc, string xpath, Dictionary<string, string> attributes)
        {
            MakeXPath(doc, doc, xpath, attributes);
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

            // Rejoin the remainder of the array as an xpath expression and recurse
            var rest = string.Join("/", partsOfXPath.Skip(1).ToArray());
            return MakeXPath(doc, node, rest, attributes);
        }

        private bool PostProcessMODPlusResults(Dictionary<int, string> paramFileList)
        {
            var successOverall = true;

            try
            {
                // Keys in this list are scan numbers with charge state encoded as Charge / 100
                // For example, if scan 1000 and charge 2, the key will be 1000.02
                // Values are a list of readers that have that given ScanPlusCharge combo
                var nextAvailableScan = new SortedList<double, List<MODPlusResultsReader>>();

                // Combine the result files using a Merge Sort (we assume the results are sorted by scan in each result file)

                if (mDebugLevel >= 1)
                {
                    LogMessage("Merging the results files");
                }

                foreach (var modPlusRunner in mMODPlusRunners)
                {
                    if (string.IsNullOrWhiteSpace(modPlusRunner.Value.OutputFilePath))
                    {
                        continue;
                    }

                    var resultFile = new FileInfo(modPlusRunner.Value.OutputFilePath);

                    if (!resultFile.Exists)
                    {
                        // Result file not found for the current thread
                        // Log an error, but continue to combine the files
                        LogError("Result file not found for thread " + modPlusRunner.Key);
                        successOverall = false;
                        continue;
                    }

                    if (resultFile.Length == 0)
                    {
                        // 0-byte result file
                        // Log an error, but continue to combine the files
                        LogError("Result file is empty for thread " + modPlusRunner.Key);
                        successOverall = false;
                        continue;
                    }

                    var reader = new MODPlusResultsReader(mDatasetName, resultFile);

                    if (reader.SpectrumAvailable)
                    {
                        PushReader(nextAvailableScan, reader);
                    }
                }

                // The final results file is named Dataset_modp.txt
                var combinedResultsFilePath = Path.Combine(mWorkDir, mDatasetName + MODPlusRunner.RESULTS_FILE_SUFFIX);
                var combinedResults = new FileInfo(combinedResultsFilePath);

                using (var combinedResultsWriter = new StreamWriter(new FileStream(combinedResults.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (nextAvailableScan.Count > 0)
                    {
                        var nextScan = nextAvailableScan.First();

                        nextAvailableScan.Remove(nextScan.Key);

                        foreach (var reader in nextScan.Value)
                        {
                            foreach (var dataLine in reader.CurrentScanData)
                            {
                                combinedResultsWriter.WriteLine(dataLine);
                            }

                            // Add a blank line
                            combinedResultsWriter.WriteLine();

                            if (reader.ReadNextSpectrum())
                            {
                                PushReader(nextAvailableScan, reader);
                            }
                        }
                    }
                }

                foreach (var modPlusRunner in mMODPlusRunners)
                {
                    mJobParams.AddResultFileToSkip(modPlusRunner.Value.OutputFilePath);
                }

                // Zip the output file along with the ConsoleOutput files
                var zipFolder = new DirectoryInfo(Path.Combine(mWorkDir, "Temp_ZipScratch"));

                if (!zipFolder.Exists)
                    zipFolder.Create();

                var filesToMove = new List<FileInfo> {
                    combinedResults
                };

                var workingDirectory = new DirectoryInfo(mWorkDir);
                filesToMove.AddRange(workingDirectory.GetFiles("*ConsoleOutput*.txt"));

                foreach (var paramFile in paramFileList)
                {
                    filesToMove.Add(new FileInfo(paramFile.Value));
                }

                foreach (var sourceFile in filesToMove)
                {
                    if (sourceFile.Exists)
                    {
                        sourceFile.MoveTo(Path.Combine(zipFolder.FullName, sourceFile.Name));
                    }
                }

                var zippedResultsFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(combinedResults.Name) + ".zip");
                var success = mZipTools.ZipDirectory(zipFolder.FullName, zippedResultsFilePath);

                if (success)
                {
                    mJobParams.AddResultFileToSkip(combinedResults.Name);
                }
                else if (string.IsNullOrEmpty(mMessage))
                {
                    LogError("Unknown error zipping the MODPlus results and console output files");
                    return false;
                }

                if (successOverall)
                {
                    mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MGF_EXTENSION);
                }

                return successOverall;
            }
            catch (Exception ex)
            {
                LogError("Exception preparing the MODPlus results for zipping: " + ex.Message);
                return false;
            }
        }

        private void PushReader(SortedList<double, List<MODPlusResultsReader>> nextAvailableScan, MODPlusResultsReader reader)
        {
            if (nextAvailableScan.TryGetValue(reader.CurrentScanChargeCombo, out var readersForValue))
            {
                readersForValue.Add(reader);
            }
            else
            {
                readersForValue = new List<MODPlusResultsReader> {
                    reader
                };

                nextAvailableScan.Add(reader.CurrentScanChargeCombo, readersForValue);
            }
        }

        /// <summary>
        /// Split the .mgf file into multiple parts
        /// </summary>
        /// <remarks>Uses a round-robin splitting</remarks>
        /// <param name="mgfFile"></param>
        /// <param name="threadCount"></param>
        /// <returns>List of newly created .mgf files</returns>
        private List<FileInfo> SplitMGFFiles(FileSystemInfo mgfFile, int threadCount)
        {
            if (mDebugLevel >= 1)
            {
                LogDebug("Splitting mgf file into " + threadCount + " parts: " + mgfFile.Name);
            }

            // Cache the current state of mMessage
            var cachedStatusMessage = mMessage;
            mMessage = string.Empty;

            var splitter = new SplitMGFFile();
            RegisterEvents(splitter);

            // Split the .mgf file
            // If an error occurs, mMessage will be updated because ErrorEventHandler calls LogError when event ErrorEvent is raised by SplitMGFFile
            var mgfFiles = splitter.SplitMgfFile(mgfFile.FullName, threadCount, "_Part");

            if (mgfFiles.Count == 0)
            {
                if (string.IsNullOrWhiteSpace(mMessage))
                {
                    LogError("SplitMgfFile returned an empty list of files");
                }

                return new List<FileInfo>();
            }

            // Restore mMessage
            mMessage = cachedStatusMessage;

            mJobParams.AddResultFileToSkip(mgfFile.FullName);

            return mgfFiles;
        }

        /// <summary>
        /// Run MODPlus
        /// </summary>
        /// <param name="javaProgLoc">Path to java.exe</param>
        /// <param name="paramFileList">Output: Dictionary where key is the thread number and value is the parameter file path</param>
        private bool StartMODPlus(string javaProgLoc, out Dictionary<int, string> paramFileList)
        {
            var currentTask = "Initializing";

            paramFileList = new Dictionary<int, string>();

            try
            {
                // We will store the MODPlus version info in the database after the header block is written to file MODPlus_ConsoleOutput.txt

                mToolVersionWritten = false;
                mMODPlusVersion = string.Empty;

                currentTask = "Determine thread count";

                var javaMemorySizeMB = mJobParams.GetJobParameter("MODPlusJavaMemorySize", 3000);
                var maxThreadsToAllow = ComputeMaxThreadsGivenMemoryPerThread(javaMemorySizeMB);

                // Determine the number of threads
                var threadCountText = mJobParams.GetJobParameter("MODPlusThreads", "90%");
                var threadCount = ParseThreadCount(threadCountText, maxThreadsToAllow);

                // Convert the .mzXML or .mzML file to the MGF format
                var spectrumFileName = mDatasetName;

                var msXmlOutputType = mJobParams.GetJobParameter("MSXMLOutputType", string.Empty);

                if (string.Equals(msXmlOutputType, "mzxml", StringComparison.OrdinalIgnoreCase))
                {
                    spectrumFileName += AnalysisResources.DOT_MZXML_EXTENSION;
                }
                else
                {
                    spectrumFileName += AnalysisResources.DOT_MZML_EXTENSION;
                }

                currentTask = "Convert .mzML file to MGF";

                var spectrumFile = new FileInfo(Path.Combine(mWorkDir, spectrumFileName));

                if (!spectrumFile.Exists)
                {
                    LogError("Spectrum file not found: " + spectrumFile.Name);
                    return false;
                }

                var mgfFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MGF_EXTENSION));

                if (mgfFile.Exists)
                {
                    // The .MGF file already exists
                    // This will typically only be true while debugging
                }
                else
                {
                    var convertSuccess = ConvertMsXmlToMGF(spectrumFile, mgfFile);

                    if (!convertSuccess)
                    {
                        return false;
                    }
                }

                currentTask = "Split the MGF file";

                // Create one MGF file for each thread
                var mgfFiles = SplitMGFFiles(mgfFile, threadCount);

                if (mgfFiles.Count == 0)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("Unknown error calling SplitMGFFiles");
                    }
                    return false;
                }

                currentTask = "Lookup job parameters";

                // Define the path to the FASTA file
                // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
                var localOrgDbFolder = mMgrParams.GetParam("OrgDbDir");
                var dbFilename = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName");
                var fastaFilePath = Path.Combine(localOrgDbFolder, dbFilename);

                var paramFileName = mJobParams.GetParam("ParamFileName");

                currentTask = "Create a parameter file for each thread";

                paramFileList = CreateParameterFiles(paramFileName, fastaFilePath, mgfFiles);

                if (paramFileList.Count == 0)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("CreateParameterFile returned an empty list in StartMODPlus");
                    }
                    return false;
                }

                currentTask = " Set up and execute a program runner to run each MODPlus instance";

                LogMessage("Running MODPlus using " + paramFileList.Count + " threads");

                mProgress = PROGRESS_PCT_MODPLUS_STARTING;
                ResetProgRunnerCpuUsage();

                mMODPlusRunners = new Dictionary<int, MODPlusRunner>();

                foreach (var paramFile in paramFileList)
                {
                    var threadNum = paramFile.Key;

                    currentTask = "LaunchingModPlus, thread " + threadNum;

                    LogDebug(currentTask);

                    var modPlusRunner =
                        new MODPlusRunner(mDatasetName, threadNum, mWorkDir, paramFile.Value, javaProgLoc, mMODPlusProgLoc)
                        {
                            JavaMemorySizeMB = javaMemorySizeMB
                        };

                    mMODPlusRunners.Add(threadNum, modPlusRunner);

                    var newThread = new System.Threading.Thread(modPlusRunner.StartAnalysis)
                    {
                        Priority = System.Threading.ThreadPriority.BelowNormal
                    };

                    newThread.Start();
                }

                // ReSharper disable once GrammarMistakeInComment

                // Wait for all of the threads to exit
                // Run for a maximum of 14 days

                currentTask = "Waiting for all of the threads to exit";

                var startTime = DateTime.UtcNow;
                var completedThreads = new SortedSet<int>();

                const int SECONDS_BETWEEN_UPDATES = 15;

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

                        if (eStatus >= MODPlusRunner.MODPlusRunnerStatusCodes.Success)
                        {
                            // Analysis completed (or failed)
                            stepsComplete++;

                            if (!completedThreads.Contains(modPlusRunner.Key))
                            {
                                completedThreads.Add(modPlusRunner.Key);
                                LogDebug("MODPlus thread " + modPlusRunner.Key + " is now complete");
                            }
                        }

                        processIDs.Add(modPlusRunner.Value.ProcessID);

                        progressSum += modPlusRunner.Value.Progress;
                        coreUsageOverall += modPlusRunner.Value.CoreUsage;

                        if (mDebugLevel >= 1)
                        {
                            if (!modPlusRunner.Value.CommandLineArgsLogged && !string.IsNullOrWhiteSpace(modPlusRunner.Value.CommandLineArgs))
                            {
                                modPlusRunner.Value.CommandLineArgsLogged = true;

                                // "C:\Program Files\Java\jre11\bin\java.exe" -Xmx3G -jar C:\DMS_Programs\MODPlus\modp_pnnl.jar
                                //   -i MODPlus_Params_Part1.xml -o E:\DMS_WorkDir2\Dataset_Part1_modp.txt > MODPlus_ConsoleOutput_Part1.txt
                                LogDebug(javaProgLoc + " " + modPlusRunner.Value.CommandLineArgs);
                            }
                        }

                        if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(modPlusRunner.Value.ReleaseDate))
                        {
                            mMODPlusVersion = modPlusRunner.Value.ReleaseDate;
                            mToolVersionWritten = StoreToolVersionInfo();
                        }
                    }

                    var subTaskProgress = progressSum / mMODPlusRunners.Count;
                    var updatedProgress = ComputeIncrementalProgress(PROGRESS_PCT_MODPLUS_STARTING, PROGRESS_PCT_MODPLUS_COMPLETE, (float)subTaskProgress);

                    if (updatedProgress > mProgress)
                    {
                        // This progress will get written to the status file and sent to the messaging queue by UpdateStatusFile()
                        mProgress = updatedProgress;
                    }

                    if (stepsComplete >= mMODPlusRunners.Count)
                    {
                        // All threads are done
                        break;
                    }

                    Global.IdleLoop(SECONDS_BETWEEN_UPDATES);

                    CmdRunner_LoopWaiting(processIDs, coreUsageOverall, SECONDS_BETWEEN_UPDATES);

                    if (DateTime.UtcNow.Subtract(startTime).TotalDays > 14)
                    {
                        LogError("MODPlus ran for over 14 days; aborting");

                        foreach (var modPlusRunner in mMODPlusRunners)
                        {
                            modPlusRunner.Value.AbortProcessingNow();
                        }

                        return false;
                    }
                }

                var success = true;
                var exitCode = 0;

                currentTask = "Looking for console output error messages";

                // Look for any console output error messages

                foreach (var modPlusRunner in mMODPlusRunners)
                {
                    // One last check for the ToolVersion info being written to the database
                    if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(modPlusRunner.Value.ReleaseDate))
                    {
                        mMODPlusVersion = modPlusRunner.Value.ReleaseDate;
                        mToolVersionWritten = StoreToolVersionInfo();
                    }

                    var progRunner = modPlusRunner.Value.ProgramRunner;

                    if (progRunner == null)
                    {
                        success = false;

                        if (string.IsNullOrWhiteSpace(mMessage))
                        {
                            mMessage = "progRunner object is null for thread " + modPlusRunner.Key;
                        }
                        continue;
                    }

                    if (!string.IsNullOrWhiteSpace(progRunner.CachedConsoleErrors))
                    {
                        // Note that ProgRunner will have already included these errors in the ConsoleOutput.txt file
                        var consoleError = "Console error for thread " + modPlusRunner.Key + ": " +
                                           progRunner.CachedConsoleErrors.Replace(Environment.NewLine, "; ");
                        LogError(consoleError);
                        success = false;
                    }

                    if (progRunner.ExitCode != 0 && exitCode == 0)
                    {
                        success = false;
                        exitCode = progRunner.ExitCode;
                    }
                }

                if (!success)
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

                mProgress = PROGRESS_PCT_MODPLUS_COMPLETE;

                mStatusTools.UpdateAndWrite(mProgress);

                if (mDebugLevel >= 3)
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
        private bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var toolVersionInfo = mMODPlusVersion;

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>();
            var modPlusProgram = new FileInfo(mMODPlusProgLoc);
            toolFiles.Add(modPlusProgram);

            if (modPlusProgram.Directory != null)
                toolFiles.Add(new FileInfo(Path.Combine(modPlusProgram.Directory.FullName, "tda_plus.jar")));

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private void MSConvert_CmdRunner_LoopWaiting()
        {
            var processIDs = new List<int> { 0 };

            // Pass -1 for coreUsageOverall so that mCoreUsageHistory does not get updated
            CmdRunner_LoopWaiting(processIDs, coreUsageOverall: -1, secondsBetweenUpdates: 30);
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting(IEnumerable<int> processIDs, float coreUsageOverall, int secondsBetweenUpdates)
        {
            UpdateStatusFile(mProgress);

            UpdateProgRunnerCpuUsage(processIDs.FirstOrDefault(), coreUsageOverall, secondsBetweenUpdates);

            LogProgress("MODPlus");
        }
    }
}
