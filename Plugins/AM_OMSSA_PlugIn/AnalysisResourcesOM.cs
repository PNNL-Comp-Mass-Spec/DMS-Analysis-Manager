using AnalysisManagerBase;
using System;
using System.IO;
using System.Xml;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerOMSSAPlugIn
{
    /// <summary>
    /// Retrieve resources for the OMSSA plugin
    /// </summary>
    public class AnalysisResourcesOM : AnalysisResources
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: dtaxml, formatdbprogloc, namespace, ncbi

        // ReSharper restore CommentTypo

        internal const string OMSSA_DEFAULT_INPUT_FILE = "OMSSA_default_input.xml";
        internal const string OMSSA_INPUT_FILE = "OMSSA_input.xml";
        protected RunDosProgram mCmdRunner;

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve Fasta file
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");
            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            LogMessage("Getting param file");

            // Retrieve param file
            if (!FileSearch.RetrieveFile(mJobParams.GetParam("ParmFileName"), mJobParams.GetParam("ParmFileStoragePath")))
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

            // Convert the .fasta file to OMSSA format using formatdb.exe
            var success = ConvertOMSSAFastaFile();
            if (!success)
            {
                LogError("AnalysisResourcesOM.GetResources(), failed converting fasta file to OMSSA format");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Retrieve settings files aka default file that will have values overwritten by parameter file values
            // Stored in same location as parameter file
            //         mJobParams.GetParam("SettingsFileName"), _
            if (!FileSearch.RetrieveFile(OMSSA_DEFAULT_INPUT_FILE, mJobParams.GetParam("ParmFileStoragePath")))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileExtensionToSkip(OMSSA_DEFAULT_INPUT_FILE);

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            if (!FileSearch.RetrieveDtaFiles())
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            success = ConvertDtaToXml();
            if (!success)
            {
                LogError("AnalysisResourcesOM.GetResources(), failed converting DTA file to xml format");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Add all the extensions of the files to delete after run
            mJobParams.AddResultFileExtensionToSkip("_dta.zip");   // Zipped DTA
            mJobParams.AddResultFileExtensionToSkip("_dta.txt");   // Unzipped, concatenated DTA
            mJobParams.AddResultFileExtensionToSkip(".dta");       // DTA files
            mJobParams.AddResultFileExtensionToSkip(DatasetName + ".xml");

            // set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
            success = MakeInputFile(out var errorMessage);

            if (!success)
            {
                var msg = "AnalysisResourcesOM.GetResources(), failed making input file: " + errorMessage;
                LogError(msg);
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected bool ConvertOMSSAFastaFile()
        {
            try
            {
                // set up formatdb.exe to reference the organism DB file (fasta)
                var OrgDBName = mJobParams.GetParam("PeptideSearch", "generatedFastaName");
                var LocalOrgDBFolder = mMgrParams.GetParam("OrgDbDir");

                LogMessage("Running formatdb.exe");

                mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
                RegisterEvents(mCmdRunner);

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerOM.OperateAnalysisTool(): Enter");
                }

                // verify that program formatdb.exe file exists
                var progLoc = mMgrParams.GetParam("formatdbprogloc");
                if (!File.Exists(progLoc))
                {
                    if (progLoc.Length == 0)
                        progLoc = "Parameter 'formatdbprogloc' not defined for this manager";
                    LogError("Cannot find OMSSA program file: " + progLoc);
                    return false;
                }

                // Set up and execute a program runner to run FormatDb.exe
                // formatdb.exe -i C:\DMS_WorkDir\Shewanella_oneidensis_MR1_Stop-to-Start_2005-10-12.fasta -p T -o T
                var arguments = " -i" + Path.Combine(LocalOrgDBFolder, OrgDBName) +
                                " -p T" +
                                " -o T";

                if (mDebugLevel >= 2)
                {
                    LogDebug("Starting FormatDb: " + progLoc + " " + arguments);
                }

                if (!mCmdRunner.RunProgram(progLoc, arguments, "FormatDb", true))
                {
                    LogError("Error running FormatDb for fasta file " + OrgDBName);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("AnalysisResourcesOM.ConvertOMSSAFastaFile, FormatDB error. " + ex.Message);
            }

            return true;
        }

        protected bool ConvertDtaToXml()
        {
            var blnSuccess = false;

            try
            {
                // Convert the _DTA.txt file to a DTA .XML file
                var sourceFilePath = Path.Combine(mWorkDir, DatasetName + "_dta.txt");

                var objDtaConverter = new DtaTextConverter.clsDtaTextToDtaXML
                {
                    // Make sure this is 0 so that all data in the _dta.txt file is transferred to the DTA .xml file
                    MaximumIonsPerSpectrum = 0,
                    ShowMessages = false,
                    LogMessagesToFile = false
                };

                if (mDebugLevel >= 2)
                {
                    LogDebug("Converting _DTA.txt file to DTA XML file using the DtaTextConverter");
                }

                blnSuccess = objDtaConverter.ProcessFile(sourceFilePath, mWorkDir);

                if (!blnSuccess)
                {
                    LogError("Error calling DtaTextConverter: " + objDtaConverter.GetErrorMessage());
                }
                else
                {
                    if (mDebugLevel >= 1)
                    {
                        LogDebug("DTA XML file created for " + Path.GetFileName(sourceFilePath));
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("AnalysisResourcesOM.ConvertDtaToXml, File conversion error. " + ex.Message);
            }

            return blnSuccess;
        }

        protected bool MakeInputFile(out string errorMessage)
        {
            var OmssaDefaultInput = Path.Combine(mWorkDir, OMSSA_DEFAULT_INPUT_FILE);
            var OmssaInput = Path.Combine(mWorkDir, OMSSA_INPUT_FILE);
            var ParamFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("parmFileName"));

            var SearchSettings = Path.Combine(mMgrParams.GetParam("OrgDbDir"), mJobParams.GetParam("PeptideSearch", "generatedFastaName"));
            var MSInfilename = Path.Combine(mWorkDir, DatasetName + ".xml");
            var MSOmxOutFilename = Path.Combine(mWorkDir, DatasetName + "_om.omx");
            var MSOmxLargeOutFilename = Path.Combine(mWorkDir, DatasetName + "_om_large.omx");
            mJobParams.AddResultFileExtensionToSkip(DatasetName + "_om_large.omx");
            var MSCsvOutFilename = Path.Combine(mWorkDir, DatasetName + "_om.csv");

            XmlNode mostRecentComment = null;

            try
            {
                var templateFile = new FileInfo(OmssaDefaultInput);
                var fileToMerge = new FileInfo(ParamFilePath);

                if (!templateFile.Exists)
                {
                    errorMessage = "File not found: " + templateFile.FullName;
                    return false;
                }

                if (!fileToMerge.Exists)
                {
                    errorMessage = "File not found: " + fileToMerge.FullName;
                    return false;
                }

                // Construct the name of the new .XML file
                var strOutputFilePath = OmssaInput;

                // Open the template XML file
                var xmlTemplate = new XmlDocument {
                    PreserveWhitespace = true
                };

                try
                {
                    xmlTemplate.Load(templateFile.FullName);
                }
                catch (Exception ex)
                {
                    errorMessage = "Error loading file " + templateFile.Name + ": " + ex.Message;
                    return false;
                }

                // Open the file to be merged
                var xmlDoc = new XmlDocument {
                    PreserveWhitespace = true
                };

                try
                {
                    xmlDoc.Load(fileToMerge.FullName);
                }
                catch (Exception ex)
                {
                    errorMessage = "Error loading file " + fileToMerge.Name + ": " + ex.Message;
                    return false;
                }

                // Define the namespace manager
                // Required because the template uses namespace "http://www.ncbi.nlm.nih.gov"
                var objNamespaceMgr = new XmlNamespaceManager(xmlTemplate.NameTable);
                objNamespaceMgr.AddNamespace("ncbi", "http://www.ncbi.nlm.nih.gov");

                // Read each node xmlDoc
                foreach (XmlNode node in xmlDoc.DocumentElement.ChildNodes)
                {
                    XmlNode objImportedNode;
                    if (node.NodeType == XmlNodeType.Comment)
                    {
                        // Save the most recent comment to possibly be included later

                        // Note that we have to use .ImportNode, otherwise we'll get a namespace error when we try to add the new node
                        objImportedNode = xmlTemplate.ImportNode(node, true);

                        mostRecentComment = objImportedNode.CloneNode(true);
                    }
                    else if (node.NodeType == XmlNodeType.Element)
                    {
                        // Note that we have to use .ImportNode, otherwise we'll get a namespace error when we try to add the new node
                        objImportedNode = xmlTemplate.ImportNode(node, true);

                        // Look for this node in xmlTemplate
                        // The Do loop is required because we have to call .SelectNodes() again after removing any extra nodes
                        XmlNodeList selectedNodes;
                        int matchCount;
                        do
                        {
                            // This XPath statement says to:
                            //  1) Go to the Document Element
                            //  2) Search its descendants
                            //  3) Use the NCBI namespace when searching
                            //  4) Find the node named objImportedNode.name
                            selectedNodes = xmlTemplate.DocumentElement.SelectNodes("descendant::ncbi:" + objImportedNode.Name, objNamespaceMgr);

                            if (selectedNodes == null)
                            {
                                matchCount = 0;
                            }
                            else
                            {
                                matchCount = selectedNodes.Count;
                            }

                            if (matchCount > 1)
                            {
                                // More than one node was matched
                                // Delete the extra nodes
                                for (var i = matchCount - 1; i >= 1; i += -1)
                                {
                                    selectedNodes.Item(i).ParentNode.RemoveChild(selectedNodes.Item(i));
                                }
                            }
                        } while (matchCount > 1);

                        XmlDocumentFragment objFrag;
                        if (matchCount == 0)
                        {
                            // Match wasn't found; need to add a new node
                            // Append this temporary node to the end of the "to" document
                            // but inside the root element.

                            try
                            {
                                if (mostRecentComment != null)
                                {
                                    // First append the most recent comment

                                    objFrag = xmlTemplate.CreateDocumentFragment();
                                    objFrag.AppendChild(xmlTemplate.CreateSignificantWhitespace("  "));
                                    objFrag.AppendChild(mostRecentComment);
                                    objFrag.AppendChild(xmlTemplate.CreateSignificantWhitespace(Environment.NewLine + "  "));

                                    xmlTemplate.DocumentElement.AppendChild(objFrag);

                                    mostRecentComment = null;
                                }
                            }
                            catch (Exception ex)
                            {
                                errorMessage = "Error appending comment for node " + objImportedNode.Name + ": " + ex.Message;
                                return false;
                            }

                            try
                            {
                                // Now append the node
                                xmlTemplate.DocumentElement.AppendChild(objImportedNode);
                                xmlTemplate.DocumentElement.AppendChild(xmlTemplate.CreateSignificantWhitespace(Environment.NewLine + Environment.NewLine));
                            }
                            catch (Exception ex)
                            {
                                errorMessage = "Error appending new node " + objImportedNode.Name + ": " + ex.Message;
                                return false;
                            }
                        }
                        else
                        {
                            // Match was found

                            if (mostRecentComment != null)
                            {
                                try
                                {
                                    // Possibly add this comment just before the current node
                                    // However, see if a duplicate comment already exists
                                    var previousNode = selectedNodes.Item(0).PreviousSibling;

                                    while (previousNode?.NodeType == XmlNodeType.Whitespace)
                                    {
                                        // previousNode is currently whitespace
                                        // Move back one node
                                        previousNode = previousNode.PreviousSibling;
                                    }

                                    var copyThisComment = true;
                                    if (previousNode?.NodeType == XmlNodeType.Comment)
                                    {
                                        if (previousNode.InnerText == mostRecentComment.InnerText)
                                        {
                                            // The comments match; skip this comment
                                            copyThisComment = false;
                                        }
                                    }

                                    if (copyThisComment)
                                    {
                                        objFrag = xmlTemplate.CreateDocumentFragment();
                                        objFrag.AppendChild(mostRecentComment);
                                        objFrag.AppendChild(xmlTemplate.CreateSignificantWhitespace(Environment.NewLine + "  "));

                                        selectedNodes.Item(0).ParentNode.InsertBefore(objFrag, selectedNodes.Item(0));
                                    }
                                }
                                catch (Exception ex)
                                {
                                    errorMessage = "Error appending comment for node " + objImportedNode.Name + ": " + ex.Message;
                                    return false;
                                }

                                mostRecentComment = null;
                            }

                            try
                            {
                                // Replace selectedNodes.Item(0) with objNodeToMerge
                                selectedNodes.Item(0).ParentNode.ReplaceChild(objImportedNode, selectedNodes.Item(0));

                                // Alternative would be to update the XML using .InnerXML
                                // However, this would miss any attributes for this element
                                // selectedNodes.Item(0).InnerXml = objImportedNode.InnerXml
                            }
                            catch (Exception ex)
                            {
                                errorMessage = "Error updating node " + objImportedNode.Name + ": " + ex.Message;
                                return false;
                            }
                        }
                    }
                }

                // Now override the values for MSInFile_infile and MSSpectrumFileType
                try
                {
                    var fileNameNodes = xmlTemplate.DocumentElement.SelectNodes(
                        "/ncbi:MSSearchSettings/ncbi:MSSearchSettings_infiles/ncbi:MSInFile/ncbi:MSInFile_infile",
                        objNamespaceMgr);

                    var fileTypeNodes = xmlTemplate.DocumentElement.SelectNodes(
                        "/ncbi:MSSearchSettings/ncbi:MSSearchSettings_infiles/ncbi:MSInFile/ncbi:MSInFile_infiletype/ncbi:MSSpectrumFileType",
                        objNamespaceMgr);

                    if (fileNameNodes == null || fileNameNodes.Count == 0)
                    {
                        errorMessage = "Did not find the MSInFile_infile node in the template file";
                        return false;
                    }

                    if (fileTypeNodes == null || fileTypeNodes.Count == 0)
                    {
                        errorMessage = "Did not find the MSSpectrumFileType node in the template file";
                        return false;
                    }

                    if (fileNameNodes.Count > 1)
                    {
                        errorMessage = "Found multiple instances of the MSInFile_infile node in the template file";
                        return false;
                    }

                    if (fileTypeNodes.Count > 1)
                    {
                        errorMessage = "Found multiple instances of the MSSpectrumFileType node in the template file";
                        return false;
                    }

                    // Everything is fine; update these nodes
                    // Note: File type 2 means a dtaxml file
                    fileNameNodes.Item(0).InnerXml = MSInfilename;
                    fileTypeNodes.Item(0).InnerXml = "2";
                }
                catch (Exception ex)
                {
                    errorMessage = "Error updating the MSInFile nodes: " + ex.Message;
                    return false;
                }

                // Now override the values for MSSearchSettings_db
                try
                {
                    var fileNameNodes = xmlTemplate.DocumentElement.SelectNodes("/ncbi:MSSearchSettings/ncbi:MSSearchSettings_db", objNamespaceMgr);

                    if (fileNameNodes.Count == 0)
                    {
                        errorMessage = "Did not find the MSSearchSettings_db node in the template file";
                        return false;
                    }

                    if (fileNameNodes.Count > 1)
                    {
                        errorMessage = "Found multiple instances of the MSSearchSettings_db node in the template file";
                        return false;
                    }

                    // Everything is fine; update node
                    fileNameNodes.Item(0).InnerXml = SearchSettings;
                }
                catch (Exception ex)
                {
                    errorMessage = "Error updating the MSSearchSettings_db node: " + ex.Message;
                    return false;
                }

                // Now override the values for MSOutFile_outfile and MSSerialDataFormat
                try
                {
                    // If we ever have to change the value of the MSOutFile_includerequest value
                    // var fileIncludeRequestNodes = xmlTemplate.DocumentElement.SelectNodes(
                    //   "/ncbi:MSSearchSettings/ncbi:MSSearchSettings_outfiles/ncbi:MSOutFile/ncbi:MSOutFile_includerequest[@value='false']",
                    //   objNamespaceMgr)
                    // fileIncludeRequestNodes.Item(1).InnerXml = "true"

                    var fileNameNodes = xmlTemplate.DocumentElement.SelectNodes(
                        "/ncbi:MSSearchSettings/ncbi:MSSearchSettings_outfiles/ncbi:MSOutFile/ncbi:MSOutFile_outfile",
                        objNamespaceMgr);

                    var fileTypeNodes = xmlTemplate.DocumentElement.SelectNodes(
                        "/ncbi:MSSearchSettings/ncbi:MSSearchSettings_outfiles/ncbi:MSOutFile/ncbi:MSOutFile_outfiletype/ncbi:MSSerialDataFormat",
                        objNamespaceMgr);

                    if (fileNameNodes== null || fileNameNodes.Count == 0)
                    {
                        errorMessage = "Did not find the MSOutFile_outfile node in the template file";
                        return false;
                    }

                    if (fileTypeNodes == null || fileTypeNodes.Count == 0)
                    {
                        errorMessage = "Did not find the MSSerialDataFormat node in the template file";
                        return false;
                    }

                    if (fileNameNodes.Count != fileTypeNodes.Count)
                    {
                        errorMessage = "The number of MSOutFile_outfile nodes doesn't match the number of MSSerialDataFormat nodes";
                        return false;
                    }

                    // Everything is fine; update these nodes
                    // Note: File type 3 means an XML file
                    fileNameNodes.Item(0).InnerXml = MSOmxOutFilename;
                    fileTypeNodes.Item(0).InnerXml = "3";

                    if (fileNameNodes.Count > 1)
                    {
                        // Note: File type 3 means a xml file
                        fileNameNodes.Item(1).InnerXml = MSOmxLargeOutFilename;
                        fileTypeNodes.Item(1).InnerXml = "3";
                    }
                    else
                    {
                        // Template only has one MSOutFile node tree defined
                        // Nothing else to update
                    }

                    if (fileNameNodes.Count > 2)
                    {
                        // Note: File type 4 means a CSV file
                        fileNameNodes.Item(2).InnerXml = MSCsvOutFilename;
                        fileTypeNodes.Item(2).InnerXml = "4";
                    }
                    else
                    {
                        // Template only has one MSOutFile node tree defined
                        // Nothing else to update
                    }
                }
                catch (Exception ex)
                {
                    errorMessage = "Error updating the MSOutfile nodes: " + ex.Message;
                    return false;
                }

                // Write out the new file
                try
                {
                    var objWriterSettings = new XmlWriterSettings
                    {
                        Indent = true,
                        IndentChars = "  ",
                        NewLineOnAttributes = true
                    };

                    var objWriter = XmlWriter.Create(strOutputFilePath, objWriterSettings);

                    objWriter.WriteRaw(xmlTemplate.DocumentElement.OuterXml);
                    objWriter.Close();
                }
                catch (Exception ex)
                {
                    errorMessage = "Error creating new XML file (" + Path.GetFileName(strOutputFilePath) + "): " + ex.Message;
                    return false;
                }
            }
            catch (Exception ex)
            {
                errorMessage = "General exception: " + ex.Message;
                return false;
            }

            errorMessage = string.Empty;
            return true;
        }
    }
}
