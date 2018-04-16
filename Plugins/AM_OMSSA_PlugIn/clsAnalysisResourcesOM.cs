using System;
using System.IO;
using System.Xml;
using AnalysisManagerBase;

namespace AnalysisManagerOMSSAPlugIn
{
    /// <summary>
    /// Retrieve resources for the OMSSA plugin
    /// </summary>
    public class clsAnalysisResourcesOM : clsAnalysisResources
    {
        internal const string OMSSA_DEFAULT_INPUT_FILE = "OMSSA_default_input.xml";
        internal const string OMSSA_INPUT_FILE = "OMSSA_input.xml";
        protected clsRunDosProgram mCmdRunner;

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
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
            var orgDbDirectoryPath = m_mgrParams.GetParam("orgdbdir");
            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // OMSSA just copies its parameter file from the central repository
            //	This will eventually be replaced by Ken Auberry dll call to make param file on the fly

            LogMessage("Getting param file");

            // Retrieve param file
            if (!FileSearch.RetrieveFile(m_jobParams.GetParam("ParmFileName"), m_jobParams.GetParam("ParmFileStoragePath")))
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

            // Convert the .fasta file to OMSSA format using formatdb.exe
            var success = ConvertOMSSAFastaFile();
            if (!success)
            {
                LogError("clsAnalysisResourcesOM.GetResources(), failed converting fasta file to OMSSA format");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Retrieve settings files aka default file that will have values overwritten by parameter file values
            // Stored in same location as parameter file
            //         m_jobParams.GetParam("SettingsFileName"), _
            if (!FileSearch.RetrieveFile(OMSSA_DEFAULT_INPUT_FILE, m_jobParams.GetParam("ParmFileStoragePath")))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            m_jobParams.AddResultFileExtensionToSkip(OMSSA_DEFAULT_INPUT_FILE);

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
                LogError("clsAnalysisResourcesOM.GetResources(), failed converting dta file to xml format");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Add all the extensions of the files to delete after run
            m_jobParams.AddResultFileExtensionToSkip("_dta.zip");   // Zipped DTA
            m_jobParams.AddResultFileExtensionToSkip("_dta.txt");   // Unzipped, concatenated DTA
            m_jobParams.AddResultFileExtensionToSkip(".dta");       // DTA files
            m_jobParams.AddResultFileExtensionToSkip(DatasetName + ".xml");

            // set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
            success = MakeInputFile(out var errorMessage);

            if (!success)
            {
                var msg = "clsAnalysisResourcesOM.GetResources(), failed making input file: " + errorMessage;
                LogError(msg);
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected bool ConvertOMSSAFastaFile()
        {
            try
            {
                // set up formatdb.exe to reference the organsim DB file (fasta)
                var OrgDBName = m_jobParams.GetParam("PeptideSearch", "generatedFastaName");
                var LocalOrgDBFolder = m_mgrParams.GetParam("orgdbdir");

                LogMessage("Running formatdb.exe");

                mCmdRunner = new clsRunDosProgram(m_WorkingDir, m_DebugLevel);
                RegisterEvents(mCmdRunner);

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerOM.OperateAnalysisTool(): Enter");
                }

                // verify that program formatdb.exe file exists
                var progLoc = m_mgrParams.GetParam("formatdbprogloc");
                if (!File.Exists(progLoc))
                {
                    if (progLoc.Length == 0)
                        progLoc = "Parameter 'formatdbprogloc' not defined for this manager";
                    LogError("Cannot find OMSSA program file: " + progLoc);
                    return false;
                }

                // Set up and execute a program runner to run FormatDb.exe
                // formatdb.exe -i C:\DMS_WorkDir\Shewanella_oneidensis_MR1_Stop-to-Start_2005-10-12.fasta -p T -o T
                var cmdStr = "-i" + Path.Combine(LocalOrgDBFolder, OrgDBName) + " -p T -o T";

                if (m_DebugLevel >= 2)
                {
                    LogDebug("Starting FormatDb: " + progLoc + " " + cmdStr);
                }

                if (!mCmdRunner.RunProgram(progLoc, cmdStr, "FormatDb", true))
                {
                    LogError("Error running FormatDb for fasta file " + OrgDBName);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("clsAnalysisResourcesOM.ConvertOMSSAFastaFile, FormatDB error. " + ex.Message);
            }

            return true;
        }

        protected bool ConvertDtaToXml()
        {
            var blnSuccess = false;

            try
            {
                // Convert the _DTA.txt file to a DTA .XML file
                var sourceFilePath = Path.Combine(m_WorkingDir, DatasetName + "_dta.txt");

                var objDtaConverter = new DtaTextConverter.clsDtaTextToDtaXML
                {
                    // Make sure this is 0 so that all data in the _dta.txt file is transferred to the DTA .xml file
                    MaximumIonsPerSpectrum = 0,
                    ShowMessages = false,
                    LogMessagesToFile = false
                };



                if (m_DebugLevel >= 2)
                {
                    LogDebug("Converting _DTA.txt file to DTA XML file using the DtaTextConverter");
                }

                blnSuccess = objDtaConverter.ProcessFile(sourceFilePath, m_WorkingDir);

                if (!blnSuccess)
                {
                    LogError("Error calling DtaTextConverter: " + objDtaConverter.GetErrorMessage());
                }
                else
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogDebug("DTA XML file created for " + Path.GetFileName(sourceFilePath));
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("clsAnalysisResourcesOM.ConvertDtaToXml, File conversion error. " + ex.Message);
            }

            return blnSuccess;
        }

        protected bool MakeInputFile(out string errorMessage)
        {
            var OmssaDefaultInput = Path.Combine(m_WorkingDir, OMSSA_DEFAULT_INPUT_FILE);
            var OmssaInput = Path.Combine(m_WorkingDir, OMSSA_INPUT_FILE);
            var ParamFilePath = Path.Combine(m_WorkingDir, m_jobParams.GetParam("parmFileName"));

            var SearchSettings = Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("PeptideSearch", "generatedFastaName"));
            var MSInfilename = Path.Combine(m_WorkingDir, DatasetName + ".xml");
            var MSOmxOutFilename = Path.Combine(m_WorkingDir, DatasetName + "_om.omx");
            var MSOmxLargeOutFilename = Path.Combine(m_WorkingDir, DatasetName + "_om_large.omx");
            m_jobParams.AddResultFileExtensionToSkip(DatasetName + "_om_large.omx");
            var MSCsvOutFilename = Path.Combine(m_WorkingDir, DatasetName + "_om.csv");

            XmlNode objMostRecentComment = null;

            try
            {
                var fiTemplateFile = new FileInfo(OmssaDefaultInput);
                var fiFileToMerge = new FileInfo(ParamFilePath);

                if (!fiTemplateFile.Exists)
                {
                    errorMessage = "File not found: " + fiTemplateFile.FullName;
                    return false;
                }

                if (!fiFileToMerge.Exists)
                {
                    errorMessage = "File not found: " + fiFileToMerge.FullName;
                    return false;
                }

                // Construct the name of the new .XML file
                var strOutputFilePath = OmssaInput;

                // Open the template XML file
                var objTemplate = new XmlDocument {
                    PreserveWhitespace = true
                };

                try
                {
                    objTemplate.Load(fiTemplateFile.FullName);
                }
                catch (Exception ex)
                {
                    errorMessage = "Error loading file " + fiTemplateFile.Name + ": " + ex.Message;
                    return false;
                }

                // Open the file to be merged
                var objFileToMerge = new XmlDocument {
                    PreserveWhitespace = true
                };

                try
                {
                    objFileToMerge.Load(fiFileToMerge.FullName);
                }
                catch (Exception ex)
                {
                    errorMessage = "Error loading file " + fiFileToMerge.Name + ": " + ex.Message;
                    return false;
                }

                // Define the namespace manager
                // Required because the template uses namespace "http://www.ncbi.nlm.nih.gov"
                var objNamespaceMgr = new XmlNamespaceManager(objTemplate.NameTable);
                objNamespaceMgr.AddNamespace("ncbi", "http://www.ncbi.nlm.nih.gov");

                // Read each node objFileToMerge
                foreach (XmlNode objNodeToMerge in objFileToMerge.DocumentElement.ChildNodes)
                {
                    XmlNode objImportedNode;
                    if (objNodeToMerge.NodeType == XmlNodeType.Comment)
                    {
                        // Save the most recent comment to possibly be included later

                        // Note that we have to use .ImportNode, otherwise we'll get a namespace error when we try to add the new node
                        objImportedNode = objTemplate.ImportNode(objNodeToMerge, true);

                        objMostRecentComment = objImportedNode.CloneNode(true);
                    }
                    else if (objNodeToMerge.NodeType == XmlNodeType.Element)
                    {
                        // Note that we have to use .ImportNode, otherwise we'll get a namespace error when we try to add the new node
                        objImportedNode = objTemplate.ImportNode(objNodeToMerge, true);

                        // Look for this node in objTemplate
                        // The Do loop is required because we have to call .SelectNodes() again after removing any extra nodes
                        XmlNodeList objSelectedNodes;
                        int intMatchCount;
                        do
                        {
                            // This XPath statement says to:
                            //  1) Go to the Document Element
                            //  2) Search its descendants
                            //  3) Use the ncbi namespace when seraching
                            //  4) Find the node named objImportedNode.name
                            objSelectedNodes = objTemplate.DocumentElement.SelectNodes("descendant::ncbi:" + objImportedNode.Name, objNamespaceMgr);

                            if (objSelectedNodes == null)
                            {
                                intMatchCount = 0;
                            }
                            else
                            {
                                intMatchCount = objSelectedNodes.Count;
                            }

                            if (intMatchCount > 1)
                            {
                                // More than one node was matched
                                // Delete the extra nodes
                                for (var i = intMatchCount - 1; i >= 1; i += -1)
                                {
                                    objSelectedNodes.Item(i).ParentNode.RemoveChild(objSelectedNodes.Item(i));
                                }
                            }
                        } while (intMatchCount > 1);

                        XmlDocumentFragment objFrag;
                        if (intMatchCount == 0)
                        {
                            // Match wasn't found; need to add a new node
                            // Append this temporary node to the end of the "to" document
                            // but inside the root element.

                            try
                            {
                                if ((objMostRecentComment != null))
                                {
                                    // First append the most recent comment

                                    objFrag = objTemplate.CreateDocumentFragment();
                                    objFrag.AppendChild(objTemplate.CreateSignificantWhitespace("  "));
                                    objFrag.AppendChild(objMostRecentComment);
                                    objFrag.AppendChild(objTemplate.CreateSignificantWhitespace(Environment.NewLine + "  "));

                                    objTemplate.DocumentElement.AppendChild(objFrag);

                                    objMostRecentComment = null;
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
                                objTemplate.DocumentElement.AppendChild(objImportedNode);
                                objTemplate.DocumentElement.AppendChild(objTemplate.CreateSignificantWhitespace(Environment.NewLine + Environment.NewLine));
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

                            if ((objMostRecentComment != null))
                            {
                                try
                                {
                                    // Possibly add this comment just before the current node
                                    // However, see if a duplicate comment already exists
                                    var objPrevNode = objSelectedNodes.Item(0).PreviousSibling;

                                    while ((objPrevNode != null) && objPrevNode.NodeType == XmlNodeType.Whitespace)
                                    {
                                        // objPrevNode is currently whitespace
                                        // Move back one node
                                        objPrevNode = objPrevNode.PreviousSibling;
                                    }

                                    var blnCopyThisComment = true;
                                    if ((objPrevNode != null) && objPrevNode.NodeType == XmlNodeType.Comment)
                                    {
                                        if (objPrevNode.InnerText == objMostRecentComment.InnerText)
                                        {
                                            // The comments match; skip this comment
                                            blnCopyThisComment = false;
                                        }
                                    }

                                    if (blnCopyThisComment)
                                    {
                                        objFrag = objTemplate.CreateDocumentFragment();
                                        objFrag.AppendChild(objMostRecentComment);
                                        objFrag.AppendChild(objTemplate.CreateSignificantWhitespace(Environment.NewLine + "  "));

                                        objSelectedNodes.Item(0).ParentNode.InsertBefore(objFrag, objSelectedNodes.Item(0));
                                    }
                                }
                                catch (Exception ex)
                                {
                                    errorMessage = "Error appending comment for node " + objImportedNode.Name + ": " + ex.Message;
                                    return false;
                                }

                                objMostRecentComment = null;
                            }

                            try
                            {
                                // Replace objSelectedNodes.Item(0) with objNodeToMerge
                                objSelectedNodes.Item(0).ParentNode.ReplaceChild(objImportedNode, objSelectedNodes.Item(0));

                                // Alternative would be to update the XML using .InnerXML
                                // However, this would miss any attributes foor this element
                                // objSelectedNodes.Item(0).InnerXml = objImportedNode.InnerXml
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
                    var objFileNameNodes = objTemplate.DocumentElement.SelectNodes(
                        "/ncbi:MSSearchSettings/ncbi:MSSearchSettings_infiles/ncbi:MSInFile/ncbi:MSInFile_infile",
                        objNamespaceMgr);

                    var objFileTypeNodes = objTemplate.DocumentElement.SelectNodes(
                        "/ncbi:MSSearchSettings/ncbi:MSSearchSettings_infiles/ncbi:MSInFile/ncbi:MSInFile_infiletype/ncbi:MSSpectrumFileType",
                        objNamespaceMgr);

                    if (objFileNameNodes.Count == 0)
                    {
                        errorMessage = "Did not find the MSInFile_infile node in the template file";
                        return false;
                    }
                    else if (objFileTypeNodes.Count == 0)
                    {
                        errorMessage = "Did not find the MSSpectrumFileType node in the template file";
                        return false;
                    }

                    if (objFileNameNodes.Count > 1)
                    {
                        errorMessage = "Found multiple instances of the MSInFile_infile node in the template file";
                        return false;
                    }

                    if (objFileTypeNodes.Count > 1)
                    {
                        errorMessage = "Found multiple instances of the MSSpectrumFileType node in the template file";
                        return false;
                    }

                    // Everything is fine; update these nodes
                    // Note: File type 2 means a dtaxml file
                    objFileNameNodes.Item(0).InnerXml = MSInfilename;
                    objFileTypeNodes.Item(0).InnerXml = "2";
                }
                catch (Exception ex)
                {
                    errorMessage = "Error updating the MSInFile nodes: " + ex.Message;
                    return false;
                }

                // Now override the values for MSSearchSettings_db
                try
                {
                    var objFileNameNodes = objTemplate.DocumentElement.SelectNodes("/ncbi:MSSearchSettings/ncbi:MSSearchSettings_db", objNamespaceMgr);

                    if (objFileNameNodes.Count == 0)
                    {
                        errorMessage = "Did not find the MSSearchSettings_db node in the template file";
                        return false;
                    }

                    if (objFileNameNodes.Count > 1)
                    {
                        errorMessage = "Found multiple instances of the MSSearchSettings_db node in the template file";
                        return false;
                    }

                    // Everything is fine; update node
                    objFileNameNodes.Item(0).InnerXml = SearchSettings;
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
                    // Dim objFileIncludeRequestNodes As XmlNodeList
                    // objFileIncludeRequestNodes = objTemplate.DocumentElement.SelectNodes(
                    //   "/ncbi:MSSearchSettings/ncbi:MSSearchSettings_outfiles/ncbi:MSOutFile/ncbi:MSOutFile_includerequest[@value='false']",
                    //   objNamespaceMgr)
                    // objFileIncludeRequestNodes.Item(1).InnerXml = "true"

                    var objFileNameNodes = objTemplate.DocumentElement.SelectNodes(
                        "/ncbi:MSSearchSettings/ncbi:MSSearchSettings_outfiles/ncbi:MSOutFile/ncbi:MSOutFile_outfile",
                        objNamespaceMgr);

                    var objFileTypeNodes = objTemplate.DocumentElement.SelectNodes(
                        "/ncbi:MSSearchSettings/ncbi:MSSearchSettings_outfiles/ncbi:MSOutFile/ncbi:MSOutFile_outfiletype/ncbi:MSSerialDataFormat",
                        objNamespaceMgr);

                    if (objFileNameNodes.Count == 0)
                    {
                        errorMessage = "Did not find the MSOutFile_outfile node in the template file";
                        return false;
                    }

                    if (objFileTypeNodes.Count == 0)
                    {
                        errorMessage = "Did not find the MSSerialDataFormat node in the template file";
                        return false;
                    }

                    if (objFileNameNodes.Count != objFileTypeNodes.Count)
                    {
                        errorMessage = "The number of MSOutFile_outfile nodes doesn't match the number of MSSerialDataFormat nodes";
                        return false;
                    }

                    // Everything is fine; update these nodes
                    // Note: File type 3 means an XML file
                    objFileNameNodes.Item(0).InnerXml = MSOmxOutFilename;
                    objFileTypeNodes.Item(0).InnerXml = "3";

                    if (objFileNameNodes.Count > 1)
                    {
                        // Note: File type 3 means a xml file
                        objFileNameNodes.Item(1).InnerXml = MSOmxLargeOutFilename;
                        objFileTypeNodes.Item(1).InnerXml = "3";
                    }
                    else
                    {
                        // Template only has one MSOutFile node tree defined
                        // Nothing else to update
                    }

                    if (objFileNameNodes.Count > 2)
                    {
                        // Note: File type 4 means a CSV file
                        objFileNameNodes.Item(2).InnerXml = MSCsvOutFilename;
                        objFileTypeNodes.Item(2).InnerXml = "4";
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

                    objWriter.WriteRaw(objTemplate.DocumentElement.OuterXml);
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
