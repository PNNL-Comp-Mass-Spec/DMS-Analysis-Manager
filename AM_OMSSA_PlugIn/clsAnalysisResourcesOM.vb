Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesOM
    Inherits clsAnalysisResources

    Friend Const OMSSA_DEFAULT_INPUT_FILE As String = "OMSSA_default_input.xml"
    Friend Const OMSSA_INPUT_FILE As String = "OMSSA_input.xml"
    Protected WithEvents CmdRunner As clsRunDosProgram

    Private mDataset As String = String.Empty

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim strErrorMessage As String = String.Empty
        Dim blnSuccess As Boolean

        mDataset = m_jobParams.GetParam("DatasetNum")

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' OMSSA just copies its parameter file from the central repository
        '	This will eventually be replaced by Ken Auberry dll call to make param file on the fly

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

        'Retrieve param file
        If Not RetrieveFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         m_WorkingDir) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'convert the .fasta file to OMSSA format using formatdb.exe
        blnSuccess = ConvertOMSSAFastaFile()
        If Not blnSuccess Then
            Dim Msg As String = "clsAnalysisResourcesOM.GetResources(), failed converting fasta file to OMSSA format."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Retrieve settings files aka default file that will have values overwritten by parameter file values
        'Stored in same location as parameter file
        '         m_jobParams.GetParam("SettingsFileName"), _
        If Not RetrieveFile(OMSSA_DEFAULT_INPUT_FILE, _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         m_WorkingDir) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        clsGlobal.m_FilesToDeleteExt.Add(OMSSA_DEFAULT_INPUT_FILE)

        'Retrieve unzipped dta files (do not unconcatenate since we will convert the _DTA.txt file to a _DTA.xml file, which OMSSA will read)
        If Not RetrieveDtaFiles(False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        blnSuccess = ConvertDtaToXml()
        If Not blnSuccess Then
            Dim Msg As String = "clsAnalysisResourcesOM.GetResources(), failed converting dta file to xml format."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        clsGlobal.m_FilesToDeleteExt.Add("_dta.zip") 'Zipped DTA
        clsGlobal.m_FilesToDeleteExt.Add("_dta.txt") 'Unzipped, concatenated DTA
        clsGlobal.m_FilesToDeleteExt.Add(".dta")  'DTA files
        clsGlobal.m_FilesToDeleteExt.Add(mDataset & ".xml")

        ' set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
        blnSuccess = MakeInputFile(strErrorMessage)

        If Not blnSuccess Then
            Dim Msg As String = "clsAnalysisResourcesOM.GetResources(), failed making input file: " & strErrorMessage
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function ConvertOMSSAFastaFile() As Boolean
        Dim CmdStr As String
        Dim result As Boolean = True

        Try
            ' set up formatdb.exe to reference the organsim DB file (fasta)
            Dim OrgDBName As String = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
            Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running formatdb.exe")

            CmdRunner = New clsRunDosProgram(m_WorkingDir)

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerOM.OperateAnalysisTool(): Enter")
            End If

            ' verify that program formatdb.exe file exists
            Dim progLoc As String = m_mgrParams.GetParam("formatdbprogloc")
            If Not System.IO.File.Exists(progLoc) Then
                If progLoc.Length = 0 Then progLoc = "Parameter 'formatdbprogloc' not defined for this manager"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find OMSSA program file: " & progLoc)
                Return False
            End If

            'Set up and execute a program runner to run FormatDb.exe
            'formatdb.exe -i C:\DMS_WorkDir\Shewanella_oneidensis_MR1_Stop-to-Start_2005-10-12.fasta -p T -o T
            CmdStr = "-i" & System.IO.Path.Combine(LocalOrgDBFolder, OrgDBName) & " -p T -o T"

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Starting FormatDb: " & progLoc & " " & CmdStr)
            End If

            If Not CmdRunner.RunProgram(progLoc, CmdStr, "FormatDb", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running FormatDb for fasta file " & OrgDBName)
                Return False
            End If

        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesOM.ConvertOMSSAFastaFile, FormatDB error. " & ex.Message)
        End Try

        Return result
    End Function

    Protected Function ConvertDtaToXml() As Boolean

        Dim objDtaConverter As DtaTextConverter.clsDtaTextToDtaXML

        Dim SourceFilePath As String

        Dim blnSuccess As Boolean = False

        Try
            ' Convert the _DTA.txt file to a DTA .XML file
            SourceFilePath = System.IO.Path.Combine(m_WorkingDir, mDataset & "_dta.txt")

            objDtaConverter = New DtaTextConverter.clsDtaTextToDtaXML

            ' Make sure this is 0 so that all data in the _dta.txt file is transferred to the DTA .xml file
            objDtaConverter.MaximumIonsPerSpectrum = 0
            objDtaConverter.ShowMessages = False
            objDtaConverter.LogMessagesToFile = False

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Converting _DTA.txt file to DTA XML file using the DtaTextConverter")
            End If

            blnSuccess = objDtaConverter.ProcessFile(SourceFilePath, m_WorkingDir)

            If Not blnSuccess Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error calling DtaTextConverter: " & objDtaConverter.GetErrorMessage())
            Else
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DTA XML file created for " & System.IO.Path.GetFileName(SourceFilePath))
                End If
            End If

        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesOM.ConvertDtaToXml, File conversion error. " & ex.Message)
        End Try

        Return blnSuccess

    End Function

    Protected Function MakeInputFile(ByRef strErrorMessage As String) As Boolean

        Dim OmssaDefaultInput As String = System.IO.Path.Combine(m_WorkingDir, OMSSA_DEFAULT_INPUT_FILE)
        Dim OmssaInput As String = System.IO.Path.Combine(m_WorkingDir, OMSSA_INPUT_FILE)
        Dim ParamFilePath As String = System.IO.Path.Combine(m_WorkingDir, m_jobParams.GetParam("parmFileName"))
        Dim FastaFilename As String = System.IO.Path.Combine(m_WorkingDir, m_jobParams.GetParam("parmFileName"))

        Dim SearchSettings As String = System.IO.Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))
        Dim MSInfilename As String = System.IO.Path.Combine(m_WorkingDir, mDataset & ".xml")
        Dim MSOmxOutFilename As String = System.IO.Path.Combine(m_WorkingDir, mDataset & "_om.omx")
        Dim MSOmxLargeOutFilename As String = System.IO.Path.Combine(m_WorkingDir, mDataset & "_om_large.omx")
        clsGlobal.m_FilesToDeleteExt.Add(mDataset & "_om_large.omx")
        Dim MSCsvOutFilename As String = System.IO.Path.Combine(m_WorkingDir, mDataset & "_om.csv")

        Dim result As Boolean = True

        Dim strOutputFilePath As String

        Dim fiTemplateFile As System.IO.FileInfo
        Dim fiFileToMerge As System.IO.FileInfo

        Dim objNamespaceMgr As System.Xml.XmlNamespaceManager

        Dim objTemplate As System.Xml.XmlDocument
        Dim objFileToMerge As System.Xml.XmlDocument

        Dim objNodeToMerge As System.Xml.XmlNode
        Dim objImportedNode As System.Xml.XmlNode

        Dim objSelectedNodes As System.Xml.XmlNodeList
        Dim intMatchCount As Integer

        Dim objMostRecentComment As System.Xml.XmlNode
        Dim blnCopyThisComment As Boolean

        Dim objFrag As System.Xml.XmlDocumentFragment

        Dim objWriterSettings As System.Xml.XmlWriterSettings
        Dim objWriter As System.Xml.XmlWriter

        Try
            fiTemplateFile = New System.IO.FileInfo(OmssaDefaultInput)
            fiFileToMerge = New System.IO.FileInfo(ParamFilePath)

            If Not fiTemplateFile.Exists Then
                strErrorMessage = "File not found: " & fiTemplateFile.FullName
                Return False
            End If

            If Not fiFileToMerge.Exists Then
                strErrorMessage = "File not found: " & fiFileToMerge.FullName
                Return False
            End If

            ' Construct the name of the new .XML file
            strOutputFilePath = OmssaInput

            ' Open the template XML file
            objTemplate = New System.Xml.XmlDocument
            objTemplate.PreserveWhitespace = True
            Try
                objTemplate.Load(fiTemplateFile.FullName)
            Catch ex As Exception
                strErrorMessage = "Error loading file " & fiTemplateFile.Name & ": " & ex.Message
                Return False
            End Try

            ' Open the file to be merged
            objFileToMerge = New System.Xml.XmlDocument
            objFileToMerge.PreserveWhitespace = True
            Try
                objFileToMerge.Load(fiFileToMerge.FullName)
            Catch ex As Exception
                strErrorMessage = "Error loading file " & fiFileToMerge.Name & ": " & ex.Message
                Return False
            End Try

            ' Define the namespace manager
            ' Required because the template uses namespace "http://www.ncbi.nlm.nih.gov"
            objNamespaceMgr = New System.Xml.XmlNamespaceManager(objTemplate.NameTable)
            objNamespaceMgr.AddNamespace("ncbi", "http://www.ncbi.nlm.nih.gov")

            ' Read each node objFileToMerge
            For Each objNodeToMerge In objFileToMerge.DocumentElement.ChildNodes

                If objNodeToMerge.NodeType = Xml.XmlNodeType.Comment Then
                    ' Save the most recent comment to possibly be included later

                    ' Note that we have to use .ImportNode, otherwise we'll get a namespace error when we try to add the new node
                    objImportedNode = objTemplate.ImportNode(objNodeToMerge, True)

                    objMostRecentComment = objImportedNode.CloneNode(True)

                ElseIf objNodeToMerge.NodeType = Xml.XmlNodeType.Element Then

                    ' Note that we have to use .ImportNode, otherwise we'll get a namespace error when we try to add the new node
                    objImportedNode = objTemplate.ImportNode(objNodeToMerge, True)

                    ' Look for this node in objTemplate
                    ' The Do loop is required because we have to call .SelectNodes() again after removing any extra nodes
                    Do

                        ' This XPath statement says to:
                        '  1) Go to the Document Element
                        '  2) Search its descendants
                        '  3) Use the ncbi namespace when seraching
                        '  4) Find the node named objImportedNode.name
                        objSelectedNodes = objTemplate.DocumentElement.SelectNodes("descendant::ncbi:" & objImportedNode.Name, objNamespaceMgr)

                        If objSelectedNodes Is Nothing Then
                            intMatchCount = 0
                        Else
                            intMatchCount = objSelectedNodes.Count
                        End If

                        If intMatchCount > 1 Then
                            ' More than one node was matched
                            ' Delete the extra nodes
                            For i As Integer = intMatchCount - 1 To 1 Step -1
                                objSelectedNodes.Item(i).ParentNode.RemoveChild(objSelectedNodes.Item(i))
                            Next
                        End If

                    Loop While intMatchCount > 1

                    If intMatchCount = 0 Then
                        ' Match wasn't found; need to add a new node
                        ' Append this temporary node to the end of the "to" document
                        ' but inside the root element.

                        Try
                            If Not objMostRecentComment Is Nothing Then
                                ' First append the most recent comment

                                objFrag = objTemplate.CreateDocumentFragment()
                                objFrag.AppendChild(objTemplate.CreateSignificantWhitespace("  "))
                                objFrag.AppendChild(objMostRecentComment)
                                objFrag.AppendChild(objTemplate.CreateSignificantWhitespace(ControlChars.NewLine & "  "))

                                objTemplate.DocumentElement.AppendChild(objFrag)

                                objMostRecentComment = Nothing
                            End If
                        Catch ex As Exception
                            strErrorMessage = "Error appending comment for node " & objImportedNode.Name & ": " & ex.Message
                            Return False
                        End Try


                        Try
                            ' Now append the node
                            objTemplate.DocumentElement.AppendChild(objImportedNode)
                            objTemplate.DocumentElement.AppendChild(objTemplate.CreateSignificantWhitespace(ControlChars.NewLine & ControlChars.NewLine))

                        Catch ex As Exception
                            strErrorMessage = "Error appending new node " & objImportedNode.Name & ": " & ex.Message
                            Return False
                        End Try


                    Else
                        ' Match was found

                        If Not objMostRecentComment Is Nothing Then
                            Try
                                ' Possibly add this comment just before the current node
                                ' However, see if a duplicate comment already exists
                                Dim objPrevNode As System.Xml.XmlNode
                                objPrevNode = objSelectedNodes.Item(0).PreviousSibling

                                Do While Not objPrevNode Is Nothing AndAlso objPrevNode.NodeType = Xml.XmlNodeType.Whitespace
                                    ' objPrevNode is currently whitespace
                                    ' Move back one node
                                    objPrevNode = objPrevNode.PreviousSibling
                                Loop

                                blnCopyThisComment = True
                                If Not objPrevNode Is Nothing AndAlso objPrevNode.NodeType = Xml.XmlNodeType.Comment Then
                                    If objPrevNode.InnerText = objMostRecentComment.InnerText Then
                                        ' The comments match; skip this comment
                                        blnCopyThisComment = False
                                    End If
                                End If

                                If blnCopyThisComment Then
                                    objFrag = objTemplate.CreateDocumentFragment()
                                    objFrag.AppendChild(objMostRecentComment)
                                    objFrag.AppendChild(objTemplate.CreateSignificantWhitespace(ControlChars.NewLine & "  "))

                                    objSelectedNodes.Item(0).ParentNode.InsertBefore(objFrag, objSelectedNodes.Item(0))
                                End If

                            Catch ex As Exception
                                strErrorMessage = "Error appending comment for node " & objImportedNode.Name & ": " & ex.Message
                                Return False
                            End Try

                            objMostRecentComment = Nothing
                        End If

                        Try
                            ' Replace objSelectedNodes.Item(0) with objNodeToMerge
                            objSelectedNodes.Item(0).ParentNode.ReplaceChild(objImportedNode, objSelectedNodes.Item(0))

                            ' Alternative would be to update the XML using .InnerXML
                            ' However, this would miss any attributes foor this element
                            'objSelectedNodes.Item(0).InnerXml = objImportedNode.InnerXml

                        Catch ex As Exception
                            strErrorMessage = "Error updating node " & objImportedNode.Name & ": " & ex.Message
                            Return False
                        End Try

                    End If

                End If

            Next

            ' Now override the values for MSInFile_infile and MSSpectrumFileType
            Try

                Dim objFileNameNodes As System.Xml.XmlNodeList
                Dim objFileTypeNodes As System.Xml.XmlNodeList

                objFileNameNodes = objTemplate.DocumentElement.SelectNodes("/ncbi:MSSearchSettings/ncbi:MSSearchSettings_infiles/ncbi:MSInFile/ncbi:MSInFile_infile", objNamespaceMgr)

                objFileTypeNodes = objTemplate.DocumentElement.SelectNodes("/ncbi:MSSearchSettings/ncbi:MSSearchSettings_infiles/ncbi:MSInFile/ncbi:MSInFile_infiletype/ncbi:MSSpectrumFileType", objNamespaceMgr)


                If objFileNameNodes.Count = 0 Then
                    strErrorMessage = "Did not find the MSInFile_infile node in the template file"
                    Return False
                ElseIf objFileTypeNodes.Count = 0 Then
                    strErrorMessage = "Did not find the MSSpectrumFileType node in the template file"
                    Return False
                End If

                If objFileNameNodes.Count > 1 Then
                    strErrorMessage = "Found multiple instances of the MSInFile_infile node in the template file"
                    Return False
                ElseIf objFileTypeNodes.Count > 1 Then
                    strErrorMessage = "Found multiple instances of the MSSpectrumFileType node in the template file"
                    Return False
                End If

                ' Everything is fine; update these nodes
                ' Note: File type 2 means a dtaxml file
                objFileNameNodes.Item(0).InnerXml = MSInfilename
                objFileTypeNodes.Item(0).InnerXml = "2"

            Catch ex As Exception
                strErrorMessage = "Error updating the MSInFile nodes: " & ex.Message
                Return False
            End Try


            ' Now override the values for MSSearchSettings_db
            Try

                Dim objFileNameNodes As System.Xml.XmlNodeList

                objFileNameNodes = objTemplate.DocumentElement.SelectNodes("/ncbi:MSSearchSettings/ncbi:MSSearchSettings_db", objNamespaceMgr)

                If objFileNameNodes.Count = 0 Then
                    strErrorMessage = "Did not find the MSSearchSettings_db node in the template file"
                    Return False
                End If

                If objFileNameNodes.Count > 1 Then
                    strErrorMessage = "Found multiple instances of the MSSearchSettings_db node in the template file"
                    Return False
                End If

                ' Everything is fine; update node
                objFileNameNodes.Item(0).InnerXml = SearchSettings

            Catch ex As Exception
                strErrorMessage = "Error updating the MSSearchSettings_db node: " & ex.Message
                Return False
            End Try


            ' Now override the values for MSOutFile_outfile and MSSerialDataFormat
            Try

                Dim objFileNameNodes As System.Xml.XmlNodeList
                Dim objFileTypeNodes As System.Xml.XmlNodeList

                'If we ever have to change the value of the MSOutFile_includerequest value 
                'Dim objFileIncludeRequestNodes As System.Xml.XmlNodeList
                'objFileIncludeRequestNodes = objTemplate.DocumentElement.SelectNodes("/ncbi:MSSearchSettings/ncbi:MSSearchSettings_outfiles/ncbi:MSOutFile/ncbi:MSOutFile_includerequest[@value='false']", objNamespaceMgr)
                'objFileIncludeRequestNodes.Item(1).InnerXml = "true"

                objFileNameNodes = objTemplate.DocumentElement.SelectNodes("/ncbi:MSSearchSettings/ncbi:MSSearchSettings_outfiles/ncbi:MSOutFile/ncbi:MSOutFile_outfile", objNamespaceMgr)

                objFileTypeNodes = objTemplate.DocumentElement.SelectNodes("/ncbi:MSSearchSettings/ncbi:MSSearchSettings_outfiles/ncbi:MSOutFile/ncbi:MSOutFile_outfiletype/ncbi:MSSerialDataFormat", objNamespaceMgr)


                If objFileNameNodes.Count = 0 Then
                    strErrorMessage = "Did not find the MSOutFile_outfile node in the template file"
                    Return False
                ElseIf objFileTypeNodes.Count = 0 Then
                    strErrorMessage = "Did not find the MSSerialDataFormat node in the template file"
                    Return False
                End If

                If objFileNameNodes.Count <> objFileTypeNodes.Count Then
                    strErrorMessage = "The number of MSOutFile_outfile nodes doesn't match the number of MSSerialDataFormat nodes"
                    Return False
                End If

                ' Everything is fine; update these nodes
                ' Note: File type 3 means an XML file
                objFileNameNodes.Item(0).InnerXml = MSOmxOutFilename
                objFileTypeNodes.Item(0).InnerXml = "3"

                If objFileNameNodes.Count > 1 Then
                    ' Note: File type 3 means a xml file
                    objFileNameNodes.Item(1).InnerXml = MSOmxLargeOutFilename
                    objFileTypeNodes.Item(1).InnerXml = "3"
                Else
                    ' Template only has one MSOutFile node tree defined
                    ' Nothing else to update
                End If

                If objFileNameNodes.Count > 2 Then
                    ' Note: File type 4 means a CSV file
                    objFileNameNodes.Item(2).InnerXml = MSCsvOutFilename
                    objFileTypeNodes.Item(2).InnerXml = "4"
                Else
                    ' Template only has one MSOutFile node tree defined
                    ' Nothing else to update
                End If


            Catch ex As Exception
                strErrorMessage = "Error updating the MSOutfile nodes: " & ex.Message
                Return False
            End Try


            ' Write out the new file

            Try
                objWriterSettings = New System.Xml.XmlWriterSettings
                objWriterSettings.Indent = True
                objWriterSettings.IndentChars = "  "
                objWriterSettings.NewLineOnAttributes = True


                objWriter = System.Xml.XmlTextWriter.Create(strOutputFilePath, objWriterSettings)

                objWriter.WriteRaw(objTemplate.DocumentElement.OuterXml)
                objWriter.Close()
            Catch ex As Exception
                strErrorMessage = "Error creating new XML file (" & System.IO.Path.GetFileName(strOutputFilePath) & "): " & ex.Message
                Return False
            End Try

        Catch ex As Exception
            strErrorMessage = "General exception: " & ex.Message
            Return False
        End Try

        Return True

    End Function

End Class
