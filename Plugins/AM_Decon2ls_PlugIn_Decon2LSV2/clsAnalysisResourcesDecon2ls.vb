Option Strict On

Imports System.IO
Imports System.Linq
Imports System.Runtime.InteropServices
Imports AnalysisManagerBase
Imports ThermoRawFileReader
Imports UIMFLibrary

Public Class clsAnalysisResourcesDecon2ls
    Inherits clsAnalysisResources

    Public Const JOB_PARAM_PROCESSMSMS_AUTO_ENABLED = "DeconTools_ProcessMsMs_Auto_Enabled"

#Region "Methods"

    ''' <summary>
    ''' Retrieves files necessary for performance of Decon2ls analysis
    ''' </summary>
    ''' <returns>CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")

        Dim msXmlOutputType As String = m_jobParams.GetParam("MSXMLOutputType")

        If Not String.IsNullOrWhiteSpace(msXmlOutputType) Then
            Dim eResult As CloseOutType

            Select Case msXmlOutputType.ToLower()
                Case "mzxml"
                    eResult = GetMzXMLFile()
                Case "mzml"
                    eResult = GetMzMLFile()
                Case Else
                    m_message = "Unsupported value for MSXMLOutputType: " & msXmlOutputType
                    eResult = CloseOutType.CLOSEOUT_FAILED
            End Select

            If eResult <> CloseOutType.CLOSEOUT_SUCCESS Then
                Return eResult
            End If
        Else
            ' Get input data file
            If Not RetrieveSpectra(strRawDataType) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.")
                Return CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        m_jobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION)
        m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)
        m_jobParams.AddResultFileExtensionToSkip(DOT_WIFF_EXTENSION)
        m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)
        m_jobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION)

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Retrieve the parameter file
        Dim paramFileName = m_jobParams.GetParam("ParmFileName")
        Dim paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath")

        If Not RetrieveFile(paramFileName, paramFileStoragePath) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If Not ValidateDeconProcessingOptions(paramFileName) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' All finished
        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Read the setting for ProcessMSMS from the DeconTools parameter file
    ''' </summary>
    ''' <param name="fiParamFile"></param>
    ''' <param name="processMSMS">Output parameter: true if ProcessMSMS is True in the parameter file</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Private Function IsMSMSProcessingEnabled(fiParamFile As FileInfo, <Out()> ByRef processMSMS As Boolean) As Boolean

        processMSMS = False

        Try

            Using srParamFile = New FileStream(fiParamFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)

                ' Open the file and parse the XML
                Dim objParamFile = New Xml.XmlDocument()
                objParamFile.Load(srParamFile)

                ' Look for the XML: <ProcessMSMS></ProcessMSMS>
                Dim objNode = objParamFile.SelectSingleNode("//parameters/Miscellaneous/ProcessMSMS")

                If Not objNode Is Nothing AndAlso objNode.HasChildNodes Then
                    ' Match found; read the value
                    If Not Boolean.TryParse(objNode.ChildNodes(0).Value, processMSMS) Then
                        ' Parameter file formatting error
                        LogError("Invalid entry for ProcessMSMS in the parameter file; should be True or False")
                        Return False
                    End If
                End If

            End Using

            Return True
        Catch ex As Exception
            LogError("Error in IsMSMSProcessingEnabled", ex)
        End Try

        Return False

    End Function

    Private Function ValidateDeconProcessingOptions(paramFileName As String) As Boolean

        Dim fiParamFile = New FileInfo(Path.Combine(m_WorkingDir, paramFileName))

        If Not fiParamFile.Exists Then
            ' Parameter file not found
            Dim errMsg = "Decon2LS param file not found by ValidateDeconProcessingOptions"
            LogError(errMsg, errMsg & ": " & paramFileName)

            Return False
        End If

        Dim processMSMS As Boolean
        If Not IsMSMSProcessingEnabled(fiParamFile, processMSMS) Then
            Return False
        End If

        If processMSMS Then
            ' No need to perform any further validation
            Return True
        End If

        ' Open the instrument data file and determine whether it only contains MS/MS spectra
        ' If that is the case, update the parameter file to have ProcessMSMS=True

        Dim countMS1 = 0
        Dim countMSn = 0

        If Not ExamineDatasetScanTypes(countMS1, countMSn) Then
            Return False
        End If

        If countMS1 = 0 And countMSn > 0 Then
            If Not EnableMSMSProcessingInParamFile(fiParamFile) Then
                Return False
            End If
        End If

        Return True

    End Function

    ''' <summary>
    ''' Determine the number of MS1 and MS2 (or higher) spectra in a dataset
    ''' </summary>
    ''' <param name="countMs1"></param>
    ''' <param name="countMSn"></param>
    ''' <returns></returns>
    ''' <remarks>At present only supports Thermo .Raw files and .UIMF files</remarks>
    Private Function ExamineDatasetScanTypes(<Out()> ByRef countMs1 As Integer, <Out()> ByRef countMSn As Integer) As Boolean

        countMs1 = 0
        countMSn = 0

        Try

            Dim rawDataTypeName = GetRawDataTypeName()

            If String.IsNullOrWhiteSpace(rawDataTypeName) Then
                Return False
            End If

            ' Gets the Decon2LS file type based on the input data type
            Dim eRawDataType = GetRawDataType(rawDataTypeName)

            Dim datasetFilePath = clsAnalysisToolRunnerDecon2ls.GetInputFilePath(m_WorkingDir, m_DatasetName, eRawDataType)
            Dim success As Boolean

            Select Case eRawDataType

                Case eRawDataTypeConstants.ThermoRawFile
                    success = ExamineScanTypesInRawFile(datasetFilePath, countMs1, countMSn)

                Case eRawDataTypeConstants.UIMF '
                    success = ExamineScanTypesInUIMFFile(datasetFilePath, countMs1, countMSn)

                Case Else
                    ' Ignore datasets that are not .raw file or .uimf files
                    success = True
            End Select

            Return success

        Catch ex As Exception
            LogError("Error in ExamineDatasetScanTypes", ex)
        End Try

        Return False

    End Function

    Private Function ExamineScanTypesInRawFile(datasetFilePath As String, <Out()> ByRef countMs1 As Integer, <Out()> ByRef countMSn As Integer) As Boolean
        countMs1 = 0
        countMSn = 0

        Try
            Using rawFileReader = New XRawFileIO()
                If Not rawFileReader.OpenRawFile(datasetFilePath) Then
                    LogError("Error opening Thermo raw file " & Path.GetFileName(datasetFilePath))
                    Return False
                End If

                For scanNumber = rawFileReader.FileInfo.ScanStart To rawFileReader.FileInfo.ScanEnd
                    Dim scanInfo As clsScanInfo = Nothing
                    If rawFileReader.GetScanInfo(scanNumber, scanInfo) Then
                        If scanInfo.MSLevel = 1 Then
                            countMs1 += 1
                        Else
                            countMSn += 1
                        End If
                    End If
                Next

                rawFileReader.CloseRawFile()

            End Using

            Return True
        Catch ex As Exception
            LogError("Error in ExamineScansTypesInRawFile", ex)
            Return False
        End Try

    End Function

    Private Function ExamineScanTypesInUIMFFile(datasetFilePath As String, <Out()> ByRef countMs1 As Integer, <Out()> ByRef countMSn As Integer) As Boolean
        countMs1 = 0
        countMSn = 0

        Try

            Using reader = New UIMFLibrary.DataReader(datasetFilePath)
                Dim frameList = reader.GetMasterFrameList()

                Dim query = From item In frameList Where item.Value = DataReader.FrameType.MS1 Select item
                countMs1 = query.Count()

                countMSn = frameList.Count - countMs1
            End Using

            Return True

        Catch ex As Exception
            LogError("Error in ExamineScansTypesInRawFile", ex)
            Return False
        End Try

    End Function

    ''' <summary>
    '''Update the parameter file to have ProcessMSMS set to True
    ''' </summary>
    ''' <param name="fiParamFile"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function EnableMSMSProcessingInParamFile(fiParamFile As FileInfo) As Boolean

        Try
            Dim deconParamFilePath = String.Copy(fiParamFile.FullName)

            ' Rename the existing parameter file
            Dim newParamFilePath = Path.Combine(m_WorkingDir, fiParamFile.Name & ".old")
            m_jobParams.AddResultFileToSkip(newParamFilePath)

            fiParamFile.MoveTo(newParamFilePath)
            Threading.Thread.Sleep(250)

            ' Open the file and parse the XML
            Dim updatedXmlDoc = New Xml.XmlDocument
            updatedXmlDoc.Load(New FileStream(fiParamFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))

            ' Look for the XML: <ProcessMSMS></ProcessMSMS> in the Miscellaneous section
            ' Set its value to "True" (the setting is added if missing)
            WriteTempParamFileUpdateElementValue(updatedXmlDoc, "//parameters/Miscellaneous", "ProcessMSMS", "True")

            Try
                ' Now write out the XML to strParamFileTemp
                Using updatedParamFileWriter = New StreamWriter(New FileStream(deconParamFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                    Dim formattedXmlWriter = New Xml.XmlTextWriter(updatedParamFileWriter)
                    formattedXmlWriter.Indentation = 1
                    formattedXmlWriter.IndentChar = ControlChars.Tab
                    formattedXmlWriter.Formatting = Xml.Formatting.Indented

                    updatedXmlDoc.WriteContentTo(formattedXmlWriter)
                End Using

                m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAM_PROCESSMSMS_AUTO_ENABLED, True)
                Return True

            Catch ex As Exception
                LogError("Error writing new param file in EnableMSMSProcessingInParamFile", ex)
                Return False

            End Try

        Catch ex As Exception
            LogError("Error reading existing param file in EnableMSMSProcessingInParamFile", ex)
            Return False
        End Try


    End Function

    ''' <summary>
    ''' Looks for the section specified by parameter XPathForSection.  If found, updates its value to NewElementValue.  If not found, tries to add a new node with name ElementName
    ''' </summary>
    ''' <param name="xmlDoc">XML Document object</param>
    ''' <param name="XPathForSection">XPath specifying the section that contains the desired element.  For example: "//parameters/Miscellaneous"</param>
    ''' <param name="ElementName">Element name to find (or add)</param>
    ''' <param name="NewElementValue">New value for this element</param>
    ''' <remarks></remarks>
    Private Sub WriteTempParamFileUpdateElementValue(xmlDoc As Xml.XmlDocument, xpathForSection As String, elementName As String, newElementValue As String)
        Dim objNode As Xml.XmlNode
        Dim objNewChild As Xml.XmlElement

        objNode = xmlDoc.SelectSingleNode(xpathForSection & "/" & elementName)

        If Not objNode Is Nothing Then
            If objNode.HasChildNodes Then
                ' Match found; update the value
                objNode.ChildNodes(0).Value = newElementValue
            End If
        Else
            objNode = xmlDoc.SelectSingleNode(xpathForSection)

            If Not objNode Is Nothing Then
                objNewChild = CType(xmlDoc.CreateNode(Xml.XmlNodeType.Element, elementName, String.Empty), Xml.XmlElement)
                objNewChild.InnerXml = newElementValue

                objNode.AppendChild(objNewChild)
            End If

        End If

    End Sub

#End Region


End Class
