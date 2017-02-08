'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports PHRPReader
Imports System.IO

' ReSharper disable once UnusedMember.Global
Public Class clsAnalysisResourcesMSGF
    Inherits clsAnalysisResources

    '*********************************************************************************************************
    'Manages retrieval of all files needed by MSGF
    '*********************************************************************************************************

#Region "Constants"

    Public Const PHRP_MOD_DEFS_SUFFIX As String = "_ModDefs.txt"

#End Region

#Region "Module variables"
    ' Keys are the original file name, values are the new name
    Private m_PendingFileRenames As Dictionary(Of String, String)

#End Region

#Region "Methods"

    ''' <summary>
    ''' Gets all files needed by MSGF
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType specifying results</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As IJobParams.CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        m_PendingFileRenames = New Dictionary(Of String, String)

        Dim strScriptName As String = m_jobParams.GetParam("ToolName")

        If Not strScriptName.ToLower().StartsWith("MSGFPlus".ToLower()) Then

            ' Make sure the machine has enough free memory to run MSGF
            If Not ValidateFreeMemorySize("MSGFJavaMemorySize", "MSGF") Then
                m_message = "Not enough free memory to run MSGF"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        End If

        'Get analysis results files
        result = GetInputFiles(m_jobParams.GetParam("ResultType"))
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    End Function

    ''' <summary>
    ''' Retrieves input files needed for MSGF
    ''' </summary>
    ''' <param name="ResultType">String specifying type of analysis results input to extraction process</param>
    ''' <returns>IJobParams.CloseOutType specifying results</returns>
    ''' <remarks></remarks>
    Private Function GetInputFiles(resultType As String) As IJobParams.CloseOutType

        Dim fileToGet As String
        Dim strSynFilePath As String = String.Empty

        Dim blnSuccess As Boolean
        Dim blnOnlyCopyFHTandSYNfiles As Boolean

        ' Make sure the ResultType is valid
        Dim eResultType = clsPHRPReader.GetPeptideHitResultType(resultType)

        If eResultType = clsPHRPReader.ePeptideHitResultType.Sequest OrElse
           eResultType = clsPHRPReader.ePeptideHitResultType.XTandem OrElse
           eResultType = clsPHRPReader.ePeptideHitResultType.Inspect OrElse
           eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB OrElse      ' MSGF+
           eResultType = clsPHRPReader.ePeptideHitResultType.MODa OrElse
           eResultType = clsPHRPReader.ePeptideHitResultType.MODPlus OrElse
           eResultType = clsPHRPReader.ePeptideHitResultType.MSPathFinder Then

            blnSuccess = True

        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                 "Invalid tool result type (not supported by MSGF): " & resultType)
            blnSuccess = False
        End If

        If Not blnSuccess Then
            Return (IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES)
        End If

        ' Make sure the dataset type is valid
        Dim rawDataType = m_jobParams.GetParam("RawDataType")
        Dim eRawDataType = GetRawDataType(rawDataType)
        Dim blnMGFInstrumentData = m_jobParams.GetJobParameter("MGFInstrumentData", False)

        If eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
            ' We do not need the mzXML file, the parameter file, or various other files if we are running MSGF+ and running MSGF v6432 or later
            ' Determine this by looking for job parameter MSGF_Version

            Dim strMSGFStepToolVersion As String = m_jobParams.GetParam("MSGF_Version")

            If String.IsNullOrWhiteSpace(strMSGFStepToolVersion) Then
                ' Production version of MSGF+; don't need the parameter file, ModSummary file, or mzXML file
                blnOnlyCopyFHTandSYNfiles = True
            Else
                ' Specific version of MSGF is defined
                ' Check whether the version is one of the known versions for the old MSGF
                If clsMSGFRunner.IsLegacyMSGFVersion(strMSGFStepToolVersion) Then
                    blnOnlyCopyFHTandSYNfiles = False
                Else
                    blnOnlyCopyFHTandSYNfiles = True
                End If
            End If
        ElseIf eResultType = clsPHRPReader.ePeptideHitResultType.MODa Or
               eResultType = clsPHRPReader.ePeptideHitResultType.MODPlus Or
               eResultType = clsPHRPReader.ePeptideHitResultType.MSPathFinder Then

            ' We do not need any raw data files for MODa, modPlus, or MSPathFinder
            blnOnlyCopyFHTandSYNfiles = True

        Else
            ' Not running MSGF+ or running MSGF+ but using legacy msgf
            blnOnlyCopyFHTandSYNfiles = False

            If Not blnMGFInstrumentData Then
                Select Case eRawDataType
                    Case eRawDataTypeConstants.ThermoRawFile, eRawDataTypeConstants.mzML, eRawDataTypeConstants.mzXML
                        ' This is a valid data type
                    Case Else
                        m_message = "Dataset type " & rawDataType & " is not supported by MSGF"
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                             m_message & "; must be one of the following: " &
                                             RAW_DATA_TYPE_DOT_RAW_FILES & ", " & RAW_DATA_TYPE_DOT_MZML_FILES & ", " &
                                             RAW_DATA_TYPE_DOT_MZXML_FILES)
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End Select
            End If

        End If

        If Not blnOnlyCopyFHTandSYNfiles Then
            ' Get the Sequest, X!Tandem, Inspect, MSGF+, MODa, MODPlus, or MSPathFinder parameter file
            fileToGet = m_jobParams.GetParam("ParmFileName")
            If Not FindAndRetrieveMiscFiles(fileToGet, False) Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
            End If
            m_jobParams.AddResultFileToSkip(fileToGet)

            ' Also copy the _ProteinMods.txt file
            fileToGet = clsPHRPReader.GetPHRPProteinModsFileName(eResultType, m_DatasetName)
            If Not FindAndRetrieveMiscFiles(fileToGet, False) Then
                ' Ignore this error; we don't really need this file
            Else
                m_jobParams.AddResultFileToKeep(fileToGet)
            End If

        End If

        ' Get the PHRP _syn.txt file
        fileToGet = clsPHRPReader.GetPHRPSynopsisFileName(eResultType, m_DatasetName)
        If Not String.IsNullOrEmpty(fileToGet) Then
            blnSuccess = FindAndRetrievePHRPDataFile(fileToGet, "")
            If Not blnSuccess Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
            End If
            strSynFilePath = Path.Combine(m_WorkingDir, fileToGet)
        End If

        ' Get the PHRP _fht.txt file
        fileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, m_DatasetName)
        If Not String.IsNullOrEmpty(fileToGet) Then
            blnSuccess = FindAndRetrievePHRPDataFile(fileToGet, strSynFilePath)
            If Not blnSuccess Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
            End If
        End If

        ' Get the PHRP _ResultToSeqMap.txt file
        fileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, m_DatasetName)
        If Not String.IsNullOrEmpty(fileToGet) Then
            blnSuccess = FindAndRetrievePHRPDataFile(fileToGet, strSynFilePath)
            If Not blnSuccess Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
            End If
        End If

        ' Get the PHRP _SeqToProteinMap.txt file
        fileToGet = clsPHRPReader.GetPHRPFirstHitsFileName(eResultType, m_DatasetName)
        If Not String.IsNullOrEmpty(fileToGet) Then
            blnSuccess = FindAndRetrievePHRPDataFile(fileToGet, strSynFilePath)
            If Not blnSuccess Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
            End If
        End If

        ' Get the PHRP _PepToProtMapMTS.txt file
        fileToGet = clsPHRPReader.GetPHRPPepToProteinMapFileName(eResultType, m_DatasetName)
        If Not String.IsNullOrEmpty(fileToGet) Then
            ' We're passing a dummy syn file name to FindAndRetrievePHRPDataFile 
            ' because there are a few jobs that have file _msgfplus_fht.txt (created by the November 2016 version of the DataExtractor tool)
            ' but also have file msgfdb_PepToProtMapMTS.txt (created by an older version of the MSGFPlus tool)
            blnSuccess = FindAndRetrievePHRPDataFile(fileToGet, "Dataset_msgfdb.txt")
            If Not blnSuccess Then
                If m_jobParams.GetJobParameter("IgnorePeptideToProteinMapError", False) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring missing _PepToProtMapMTS.txt file since 'IgnorePeptideToProteinMapError' = True")
                ElseIf m_jobParams.GetJobParameter("SkipProteinMods", False) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring missing _PepToProtMapMTS.txt file since 'SkipProteinMods' = True")
                Else
                    ' Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                End If
            End If
        End If

        blnSuccess = MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders)
        If Not blnSuccess Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim synFileSizeBytes As Int64 = 0
        Dim fiSynopsisFile = New FileInfo(strSynFilePath)
        If fiSynopsisFile.Exists Then
            synFileSizeBytes = fiSynopsisFile.Length
        End If

        If Not blnOnlyCopyFHTandSYNfiles Then
            ' Get the ModSummary.txt file        
            fileToGet = clsPHRPReader.GetPHRPModSummaryFileName(eResultType, m_DatasetName)
            blnSuccess = FindAndRetrievePHRPDataFile(fileToGet, strSynFilePath)
            If Not blnSuccess Then
                ' _ModSummary.txt file not found
                ' This will happen if the synopsis file is empty
                ' Try to copy the _ModDefs.txt file instead

                If synFileSizeBytes = 0 Then
                    ' If the synopsis file is 0-bytes, then the _ModSummary.txt file won't exist; that's OK
                    Dim strModDefsFile As String
                    Dim strTargetFile As String = Path.Combine(m_WorkingDir, fileToGet)

                    strModDefsFile = Path.GetFileNameWithoutExtension(m_jobParams.GetParam("ParmFileName")) &
                                     PHRP_MOD_DEFS_SUFFIX

                    If Not FindAndRetrieveMiscFiles(strModDefsFile, False) Then
                        ' Rename the file to end in _ModSummary.txt
                        m_PendingFileRenames.Add(strModDefsFile, strTargetFile)
                    Else
                        'Errors were reported in function call, so just return
                        Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                    End If
                Else
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                End If
            End If
        End If

        ' Copy the PHRP files so that the PHRPReader can determine the modified residues and extract the protein names
        ' clsMSGFResultsSummarizer also uses these files

        fileToGet = clsPHRPReader.GetPHRPResultToSeqMapFileName(eResultType, m_DatasetName)
        If Not String.IsNullOrEmpty(fileToGet) Then
            If Not FindAndRetrievePHRPDataFile(fileToGet, strSynFilePath) Then
                If synFileSizeBytes = 0 Then
                    ' If the synopsis file is 0-bytes, then the _ResultToSeqMap.txt file won't exist
                    ' That's OK; we'll create an empty file with just a header line
                    If Not CreateEmptyResultToSeqMapFile(fileToGet) Then
                        Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                    End If
                Else
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                End If
            End If

        End If

        fileToGet = clsPHRPReader.GetPHRPSeqToProteinMapFileName(eResultType, m_DatasetName)
        If Not String.IsNullOrEmpty(fileToGet) Then
            If Not FindAndRetrievePHRPDataFile(fileToGet, strSynFilePath) Then
                If synFileSizeBytes = 0 Then
                    ' If the synopsis file is 0-bytes, then the _SeqToProteinMap.txt file won't exist
                    ' That's OK; we'll create an empty file with just a header line
                    If Not CreateEmptySeqToProteinMapFile(fileToGet) Then
                        Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                    End If
                Else
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                End If
            End If
        End If

        fileToGet = clsPHRPReader.GetPHRPSeqInfoFileName(eResultType, m_DatasetName)
        If Not String.IsNullOrEmpty(fileToGet) Then
            If FindAndRetrievePHRPDataFile(fileToGet, strSynFilePath) Then

            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                     "SeqInfo file not found (" & fileToGet &
                                     "); modifications will be inferred using the ModSummary.txt file")
            End If
        End If

        If blnMGFInstrumentData Then

            Dim strFileToFind As String = m_DatasetName & DOT_MGF_EXTENSION
            If Not FindAndRetrieveMiscFiles(strFileToFind, False) Then
                m_message = "Instrument data not found: " & strFileToFind
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     "clsAnalysisResourcesMSGF.GetResources: " & m_message)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            Else
                m_jobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION)
            End If

        ElseIf Not blnOnlyCopyFHTandSYNfiles Then

            Dim strMzXMLFilePath As String = String.Empty

            ' See if a .mzXML file already exists for this dataset
            blnSuccess = RetrieveMZXmlFile(False, strMzXMLFilePath)

            ' Make sure we don't move the .mzXML file into the results folder
            m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)

            If blnSuccess Then
                ' .mzXML file found and copied locally; no need to retrieve the .Raw file
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                                         "Existing .mzXML file found: " & strMzXMLFilePath)
                End If

                ' Possibly unzip the .mzXML file
                Dim fiMzXMLFile = New FileInfo(Path.Combine(m_WorkingDir,
                                                            m_DatasetName & DOT_MZXML_EXTENSION & DOT_GZ_EXTENSION))
                If fiMzXMLFile.Exists Then
                    m_jobParams.AddResultFileExtensionToSkip(DOT_GZ_EXTENSION)

                    If Not m_IonicZipTools.GUnzipFile(fiMzXMLFile.FullName) Then
                        m_message = "Error decompressing .mzXML.gz file"
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    End If

                End If
            Else
                ' .mzXML file not found
                ' Retrieve the .Raw file so that we can make the .mzXML file prior to running MSGF
                If RetrieveSpectra(rawDataType) Then
                    m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)         ' Raw file
                    m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)       ' mzXML file
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                         "clsAnalysisResourcesMSGF.GetResources: Error occurred retrieving spectra.")
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

            End If

        End If

        blnSuccess = MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders)
        If Not blnSuccess Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        For Each entry In m_PendingFileRenames
            Dim sourceFile As New FileInfo(Path.Combine(m_WorkingDir, entry.Key))
            If sourceFile.Exists Then
                sourceFile.MoveTo(Path.Combine(m_WorkingDir, entry.Value))
            End If
        Next

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    End Function

    Private Function CreateEmptyResultToSeqMapFile(fileName As String) As Boolean
        Dim strFilePath As String

        Try
            strFilePath = Path.Combine(m_WorkingDir, fileName)
            Using swOutfile = New StreamWriter(New FileStream(strFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read))
                swOutfile.WriteLine("Result_ID" & ControlChars.Tab & "Unique_Seq_ID")
            End Using
        Catch ex As Exception
            Dim Msg As String = "Error creating empty ResultToSeqMap file: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return False
        End Try

        Return True
    End Function

    Private Function CreateEmptySeqToProteinMapFile(FileName As String) As Boolean
        Dim strFilePath As String

        Try
            strFilePath = Path.Combine(m_WorkingDir, FileName)
            Using swOutfile = New StreamWriter(New FileStream(strFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read))
                swOutfile.WriteLine(
                    "Unique_Seq_ID" & ControlChars.Tab & "Cleavage_State" & ControlChars.Tab & "Terminus_State" &
                    ControlChars.Tab & "Protein_Name" & ControlChars.Tab & "Protein_Expectation_Value_Log(e)" &
                    ControlChars.Tab & "Protein_Intensity_Log(I)")
            End Using
        Catch ex As Exception
            Dim Msg As String = "Error creating empty SeqToProteinMap file: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return False
        End Try

        Return True
    End Function

#End Region
End Class
