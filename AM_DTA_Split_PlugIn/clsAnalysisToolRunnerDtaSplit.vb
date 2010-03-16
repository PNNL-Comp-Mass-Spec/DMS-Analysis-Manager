'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 01/29/2009
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports System.io
Imports System.Text.RegularExpressions
Imports System.Collections.Generic

Public Class clsAnalysisToolRunnerDtaSplit
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running DTA splitter
    '*********************************************************************************************************

#Region "Module Variables"

    Protected r_FileSeparator As Regex
    Protected r_DTAFirstLine As Regex       ' Presently not used
#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New()
        Me.r_FileSeparator = New Regex("^\s*[=]{5,}\s+\""(?<rootname>.+)\.(?<startscan>\d+)\." + _
            "(?<endscan>\d+)\.(?<chargestate>\d+)\.(?<filetype>.+)\""\s+[=]{5,}\s*$", _
            RegexOptions.CultureInvariant _
            Or RegexOptions.Compiled)

        ' Presently not used
        Me.r_DTAFirstLine = New Regex( _
              "^\s*(?<parentmass>\d+\.\d+)\s+\d+\s+scan\=(?<scannum>\d+)\s+" + _
              "cs\=(?<chargestate>\d+)$", _
            RegexOptions.CultureInvariant _
            Or RegexOptions.Compiled)

    End Sub

    ''' <summary>
    ''' Initializes class
    ''' </summary>
    ''' <param name="mgrParams">Object containing manager parameters</param>
    ''' <param name="jobParams">Object containing job parameters</param>
    ''' <param name="StatusTools">Object for updating status file as job progresses</param>
    ''' <remarks></remarks>
    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
      ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDtaSplit.Setup()")
        End If
    End Sub

    ''' <summary>
    ''' Runs InSpecT tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType
        Dim strCattedFile As String
        Dim intSegmentCountToCreate As Integer
        Dim blnSplitToEqualScanCounts As Boolean

        Try
            strCattedFile = Path.Combine(m_mgrParams.GetParam("WorkDir"), m_jobParams.GetParam("DatasetNum") & "_dta.txt")

            Try
                intSegmentCountToCreate = clsGlobal.GetJobParameter(m_jobParams, "NumberOfClonedSteps", 0)
                If intSegmentCountToCreate = 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Setting 'NumberOfClonedSteps' not found in the job parameters; will assume NumberOfClonedSteps=4")
                    intSegmentCountToCreate = 4
                End If
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Setting 'NumberOfClonedSteps' is not numeric in the job parameters; will assume NumberOfClonedSteps=4")
                intSegmentCountToCreate = 4
            End Try

            Try
                blnSplitToEqualScanCounts = clsGlobal.GetJobParameter(m_jobParams, "ClonedStepsHaveEqualNumSpectra", True)
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Setting 'ClonedStepsHaveEqualNumSpectra' is not valid in the job parameters; will assume True")
                blnSplitToEqualScanCounts = True
            End Try

            'Start the job timer
            m_StartTime = System.DateTime.Now

            result = SplitCattedDtaFileIntoSegments(strCattedFile, intSegmentCountToCreate, blnSplitToEqualScanCounts)

            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

            'Stop the job timer
            m_StopTime = System.DateTime.Now

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, 100, intSegmentCountToCreate, "", "", "", False)

            result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return result
            End If

            result = MoveResultFiles()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return result
            End If

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return result
            End If

            If Not clsGlobal.RemoveNonResultFiles(m_mgrParams.GetParam("workdir"), m_DebugLevel) Then
                m_message = AppendToComment(m_message, "Error deleting non-result files")
                'TODO: Figure out what to do here
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            m_message = "Error in DtaSplitPlugin->RunTool: " & ex.Message
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

    End Function

    ''' <summary>
    ''' Split the dta txt file into multiple files
    ''' </summary>
    ''' <param name="strSourceFilePath">Input data file path</param>
    ''' <param name="intSegmentCountToCreate">Number of segments to create</param>
    ''' <param name="blnSplitToEqualScanCounts">When True, then the split files will each have the same number of spectra; when false, then the split files will each have a similar number of bytes</param>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function SplitCattedDtaFileIntoSegments(ByVal strSourceFilePath As String, _
                                                    ByVal intSegmentCountToCreate As Integer, _
                                                    ByVal blnSplitToEqualScanCounts As Boolean) As IJobParams.CloseOutType

        Const STATUS_UPDATE_INTERVAL_SECONDS As Single = 15

        Dim srInFile As System.IO.StreamReader = Nothing
        Dim strLineIn As String
        Dim splitMatch As Match = Nothing
        Dim fileNameCounter As Integer

        Dim lngTargetBytesPerSegment As Long
        Dim lngNextTargetByteThreshold As Long
        Dim lngBytesRead As Long

        Dim intTargetSpectraPerSegment As Integer
        Dim intSpectraCountReadCurrentSegment As Integer
        Dim intSpectraCountRead As Integer
        Dim intSpectraCountExpected As Integer

        Dim fi As System.IO.FileInfo
        Dim lineEndCharCount As Integer
        Dim swOutFile As System.IO.StreamWriter
        Dim strSegmentDescription As String

        Dim sngPercentComplete As Single
        Dim dtLastStatusUpdate As System.DateTime = System.DateTime.Now

        Try
            If intSegmentCountToCreate < 1 Then intSegmentCountToCreate = 1

            If blnSplitToEqualScanCounts AndAlso intSegmentCountToCreate > 1 Then
                ' Need to prescan the file to count the number of spectra in it
                intSpectraCountExpected = CountSpectraInCattedDtaFile(strSourceFilePath)

                If intSpectraCountExpected = 0 Then
                    blnSplitToEqualScanCounts = False
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "CountSpectraInCattedDtaFile returned a spectrum count of 0; will instead split the files to an approximately equal byte length")
                End If
            End If

            fi = New System.IO.FileInfo(strSourceFilePath)

            If intSegmentCountToCreate = 1 Then
                ' Nothing to do except create a file named Dataset_1_dta.txt
                ' Simply rename the input file

                Try
                    Dim strDestFileName As String
                    strDestFileName = GetNewSplitDTAFileName(1)

                    fi.MoveTo(strDestFileName)

                Catch ex As Exception
                    If strSourceFilePath Is Nothing Then strSourceFilePath = "??"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in SplitCattedDtaFileIntoSegments renaming file: " & strSourceFilePath & " to _1_dta.txt; " & ex.Message)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End Try

                Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            End If


            lineEndCharCount = LineEndCharacterCount(fi)

            intTargetSpectraPerSegment = CInt(Math.Ceiling(intSpectraCountExpected / CDbl(intSegmentCountToCreate)))
            If intTargetSpectraPerSegment < 1 Then intTargetSpectraPerSegment = 1
            intSpectraCountReadCurrentSegment = 0
            intSpectraCountRead = 0

            lngTargetBytesPerSegment = CLng(Math.Ceiling(fi.Length / CDbl(intSegmentCountToCreate)))
            lngNextTargetByteThreshold = lngTargetBytesPerSegment
            lngBytesRead = 0

            If m_DebugLevel >= 1 Then
                If blnSplitToEqualScanCounts Then
                    strSegmentDescription = "spectra per segment = " & intTargetSpectraPerSegment
                Else
                    strSegmentDescription = "bytes per segment = " & lngTargetBytesPerSegment
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Splitting " & System.IO.Path.GetFileName(strSourceFilePath) & " into " & intSegmentCountToCreate & " segments; " & strSegmentDescription)
            End If

            ' Create the first output file
            fileNameCounter = 1
            swOutFile = CreateNewSplitDTAFile(fileNameCounter)
            If swOutFile Is Nothing Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

            ' Open the input file
            srInFile = New System.IO.StreamReader(strSourceFilePath)

            Do While srInFile.Peek() >= 0
                strLineIn = srInFile.ReadLine

                ' Increment the bytes read counter
                lngBytesRead += strLineIn.Length + lineEndCharCount

                If fileNameCounter < intSegmentCountToCreate Then
                    If blnSplitToEqualScanCounts Then

                        ' Look for the spectrum separator line
                        splitMatch = Me.r_FileSeparator.Match(strLineIn)
                        If splitMatch.Success Then
                            intSpectraCountReadCurrentSegment += 1
                            intSpectraCountRead += 1

                            If intSpectraCountReadCurrentSegment > intTargetSpectraPerSegment Then
                                If m_DebugLevel >= 3 Then
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Segment " & fileNameCounter.ToString & " has " & (intSpectraCountReadCurrentSegment - 1).ToString & " spectra; creating next segment; bytes read = " & lngBytesRead.ToString)
                                End If

                                ' Number of desired spectra have been written to this segment
                                ' Close the current file and create the next segment
                                CreateNextSegment(swOutFile, fileNameCounter)
                                If swOutFile Is Nothing Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

                                intSpectraCountReadCurrentSegment = 1
                            End If
                        End If

                    Else

                        If lngBytesRead >= lngNextTargetByteThreshold Then
                            ' We have passed the target byte threshold
                            ' Now start looking for the spectrum separator line
                            splitMatch = Me.r_FileSeparator.Match(strLineIn)
                            If splitMatch.Success Then
                                If m_DebugLevel >= 3 Then
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Target byte threshold reached: " & lngBytesRead.ToString & " bytes read vs. threshold of " & lngNextTargetByteThreshold.ToString & " bytes")
                                End If

                                intSpectraCountReadCurrentSegment += 1
                                intSpectraCountRead += 1

                                ' File separator found; close the current file and create the next segment
                                CreateNextSegment(swOutFile, fileNameCounter)
                                If swOutFile Is Nothing Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

                                ' Increment lngNextTargetByteThreshold
                                lngNextTargetByteThreshold += lngTargetBytesPerSegment
                            End If
                        End If

                    End If
                End If

                If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= STATUS_UPDATE_INTERVAL_SECONDS Then
                    dtLastStatusUpdate = System.DateTime.Now
                    sngPercentComplete = (lngBytesRead / CSng(srInFile.BaseStream.Length) * 100)
                    m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, fileNameCounter, "", "", "", False)
                End If

                swOutFile.WriteLine(strLineIn)
            Loop

            'close the main dta file and the last split dta file
            swOutFile.Close()
            srInFile.Close()

        Catch ex As Exception
            If strSourceFilePath Is Nothing Then strSourceFilePath = "??"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in SplitCattedDtaFileIntoSegments reading file: " & strSourceFilePath & "; Current segment = " & fileNameCounter.ToString & "; " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Closes the current segment and then creates the next one
    ''' </summary>
    ''' <param name="swOutFile"></param>
    ''' <param name="fileNameCounter"></param>
    ''' <remarks></remarks>
    Private Sub CreateNextSegment(ByRef swOutFile As System.IO.StreamWriter, ByRef fileNameCounter As Integer)

        swOutFile.Close()
        swOutFile.Dispose()

        ' Increment the file counter, then create the next file
        fileNameCounter += 1
        swOutFile = CreateNewSplitDTAFile(fileNameCounter)

    End Sub

    ''' <summary>
    ''' Counts the number of spectra in the input concatenated DTA file (_dta.txt file)
    ''' </summary>
    ''' <param name="strSourceFilePath"></param>
    ''' <returns>The number of spectra found (i.e. the number of header lines found); returns 0 if any problems</returns>
    ''' <remarks></remarks>
    Private Function CountSpectraInCattedDtaFile(ByVal strSourceFilePath As String) As Integer

        Dim srInFile As System.IO.StreamReader = Nothing
        Dim strLineIn As String
        Dim splitMatch As Match = Nothing

        Dim intSpectraCount As Integer
        Dim fi As System.IO.FileInfo

        Try
            intSpectraCount = 0

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Counting the number of spectra in the source _Dta.txt file: " & System.IO.Path.GetFileName(strSourceFilePath))
            End If

            fi = New System.IO.FileInfo(strSourceFilePath)

            ' Open the input file
            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

            Do While srInFile.Peek() >= 0
                strLineIn = srInFile.ReadLine

                splitMatch = Me.r_FileSeparator.Match(strLineIn)
                If splitMatch.Success Then
                    intSpectraCount += 1
                End If
            Loop

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Spectrum count in source _Dta.txt file: " & intSpectraCount)
            End If

            ' Close the input file
            srInFile.Close()

        Catch ex As Exception
            If strSourceFilePath Is Nothing Then strSourceFilePath = "??"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error counting the number of spectra in '" & strSourceFilePath & "'; " & ex.Message)
            intSpectraCount = 0
        End Try

        Return intSpectraCount

    End Function

    Private Function CreateNewSplitDTAFile(ByVal fileNameCounter As Integer) As System.IO.StreamWriter
        Dim strFileName As String = String.Empty
        Dim strFilePath As String
        Dim swOutFile As System.IO.StreamWriter = Nothing

        Try
            strFilePath = GetNewSplitDTAFileName(fileNameCounter)

            strFileName = System.IO.Path.GetFileName(strFilePath)

            If System.IO.File.Exists(strFilePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Warning: Split DTA file already exists " & strFilePath)
            End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating split DTA file " & strFileName)
            End If

            swOutFile = New System.IO.StreamWriter(strFilePath, False)

        Catch ex As Exception
            If strFileName Is Nothing Then strFileName = "??"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CreateNewSplitDTAFile creating file: " & strFileName & "; " & ex.Message)
        End Try

        Return swOutFile

    End Function

    Private Function GetNewSplitDTAFileName(ByVal fileNameCounter As Integer) As String
        Dim strFileName As String
        Dim strFilePath As String

        strFileName = m_jobParams.GetParam("DatasetNum") + "_" + CStr(fileNameCounter) + "_dta.txt"
        m_ExceptionFiles.Add(strFileName)

        strFilePath = System.IO.Path.Combine(m_mgrParams.GetParam("WorkDir"), strFileName)

        Return strFilePath

    End Function

    ''' <summary>
    ''' This function reads the input file one byte at a time, looking for the first occurence of Chr(10) or Chr(13) (aka vbCR or VBLF)
    ''' When found, the next byte is examined
    ''' If the next byte is also Chr(10) or Chr(13), then the line terminator is assumed to be 2 bytes; if not found, then it is assumed to be one byte
    ''' </summary>
    ''' <param name="fi"></param>
    ''' <returns>1 if a one-byte line terminator; 2 if a two-byte line terminator</returns>
    ''' <remarks></remarks>
    Private Function LineEndCharacterCount(ByVal fi As System.IO.FileInfo) As Integer
        Dim tr As System.IO.TextReader
        Dim testcode As Integer
        Dim testcode2 As Integer
        Dim counter As Long
        Dim endCount As Integer = 1         ' Initially assume a one-byte line terminator

        If (fi.Exists) Then
            tr = fi.OpenText
            For counter = 1 To fi.Length
                testcode = tr.Read()
                If testcode = 10 Or testcode = 13 Then
                    testcode2 = tr.Read()
                    If testcode2 = 10 Or testcode2 = 13 Then
                        endCount = 2
                        Exit For
                    Else
                        endCount = 1
                        Exit For
                    End If
                End If
            Next

            tr.Close()
        End If

        tr = Nothing
        Return endCount

    End Function

#End Region

End Class
