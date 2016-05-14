'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

''' <summary>
''' Base class for performing MASIC analysis
''' </summary>
''' <remarks></remarks>
Public MustInherit Class clsAnalysisToolRunnerMASICBase
    Inherits clsAnalysisToolRunnerBase

#Region "Module variables"
    Protected Const SICS_XML_FILE_SUFFIX As String = "_SICs.xml"


    'Job running status variable
    Protected m_JobRunning As Boolean

    Protected m_ErrorMessage As String = String.Empty
    Protected m_ProcessStep As String = String.Empty
    Protected m_MASICStatusFileName As String = String.Empty
    Protected m_MASICLogFileName As String = String.Empty

#End Region

#Region "Methods"
    Public Sub New()
    End Sub

    Protected Sub ExtractErrorsFromMASICLogFile(strLogFilePath As String)
        ' Read the most recent MASIC_Log file and look for any lines with the text "Error"
        ' Use clsLogTools.WriteLog to write these to the log

        Dim srInFile As StreamReader
        Dim strLineIn As String
        Dim intErrorCount As Integer

        Try
            ' Fix the case of the MASIC LogFile
            Dim ioFileInfo As New FileInfo(strLogFilePath)
            Dim strLogFileNameCorrectCase As String

            strLogFileNameCorrectCase = Path.GetFileName(strLogFilePath)

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Checking capitalization of the the MASIC Log File: should be " & strLogFileNameCorrectCase & "; is currently " & ioFileInfo.Name)
            End If

            If ioFileInfo.Name <> strLogFileNameCorrectCase Then
                ' Need to fix the case
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Fixing capitalization of the MASIC Log File: " & strLogFileNameCorrectCase & " instead of " & ioFileInfo.Name)
                End If
                ioFileInfo.MoveTo(Path.Combine(ioFileInfo.Directory.Name, strLogFileNameCorrectCase))
            End If

        Catch ex As Exception
            ' Ignore errors here
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error fixing capitalization of the MASIC Log File at " & strLogFilePath & ": " & ex.Message)
        End Try

        Try
            If strLogFilePath Is Nothing OrElse strLogFilePath.Length = 0 Then
                Exit Sub
            End If

            srInFile = New StreamReader(New FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

            intErrorCount = 0
            Do While srInFile.Peek >= 0
                strLineIn = srInFile.ReadLine()

                If Not strLineIn Is Nothing Then
                    If strLineIn.ToLower.Contains("error") Then
                        If intErrorCount = 0 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Errors found in the MASIC Log File for job " & m_JobNum)
                        End If

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, " ... " & strLineIn)

                        intErrorCount += 1
                    End If
                End If
            Loop

            srInFile.Close()

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reading MASIC Log File at '" & strLogFilePath & "'; " & ex.Message)
        End Try

    End Sub

    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim eStepResult As IJobParams.CloseOutType

        'Call base class for initial setup
        MyBase.RunTool()

        ' Store the MASIC version info in the database
        If Not StoreToolVersionInfo() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
            m_message = "Error determining MASIC version"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Start the job timer
        m_StartTime = DateTime.UtcNow
        m_message = String.Empty

        'Make the SIC's 
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Calling MASIC to create the SIC files, job " & m_JobNum)
        Try
            ' Note that RunMASIC will populate the File Path variables, then will call 
            '  StartMASICAndWait() and WaitForJobToFinish(), which are in this class
            eStepResult = RunMASIC()
            If eStepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return eStepResult
            End If
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.RunTool(), Exception calling MASIC to create the SIC files, " & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        m_progress = 100
        UpdateStatusFile()

        'Run the cleanup routine from the base class
        If PerfPostAnalysisTasks("SIC") <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Make the results folder
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMASICBase.RunTool(), Making results folder")
        End If

        eStepResult = MakeResultsFolder()
        If eStepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MakeResultsFolder handles posting to local log, so set database error message and exit
            m_message = "Error making results folder"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        eStepResult = MoveResultFiles()
        If eStepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            m_message = "Error moving files into results folder"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        eStepResult = CopyResultsFolderToServer()
        If eStepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return eStepResult
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function StartMASICAndWait(strInputFilePath As String, strOutputFolderPath As String, strParameterFilePath As String) As IJobParams.CloseOutType
        ' Note that this function is normally called by RunMasic() in the subclass

        Dim strMASICExePath As String = String.Empty
        Dim objMasicProgRunner As PRISM.Processes.clsProgRunner
        Dim CmdStr As String
        Dim blnSuccess As Boolean

        m_ErrorMessage = String.Empty
        m_ProcessStep = "NewTask"

        Try
            m_MASICStatusFileName = "MasicStatus_" & m_MachName & ".xml"
            If m_MASICStatusFileName Is Nothing Then
                m_MASICStatusFileName = "MasicStatus.xml"
            End If
        Catch ex As Exception
            m_MASICStatusFileName = "MasicStatus.xml"
        End Try

        ' Make sure the MASIC.Exe file exists
        Try
            strMASICExePath = m_mgrParams.GetParam("masicprogloc")
            If Not File.Exists(strMASICExePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); MASIC not found at: " & strMASICExePath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Error looking for MASIC .Exe at " & strMASICExePath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        ' Call MASIC using the Program Runner class

        ' Define the parameters to send to Masic.exe
        CmdStr = "/I:" & strInputFilePath & " /O:" & strOutputFolderPath & " /P:" & strParameterFilePath & " /Q /SF:" & m_MASICStatusFileName

        If m_DebugLevel >= 2 Then
            ' Create a MASIC Log File
            CmdStr &= " /L"
            m_MASICLogFileName = "MASIC_Log_Job" & m_JobNum & ".txt"

            CmdStr &= ":" & Path.Combine(m_WorkDir, m_MASICLogFileName)
        Else
            m_MASICLogFileName = String.Empty
        End If

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMASICExePath & " " & CmdStr)
        End If

        objMasicProgRunner = New PRISM.Processes.clsProgRunner
        With objMasicProgRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = True             ' Echo the output to the console
            .WriteConsoleOutputToFile = False       ' Do not write the console output to a file

            .Name = "MASIC"
            .Program = strMASICExePath
            .Arguments = CmdStr
            .WorkDir = m_WorkDir
        End With

        ResetProgRunnerCpuUsage()

        objMasicProgRunner.StartAndMonitorProgram()

        'Wait for the job to complete
        blnSuccess = WaitForJobToFinish(objMasicProgRunner)

        objMasicProgRunner = Nothing
        Threading.Thread.Sleep(3000)                'Delay for 3 seconds to make sure program exits

        If Not String.IsNullOrEmpty(m_MASICLogFileName) Then
            ' Read the most recent MASIC_Log file and look for any lines with the text "Error"
            ' Use clsLogTools.WriteLog to write these to the log
            ExtractErrorsFromMASICLogFile(Path.Combine(m_WorkDir, m_MASICLogFileName))
        End If

        'Verify MASIC exited due to job completion
        If Not blnSuccess Then

            If m_DebugLevel > 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "WaitForJobToFinish returned False")
            End If

            If Not m_ErrorMessage Is Nothing AndAlso m_ErrorMessage.Length > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Masic Error message: " & m_ErrorMessage)
                If String.IsNullOrEmpty(m_message) Then m_message = m_ErrorMessage
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Masic Error message is blank")
                If String.IsNullOrEmpty(m_message) Then m_message = "Unknown error running MASIC"
            End If
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        Else
            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); m_ProcessStep=" & m_ProcessStep)
            End If
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If

    End Function

    Protected MustOverride Function RunMASIC() As IJobParams.CloseOutType

    Protected MustOverride Function DeleteDataFile() As IJobParams.CloseOutType

    Protected Overridable Sub CalculateNewStatus(strMasicProgLoc As String)

        'Calculates status information for progress file
        'Does this by reading the MasicStatus.xml file

        Dim strPath As String

        Dim fsInFile As FileStream = Nothing
        Dim objXmlReader As Xml.XmlTextReader = Nothing

        Dim strProgress As String = String.Empty

        Try
            strPath = Path.Combine(Path.GetDirectoryName(strMasicProgLoc), m_MASICStatusFileName)

            If File.Exists(strPath) Then

                fsInFile = New FileStream(strPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
                objXmlReader = New Xml.XmlTextReader(fsInFile)
                objXmlReader.WhitespaceHandling = Xml.WhitespaceHandling.None

                While objXmlReader.Read()

                    If objXmlReader.NodeType = Xml.XmlNodeType.Element Then
                        Select Case objXmlReader.Name
                            Case "ProcessingStep"
                                If Not objXmlReader.IsEmptyElement Then
                                    If objXmlReader.Read() Then m_ProcessStep = objXmlReader.Value
                                End If
                            Case "Progress"
                                If Not objXmlReader.IsEmptyElement Then
                                    If objXmlReader.Read() Then strProgress = objXmlReader.Value
                                End If
                            Case "Error"
                                If Not objXmlReader.IsEmptyElement Then
                                    If objXmlReader.Read() Then m_ErrorMessage = objXmlReader.Value
                                End If
                        End Select
                    End If
                End While

                If strProgress.Length > 0 Then
                    Try
                        m_progress = Single.Parse(strProgress)
                    Catch ex As Exception
                        ' Ignore errors
                    End Try
                End If

            End If

        Catch ex As Exception
            ' Ignore errors
        Finally
            If Not objXmlReader Is Nothing Then
                objXmlReader.Close()
                objXmlReader = Nothing
            End If

            If Not fsInFile Is Nothing Then
                fsInFile.Close()
                fsInFile = Nothing
            End If
        End Try

    End Sub

    Protected Overridable Function PerfPostAnalysisTasks(ResType As String) As IJobParams.CloseOutType

        Dim StepResult As IJobParams.CloseOutType
        Dim FoundFiles() As String

        'Stop the job timer
        m_StopTime = DateTime.UtcNow

        'Get rid of raw data file
        StepResult = DeleteDataFile()
        If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return StepResult
        End If

        'Zip the _SICs.XML file (if it exists; it won't if SkipSICProcessing = True in the parameter file)
        FoundFiles = Directory.GetFiles(m_WorkDir, "*" & SICS_XML_FILE_SUFFIX)

        If FoundFiles.Length > 0 Then
            'Setup zipper

            Dim ZipFileName As String

            ZipFileName = m_Dataset & "_SICs.zip"

            If Not MyBase.ZipFile(FoundFiles(0), True, Path.Combine(m_WorkDir, ZipFileName)) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error zipping " & Path.GetFileName(FoundFiles(0)) & ", job " & m_JobNum)
                m_message = clsGlobal.AppendToComment(m_message, "Error zipping " & SICS_XML_FILE_SUFFIX & " file")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        End If

        'Add all the extensions of the files to delete after run
        m_jobParams.AddResultFileExtensionToSkip(SICS_XML_FILE_SUFFIX) 'Unzipped, concatenated DTA

        'Add the current job data to the summary file
        Try
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim strMASICExePath As String = m_mgrParams.GetParam("masicprogloc")
        Dim blnSuccess As Boolean

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Lookup the version of MASIC
        blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strMASICExePath)
        If Not blnSuccess Then Return False

        ' Store path to MASIC.exe in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(New FileInfo(strMASICExePath))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Validate that required options are defined in the MASIC parameter file
    ''' </summary>
    ''' <param name="strParameterFilePath"></param>
    ''' <remarks></remarks>
    Protected Function ValidateParameterFile(strParameterFilePath As String) As Boolean

        If String.IsNullOrWhiteSpace(strParameterFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "The MASIC Parameter File path is empty; nothing to validate")
            Return True
        End If

        Dim objSettingsFile = New PRISM.Files.XmlSettingsFileAccessor()

        If Not objSettingsFile.LoadSettings(strParameterFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error loading parameter file " & strParameterFilePath)
            Return False
        End If

        If Not objSettingsFile.SectionPresent("MasicExportOptions") Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MasicExportOptions section not found in " & strParameterFilePath)
            objSettingsFile.SetParam("MasicExportOptions", "IncludeHeaders", "True")
            objSettingsFile.SaveSettings()
            Return True
        End If

        Dim includeHeaders = objSettingsFile.GetParam("MasicExportOptions", "IncludeHeaders", False)

        If Not includeHeaders Then
            ' File needs to be updated
            objSettingsFile.SetParam("MasicExportOptions", "IncludeHeaders", "True")
            objSettingsFile.SaveSettings()
        End If

        Return True

    End Function

    Protected Function WaitForJobToFinish(objMasicProgRunner As PRISM.Processes.clsProgRunner) As Boolean
        Const MAX_RUNTIME_HOURS = 12
        Const SECONDS_BETWEEN_UPDATE = 15

        Dim blnSICsXMLFileExists As Boolean
        Dim dtStartTime As DateTime = DateTime.UtcNow
        Dim blnAbortedProgram = False

        'Wait for completion
        m_JobRunning = True

        Dim dtLastUpdate = DateTime.UtcNow

        While m_JobRunning

            ' Wait for 15 seconds
            While DateTime.UtcNow.Subtract(dtLastUpdate).TotalSeconds < SECONDS_BETWEEN_UPDATE
                Threading.Thread.Sleep(250)
            End While
            dtLastUpdate = DateTime.UtcNow

            If objMasicProgRunner.State = PRISM.Processes.clsProgRunner.States.NotMonitoring Then
                m_JobRunning = False
            Else
                ' Update the status
                CalculateNewStatus(objMasicProgRunner.Program)
                UpdateStatusFile()

                ' Note that the call to GetCoreUsage() will take at least 1 second
                Dim coreUsage = objMasicProgRunner.GetCoreUsage()

                UpdateProgRunnerCpuUsage(objMasicProgRunner.PID, coreUsage, SECONDS_BETWEEN_UPDATE)

                LogProgress("MASIC")
            End If

            If DateTime.UtcNow.Subtract(dtStartTime).TotalHours >= MAX_RUNTIME_HOURS Then
                ' Abort processing
                objMasicProgRunner.StopMonitoringProgram(Kill:=True)
                blnAbortedProgram = True
            End If
        End While

        If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); MASIC process has ended")
        End If

        If blnAbortedProgram Then
            m_ErrorMessage = "Aborted MASIC processing since over " & MAX_RUNTIME_HOURS & " hours have elapsed"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " & m_ErrorMessage)
            Return False
        ElseIf objMasicProgRunner.State = 10 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); objMasicProgRunner.State = 10")
            Return False
        ElseIf objMasicProgRunner.ExitCode <> 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); objMasicProgRunner.ExitCode is nonzero: " & objMasicProgRunner.ExitCode)

            ' See if a _SICs.XML file was created
            If Directory.GetFiles(m_WorkDir, "*" & SICS_XML_FILE_SUFFIX).Length > 0 Then
                blnSICsXMLFileExists = True
            End If

            If objMasicProgRunner.ExitCode = 32 Then
                ' FindSICPeaksError
                ' As long as the _SICs.xml file was created, we can safely ignore this error
                If blnSICsXMLFileExists Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " & SICS_XML_FILE_SUFFIX & " file found, so ignoring non-zero exit code")
                    Return True
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " & SICS_XML_FILE_SUFFIX & " file not found")
                    Return False
                End If
            Else
                ' Return False for any other exit codes
                Return False
            End If
        Else
            Return True
        End If

    End Function

#End Region

    Protected Overrides Sub Finalize()
        MyBase.Finalize()
    End Sub
End Class
