Option Strict On

'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 01/15/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports PRISM.Files
Imports AnalysisManagerBase
Imports AnalysisManagerBase.clsGlobal

Public MustInherit Class clsAnalysisToolRunnerMASICBase
    Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Base class for performing MASIC analysis
	'*********************************************************************************************************

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

    Protected Sub ExtractErrorsFromMASICLogFile(ByVal strLogFilePath As String)
        ' Read the most recent MASIC_Log file and look for any lines with the text "Error"
        ' Use clsLogTools.WriteLog to write these to the log

        Dim srInFile As System.IO.StreamReader
        Dim strLineIn As String
        Dim intErrorCount As Integer

        Try
            ' Fix the case of the MASIC LogFile
            Dim ioFileInfo As New System.IO.FileInfo(strLogFilePath)
            Dim strLogFileNameCorrectCase As String

            strLogFileNameCorrectCase = System.IO.Path.GetFileName(strLogFilePath)

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Checking capitalization of the the MASIC Log File: should be " & strLogFileNameCorrectCase & "; is currently " & ioFileInfo.Name)
            End If

            If ioFileInfo.Name <> strLogFileNameCorrectCase Then
                ' Need to fix the case
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Fixing capitalization of the MASIC Log File: " & strLogFileNameCorrectCase & " instead of " & ioFileInfo.Name)
                End If
                ioFileInfo.MoveTo(System.IO.Path.Combine(ioFileInfo.Directory.Name, strLogFileNameCorrectCase))
            End If

        Catch ex As Exception
            ' Ignore errors here
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error fixing capitalization of the MASIC Log File at " & strLogFilePath & ": " & ex.Message)
        End Try

        Try
            If strLogFilePath Is Nothing OrElse strLogFilePath.Length = 0 Then
                Exit Sub
            End If

            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strLogFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

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

        Dim StepResult As IJobParams.CloseOutType

        ' Reset this variable

        'Start the job timer
        m_StartTime = System.DateTime.Now
        m_message = String.Empty

        'Make the SIC's 
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Calling MASIC to create the SIC files, job " & m_JobNum)
        Try
            ' Note that RunMASIC will populate the File Path variables, then will call 
            '  StartMASICAndWait() and WaitForJobToFinish(), which are in this class
            StepResult = RunMASIC()
            If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return StepResult
            End If
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.RunTool(), Exception calling MASIC to create the SIC files, " & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        ' Update progress to 100%
        m_progress = 100
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)

        'Run the cleanup routine from the base class
        If PerfPostAnalysisTasks("SIC") <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Make the results folder
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMASICBase.RunTool(), Making results folder")
        End If

        StepResult = MakeResultsFolder()
        If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MakeResultsFolder handles posting to local log, so set database error message and exit
            m_message = "Error making results folder"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        StepResult = MoveResultFiles()
        If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MoveResultFiles moves the result files to the result folder
            m_message = "Error making results folder"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        StepResult = CopyResultsFolderToServer()
        If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return StepResult
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function StartMASICAndWait(ByVal strInputFilePath As String, ByVal strOutputFolderPath As String, ByVal strParameterFilePath As String) As IJobParams.CloseOutType
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
            If Not System.IO.File.Exists(strMASICExePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); MASIC not found at: " & strMASICExePath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Error looking for MASIC .Exe at " & strMASICExePath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        ' Call MASIC using the Program Runner class

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MASIC on file " & strInputFilePath)
        End If

        ' Define the parameters to send to Masic.exe
        CmdStr = "/I:" & strInputFilePath & " /O:" & strOutputFolderPath & " /P:" & strParameterFilePath & " /Q /SF:" & m_MASICStatusFileName

        If m_DebugLevel >= 2 Then
            ' Create a MASIC Log File
            CmdStr &= " /L"
            m_MASICLogFileName = "MASIC_Log_Job" & m_JobNum & ".txt"

            CmdStr &= ":" & System.IO.Path.Combine(m_WorkDir, m_MASICLogFileName)
        Else
            m_MASICLogFileName = String.Empty
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

        objMasicProgRunner.StartAndMonitorProgram()

        'Wait for the job to complete
        blnSuccess = WaitForJobToFinish(objMasicProgRunner)

        objMasicProgRunner = Nothing
        System.Threading.Thread.Sleep(3000)             'Delay for 3 seconds to make sure program exits

        If Not String.IsNullOrEmpty(m_MASICLogFileName) Then
            ' Read the most recent MASIC_Log file and look for any lines with the text "Error"
            ' Use clsLogTools.WriteLog to write these to the log
            ExtractErrorsFromMASICLogFile(System.IO.Path.Combine(m_WorkDir, m_MASICLogFileName))
        End If

        'Verify MASIC exited due to job completion
        If Not blnSuccess Then

            If m_DebugLevel > 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "WaitForJobToFinish returned False")
            End If

            If Not m_ErrorMessage Is Nothing AndAlso m_ErrorMessage.Length > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Masic Error message: " & m_ErrorMessage)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Masic Error message is blank")
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

    Protected Overridable Sub CalculateNewStatus(ByVal strMasicProgLoc As String)

        'Calculates status information for progress file
        'Does this by reading the MasicStatus.xml file

        Dim strPath As String

        Dim fsInFile As System.IO.FileStream = Nothing
        Dim objXmlReader As System.Xml.XmlTextReader = Nothing

        Dim strProgress As String = String.Empty

        Try
            strPath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(strMasicProgLoc), m_MASICStatusFileName)

            If System.IO.File.Exists(strPath) Then

                fsInFile = New System.IO.FileStream(strPath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite)
                objXmlReader = New System.Xml.XmlTextReader(fsInFile)
                objXmlReader.WhitespaceHandling = Xml.WhitespaceHandling.None

                While objXmlReader.Read()

                    If objXmlReader.NodeType = System.Xml.XmlNodeType.Element Then
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

    Protected Overridable Function PerfPostAnalysisTasks(ByVal ResType As String) As IJobParams.CloseOutType

        Dim StepResult As IJobParams.CloseOutType
        Dim FoundFiles() As String

        'Stop the job timer
        m_StopTime = System.DateTime.Now

        'Get rid of raw data file
        StepResult = DeleteDataFile()
        If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return StepResult
        End If

        'Zip the _SICs.XML file (if it exists; it won't if SkipSICProcessing = True in the parameter file)
        FoundFiles = System.IO.Directory.GetFiles(m_WorkDir, "*" & SICS_XML_FILE_SUFFIX)

        If FoundFiles.Length > 0 Then
            'Setup zipper

            Dim ZipFileName As String

            ZipFileName = m_Dataset & "_SICs.zip"

            If Not MyBase.ZipFile(FoundFiles(0), True, System.IO.Path.Combine(m_WorkDir, ZipFileName)) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error zipping " & System.IO.Path.GetFileName(FoundFiles(0)) & ", job " & m_JobNum)
                m_message = AppendToComment(m_message, "Error zipping " & SICS_XML_FILE_SUFFIX & " file")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        End If

        'Add all the extensions of the files to delete after run
        clsGlobal.m_FilesToDeleteExt.Add(SICS_XML_FILE_SUFFIX) 'Unzipped, concatenated DTA

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

    Protected Function WaitForJobToFinish(ByRef objMasicProgRunner As PRISM.Processes.clsProgRunner) As Boolean
        Const MINIMUM_LOG_INTERVAL_SEC As Integer = 120
        Static dtLastLogTime As DateTime
        Static sngProgressSaved As Single = -1

        Dim blnSICsXMLFileExists As Boolean

        'Wait for completion
        m_JobRunning = True

        While m_JobRunning
            System.Threading.Thread.Sleep(5000)             'Delay for 5 seconds

            If objMasicProgRunner.State = PRISM.Processes.clsProgRunner.States.NotMonitoring Or objMasicProgRunner.State = 10 Then
                m_JobRunning = False
            Else

                ' Synchronize the stored Debug level with the value stored in the database
                Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
                MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

                CalculateNewStatus(objMasicProgRunner.Program)                       'Update the status
                m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)

                If m_DebugLevel >= 3 Then
                    If System.DateTime.Now.Subtract(dtLastLogTime).TotalSeconds >= MINIMUM_LOG_INTERVAL_SEC OrElse _
                        m_progress - sngProgressSaved >= 25 Then
                        dtLastLogTime = System.DateTime.Now
                        sngProgressSaved = m_progress
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " & _
                                           "Continuing loop: " & m_ProcessStep & " (" & Math.Round(m_progress, 2).ToString & ")")
                    End If
                End If
            End If

        End While

        If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); MASIC process has ended")
        End If

        If objMasicProgRunner.State = 10 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); objMasicProgRunner.State = 10")
            Return False
        ElseIf objMasicProgRunner.ExitCode <> 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); objMasicProgRunner.ExitCode is nonzero: " & objMasicProgRunner.ExitCode)

            ' See if a _SICs.XML file was created
            If System.IO.Directory.GetFiles(m_WorkDir, "*" & SICS_XML_FILE_SUFFIX).Length > 0 Then
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
