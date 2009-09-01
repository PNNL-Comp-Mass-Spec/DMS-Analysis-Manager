'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.Xml
Imports System.IO
Imports AnalysisManagerBase.clsGlobal

Namespace AnalysisManagerBase

	Public Class clsAnalysisToolRunnerBase
		Implements IToolRunner

		'*********************************************************************************************************
		'Base class for analysis tool runner
		'*********************************************************************************************************

#Region "Module variables"
		'status tools
		Protected m_StatusTools As IStatusFile

		' access to the job parameters
		Protected m_jobParams As IJobParams

        ' access to mgr parameters
		Protected m_mgrParams As IMgrParams

		' access to settings file parameters
		Protected m_settingsFileParams As New PRISM.Files.XmlSettingsFileAccessor

        ' progress of run (in percent); This is a value between 0 and 100
		Protected m_progress As Single = 0

		'	status code
        Protected m_status As IStatusFile.EnumMgrStatus

		'DTA count for status report
		Protected m_DtaCount As Integer = 0

		' for posting a general explanation for external consumption
        Protected m_message As String = String.Empty

		'Debug level
		Protected m_DebugLevel As Short

		'Working directory, machine name, & job number (used frequently by subclasses)
		Protected m_WorkDir As String
		Protected m_MachName As String
		Protected m_JobNum As String

		'Elapsed time information
		Protected m_StartTime As Date
		Protected m_StopTime As Date

		'Results folder name
		Protected m_ResFolderName As String

		'DLL file info
		Protected m_FileVersion As String
		Protected m_FileDate As String

		Protected m_ResourcerDataFileList() As String
#End Region

#Region "Properties"
		'Publicly accessible results folder name and path
		Public ReadOnly Property ResFolderName() As String Implements IToolRunner.ResFolderName
			Get
				Return m_ResFolderName
			End Get
		End Property

		' explanation of what happened to last operation this class performed
		Public ReadOnly Property Message() As String Implements IToolRunner.Message
			Get
				Return m_message
			End Get
		End Property

		' the state of completion of the job (as a percentage)
		Public ReadOnly Property Progress() As Single Implements IToolRunner.Progress
			Get
				Return m_progress
			End Get
		End Property
#End Region

#Region "Methods"
		''' <summary>
		''' Constructor
		''' </summary>
		''' <remarks>Does nothing at present</remarks>
		Public Sub New()
		End Sub

		''' <summary>
		''' Initializes class
		''' </summary>
		''' <param name="mgrParams">Object holding manager parameters</param>
		''' <param name="jobParams">Object holding job parameters</param>
        ''' <param name="StatusTools">Object for status reporting</param>
		''' <remarks></remarks>
        Public Overridable Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
          ByVal StatusTools As IStatusFile) Implements IToolRunner.Setup

            m_mgrParams = mgrParams
            m_jobParams = jobParams
            m_StatusTools = StatusTools
            m_WorkDir = m_mgrParams.GetParam("workdir")
            m_MachName = m_mgrParams.GetParam("MgrName")
            m_JobNum = m_jobParams.GetParam("Job")
            m_DebugLevel = CShort(m_mgrParams.GetParam("debuglevel"))
            m_StatusTools.Tool = m_jobParams.GetCurrentJobToolDescription()

            m_ResFolderName = m_jobParams.GetParam("OutputFolderName")

            If m_DebugLevel > 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.Setup()")
            End If

        End Sub

		''' <summary>
		''' Loads the job settings file
		''' </summary>
		''' <returns>TRUE for success, FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Function LoadSettingsFile() As Boolean
			Dim fileName As String = m_jobParams.GetParam("settingsFileName")
			If fileName <> "na" Then
				Dim filePath As String = System.IO.Path.Combine(m_WorkDir, fileName)
				If File.Exists(filePath) Then			 'XML tool Loadsettings returns True even if file is not found, so separate check reqd
					Return m_settingsFileParams.LoadSettings(filePath)
				Else
					Return False			'Settings file wasn't found
				End If
			Else
				Return True		  'Settings file wasn't required
			End If

		End Function

		''' <summary>
		''' Runs the analysis tool
		''' </summary>
		''' <returns>CloseoutType enum representing completion status</returns>
		''' <remarks></remarks>
		Public Overridable Function RunTool() As IJobParams.CloseOutType Implements IToolRunner.RunTool

            ' Synchronize the stored Debug level with the value stored in the database
            GetCurrentMgrSettingsFromDB()

			'Runs the job. Major work is performed by overrides

            'Make log entry (both locally and in the DB)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_MachName & ": Starting analysis, job " & m_JobNum)
            'Start the job timer
            m_StartTime = System.DateTime.Now()

			'Remainder of method is supplied by subclasses

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		End Function

        ''' <summary>
        ''' Looks up the current debug level for the manager.  If the call to the server fails, m_DebugLevel will be left unchanged
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Function GetCurrentMgrSettingsFromDB() As Boolean
            GetCurrentMgrSettingsFromDB(0)
        End Function

        ''' <summary>
        ''' Looks up the current debug level for the manager.  If the call to the server fails, m_DebugLevel will be left unchanged
        ''' </summary>
        ''' <param name="intUpdateIntervalSeconds">The minimum number of seconds between updates; if fewer than intUpdateIntervalSeconds seconds have elapsed since the last call to this function, then no update will occur</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Function GetCurrentMgrSettingsFromDB(ByVal intUpdateIntervalSeconds As Integer) As Boolean
            GetCurrentMgrSettingsFromDB(intUpdateIntervalSeconds, m_mgrParams, m_DebugLevel)
        End Function

        ''' <summary>
        ''' Looks up the current debug level for the manager.  If the call to the server fails, DebugLevel will be left unchanged
        ''' </summary>
        ''' <param name="DebugLevel">Input/Output parameter: set to the current debug level, will be updated to the debug level in the manager control DB</param>
        ''' <returns>True for success; False for error</returns>
        ''' <remarks></remarks>
        Public Shared Function GetCurrentMgrSettingsFromDB(ByVal intUpdateIntervalSeconds As Integer, _
                                                           ByRef objMgrParams As IMgrParams, _
                                                           ByRef DebugLevel As Short) As Boolean

            Dim MyConnection As System.Data.SqlClient.SqlConnection
            Dim MyCmd As New System.Data.SqlClient.SqlCommand
            Dim drSqlReader As System.Data.SqlClient.SqlDataReader
            Dim ConnectionString As String

            Dim strParamName As String
            Dim strParamValue As String
            Dim intValueCountRead As Integer = 0

            Dim intNewDebugLevel As Short

            Static dtLastUpdateTime As System.DateTime

            Try

                If intUpdateIntervalSeconds > 0 AndAlso System.DateTime.Now.Subtract(dtLastUpdateTime).TotalSeconds < intUpdateIntervalSeconds Then
                    Return True
                End If
                dtLastUpdateTime = System.DateTime.Now

                If DebugLevel >= 5 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updating manager settings from the Manager Control DB")
                End If

                ConnectionString = objMgrParams.GetParam("MgrCnfgDbConnectStr")
                MyConnection = New System.Data.SqlClient.SqlConnection(ConnectionString)
                MyConnection.Open()

                'Set up the command object prior to SP execution
                With MyCmd
                    .CommandType = CommandType.Text
                    .CommandText = "SELECT ParameterName, ParameterValue FROM V_MgrParams " & _
                                   "WHERE ManagerName = '" & objMgrParams.GetParam("MgrName") & "' AND " & _
                                        " ParameterName IN ('debuglevel')"

                    .Connection = MyConnection
                End With

                'Execute the SP
                drSqlReader = MyCmd.ExecuteReader(CommandBehavior.CloseConnection)

                While drSqlReader.Read
                    strParamName = drSqlReader.GetString(0)
                    strParamValue = drSqlReader.GetString(1)

                    If Not strParamName Is Nothing And Not strParamValue Is Nothing Then
                        Select Case strParamName
                            Case "debuglevel"
                                intNewDebugLevel = Short.Parse(strParamValue)

                                If DebugLevel > 0 AndAlso intNewDebugLevel <> DebugLevel Then
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Debug level changed from " & DebugLevel.ToString & " to " & intNewDebugLevel.ToString)
                                    DebugLevel = intNewDebugLevel
                                End If
                                intValueCountRead += 1
                            Case Else
                                ' Unknown parameter
                        End Select
                    End If
                End While

                drSqlReader.Close()

            Catch ex As System.Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception getting current manager settings from the manager control DB" & ex.Message)
            End Try

            If intValueCountRead > 0 Then
                Return True
            Else
                Return False
            End If

        End Function

        ''' <summary>
        ''' Creates a results folder after analysis complete
        ''' </summary>
        ''' <returns>CloseOutType enum indicating success or failure</returns>
        ''' <remarks></remarks>
        Protected Overridable Function MakeResultsFolder() As IJobParams.CloseOutType

            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.PACKAGING_RESULTS, 0)

            'Makes results folder and moves files into it
            Dim ResFolderNamePath As String

            'Log status (both locally and in the DB)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_MachName & ": Creating results folder, Job " & m_JobNum)
            ResFolderNamePath = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)

            'make the results folder
            Try
                Directory.CreateDirectory(ResFolderNamePath)
            Catch Err As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error making results folder, job " & m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(Err))
                m_message = AppendToComment(m_message, "Error making results folder")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try

        End Function

        ''' <summary>
        ''' Moves result files after tool has completed
        ''' </summary>
        ''' <returns>CloseOutType enum indicating success or failure</returns>
        ''' <remarks></remarks>
        Protected Overridable Function MoveResultFiles() As IJobParams.CloseOutType
            Const REJECT_LOGGING_THRESHOLD As Integer = 10
            Const ACCEPT_LOGGING_THRESHOLD As Integer = 50
            Const LOG_LEVEL_REPORT_ACCEPT_OR_REJECT As Integer = 5

            'Makes results folder and moves files into it
            Dim ResFolderNamePath As String = String.Empty
            Dim strTargetFilePath As String = String.Empty

            Dim Files() As String
            Dim TmpFile As String = String.Empty
            Dim TmpFileNameLcase As String = String.Empty
            Dim OkToMove As Boolean = False
            Dim ext As String
            Dim excpt As String
            Dim intIndex As Integer
            Dim strLogMessage As String

            Dim strExtension As String
            Dim htRejectStats As System.Collections.Hashtable
            Dim htAcceptStats As System.Collections.Hashtable
            Dim objExtension As IDictionaryEnumerator

            Dim blnErrorEncountered As Boolean = False

            'Move files into results folder
            Try
                m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.PACKAGING_RESULTS, 0)
                ResFolderNamePath = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
                htRejectStats = New System.Collections.Hashtable
                htAcceptStats = New System.Collections.Hashtable

                'Log status
                If m_DebugLevel >= 2 Then
                    strLogMessage = "Move Result Files to " & ResFolderNamePath
                    If m_DebugLevel >= 3 Then
                        strLogMessage &= "; FilesToDelete contains " & FilesToDelete.Count.ToString & " entries" & _
                                         "; m_FilesToDeleteExt contains " & m_FilesToDeleteExt.Count.ToString & " entries" & _
                                         "; m_ExceptionFiles contains " & m_ExceptionFiles.Count.ToString & " entries"
                    End If
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strLogMessage)
                End If


                ' Make sure the entries in FilesToDelete are all lowercase
                For intIndex = 0 To clsGlobal.FilesToDelete.Count - 1
                    clsGlobal.FilesToDelete(intIndex) = clsGlobal.FilesToDelete(intIndex).ToLower
                Next intIndex

                ' Make sure FilesToDelete is sorted
                FilesToDelete.Sort()

                ' Make sure the entries in clsGlobal.m_FilesToDeleteExt are all lowercase
                For intIndex = 0 To clsGlobal.m_FilesToDeleteExt.Count - 1
                    clsGlobal.m_FilesToDeleteExt(intIndex) = clsGlobal.m_FilesToDeleteExt(intIndex).ToLower
                Next intIndex

                ' Make sure the entries in  clsGlobal.m_ExceptionFiles are all lowercase
                For intIndex = 0 To clsGlobal.m_ExceptionFiles.Count - 1
                    clsGlobal.m_ExceptionFiles(intIndex) = clsGlobal.m_ExceptionFiles(intIndex).ToLower
                Next intIndex


                ' Obtain a list of all files in the working directory
                Files = Directory.GetFiles(m_WorkDir, "*.*")

                ' Check each file against clsGlobal.m_FilesToDeleteExt and clsGlobal.m_ExceptionFiles
                For Each TmpFile In Files
                    OkToMove = True
                    TmpFileNameLcase = System.IO.Path.GetFileName(TmpFile).ToLower

                    ' Check to see if the filename is defined in FilesToDelete
                    ' Note that entries in FilesToDelete are already lowercase (thanks to a for loop earlier in this function)
                    If FilesToDelete.BinarySearch(TmpFileNameLcase) >= 0 Then
                        ' File found in the FilesToDelete list; do not move it
                        OkToMove = False
                    End If

                    If OkToMove Then
                        'Check to see if the file ends with an entry specified in m_FilesToDeleteExt
                        ' Note that entries in m_FilesToDeleteExt are already lowercase (thanks to a for loop earlier in this function)
                        For Each ext In clsGlobal.m_FilesToDeleteExt
                            If TmpFileNameLcase.EndsWith(ext) Then
                                OkToMove = False
                                Exit For
                            End If
                        Next
                    End If

                    If Not OkToMove Then
                        ' Check to see if the file is a result file that got captured as a non result file
                        ' Note that entries in m_ExceptionFiles are already lowercase (thanks to a for loop earlier in this function)
                        For Each excpt In clsGlobal.m_ExceptionFiles
                            If TmpFileNameLcase.Contains(excpt) Then
                                OkToMove = True
                                Exit For
                            End If
                        Next
                    End If

                    ' Look for invalid characters in the filename
                    '	(Required because extract_msn.exe sometimes leaves files with names like "C3 90 68 C2" (ascii codes) in working directory) 
                    ' Note: now evaluating each character in the filename
                    If OkToMove Then
                        Dim intAscValue As Integer
                        For Each chChar As Char In System.IO.Path.GetFileName(TmpFile).ToCharArray
                            intAscValue = System.Convert.ToInt32(chChar)
                            If intAscValue <= 31 Or intAscValue >= 128 Then
                                ' Invalid character found
                                OkToMove = False
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted file:  " & TmpFile)
                                Exit For
                            End If
                        Next
                    Else
                        If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
                            strExtension = System.IO.Path.GetExtension(TmpFile)
                            If htRejectStats.Contains(strExtension) Then
                                htRejectStats(strExtension) = CInt(htRejectStats(strExtension)) + 1
                            Else
                                htRejectStats.Add(strExtension, 1)
                            End If

                            ' Only log the first 10 times files of a given extension are rejected
                            '  However, if a file was rejected due to invalid characters in the name, then we don't track that rejection with htRejectStats
                            If CInt(htRejectStats(strExtension)) <= REJECT_LOGGING_THRESHOLD Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Rejected file:  " & TmpFile)
                            End If
                        End If
                    End If

                    'If valid file name, then move file to results folder
                    If OkToMove Then
                        If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
                            strExtension = System.IO.Path.GetExtension(TmpFile).ToLower
                            If htAcceptStats.Contains(strExtension) Then
                                htAcceptStats(strExtension) = CInt(htAcceptStats(strExtension)) + 1
                            Else
                                htAcceptStats.Add(strExtension, 1)
                            End If

                            ' Only log the first 50 times files of a given extension are accepted
                            If CInt(htAcceptStats(strExtension)) <= ACCEPT_LOGGING_THRESHOLD Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted file:  " & TmpFile)
                            End If
                        End If

                        Try
                            strTargetFilePath = System.IO.Path.Combine(ResFolderNamePath, System.IO.Path.GetFileName(TmpFile))
                            System.IO.File.Move(TmpFile, strTargetFilePath)
                        Catch ex As Exception
                            Try
                                ' Move failed
                                ' Attempt to copy the file instead of moving the file
                                System.IO.File.Copy(TmpFile, strTargetFilePath, True)
                                ' If we get here, then the copy succeeded; the original file (in the work folder) will get deleted when the work folder is "cleaned" after the job finishes

                            Catch ex2 As Exception
                                ' Copy also failed
                                ' Continue moving files; we'll fail the results at the end of this function
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, " MoveResultFiles: error moving/copying file: " & TmpFile & ex.Message)
                                blnErrorEncountered = True
                            End Try
                        End Try
                    End If
                Next

                If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
                    ' Look for any extensions in htAcceptStats that had over 50 accepted files
                    objExtension = htAcceptStats.GetEnumerator
                    Do While objExtension.MoveNext
                        If CInt(objExtension.Value) > ACCEPT_LOGGING_THRESHOLD Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted a total of " & CInt(objExtension.Value) & " files with extension " & CStr(objExtension.Key))
                        End If
                    Loop

                    ' Look for any extensions in htRejectStats that had over 10 rejected files
                    objExtension = htRejectStats.GetEnumerator
                    Do While objExtension.MoveNext
                        If CInt(objExtension.Value) > REJECT_LOGGING_THRESHOLD Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Rejected a total of " & CInt(objExtension.Value) & " files with extension " & CStr(objExtension.Key))
                        End If
                    Loop
                End If

            Catch Err As Exception
                If m_DebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerBase.MoveResultFiles(); Error moving files to results folder")
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Tmpfile = " & TmpFile)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Results folder name = " & System.IO.Path.Combine(ResFolderNamePath, Path.GetFileName(TmpFile)))
                End If
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error moving results files, job " & m_JobNum & Err.Message)
                m_message = AppendToComment(m_message, "Error moving results files")

                blnErrorEncountered = True
            End Try

            Try
                'Make the summary file
                OutputSummary(ResFolderNamePath)
            Catch ex As Exception
                ' Ignore errors here
            End Try

            If blnErrorEncountered Then
                ' Try to save whatever files were moved into the results folder
                Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
                objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            Else
                Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            End If

        End Function

        ''' <summary>
        ''' Updates the analysis summary file
        ''' </summary>
        ''' <returns>TRUE for success, FALSE for failure</returns>
        ''' <remarks></remarks>
        Protected Overridable Function UpdateSummaryFile() As Boolean
            Dim strTool As String
            Dim strToolAndStepTool As String
            Try
                'Add a separator
                clsSummaryFile.Add(vbCrLf & "=====================================================================================" & vbCrLf)

                ' Construct the Tool description (combination of Tool name and Step Tool name)
                strTool = m_jobParams.GetParam("ToolName")

                strToolAndStepTool = m_jobParams.GetParam("StepTool")
                If strToolAndStepTool Is Nothing Then strToolAndStepTool = String.Empty

                If strToolAndStepTool <> strTool Then
                    If strToolAndStepTool.Length > 0 Then
                        strToolAndStepTool &= " (" & strTool & ")"
                    Else
                        strToolAndStepTool &= strTool
                    End If
                End If

                'Add the data
                clsSummaryFile.Add("Job Number" & ControlChars.Tab & m_jobParams.GetParam("Job"))
                clsSummaryFile.Add("Job Step" & ControlChars.Tab & m_jobParams.GetParam("Step"))
                clsSummaryFile.Add("Date" & ControlChars.Tab & System.DateTime.Now().ToString)
                clsSummaryFile.Add("Processor" & ControlChars.Tab & m_MachName)
                clsSummaryFile.Add("Tool" & ControlChars.Tab & strToolAndStepTool)
                clsSummaryFile.Add("Dataset Name" & ControlChars.Tab & m_jobParams.GetParam("datasetNum"))
                clsSummaryFile.Add("Xfer Folder" & ControlChars.Tab & m_jobParams.GetParam("transferFolderPath"))
                clsSummaryFile.Add("Param File Name" & ControlChars.Tab & m_jobParams.GetParam("parmFileName"))
                clsSummaryFile.Add("Settings File Name" & ControlChars.Tab & m_jobParams.GetParam("settingsFileName"))
                clsSummaryFile.Add("Legacy Organism Db Name" & ControlChars.Tab & m_jobParams.GetParam("LegacyFastaFileName"))
                clsSummaryFile.Add("Protein Collection List" & ControlChars.Tab & m_jobParams.GetParam("ProteinCollectionList"))
                clsSummaryFile.Add("Protein Options List" & ControlChars.Tab & m_jobParams.GetParam("ProteinOptions"))
                clsSummaryFile.Add("Fasta File Name" & ControlChars.Tab & m_jobParams.GetParam("generatedFastaName"))
                clsSummaryFile.Add("Analysis Time (hh:mm:ss)" & ControlChars.Tab & CalcElapsedTime(m_StartTime, m_StopTime))

                'Add another separator
                clsSummaryFile.Add(vbCrLf & "=====================================================================================" & vbCrLf)

            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") _
                 & " - " & ex.Message)
                Return False
            End Try

            Return True

        End Function

        ''' <summary>
        ''' Calculates total run time for a job
        ''' </summary>
        ''' <param name="StartTime">Time job started</param>
        ''' <param name="StopTime">Time of job completion</param>
        ''' <returns>Total job run time (HH:MM)</returns>
        ''' <remarks></remarks>
        Protected Function CalcElapsedTime(ByVal StartTime As DateTime, ByVal StopTime As DateTime) As String
            Dim dtElapsedTime As System.TimeSpan

            If StopTime < StartTime Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Stop time is less than StartTime; this is unexpected.  Assuming current time for StopTime")
                StopTime = System.DateTime.Now
            End If

            If StopTime < StartTime OrElse StartTime = System.DateTime.MinValue Then
                Return String.Empty
            End If

            dtElapsedTime = StopTime.Subtract(StartTime)

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, StartTime = " & StartTime.ToString)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, Stoptime = " & StopTime.ToString)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, Hours = " & dtElapsedTime.Hours.ToString)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, Minutes = " & dtElapsedTime.Minutes.ToString)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, Seconds = " & dtElapsedTime.Seconds.ToString)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, TotalMinutes = " & dtElapsedTime.TotalMinutes.ToString("0.00"))
            End If

            Return dtElapsedTime.Hours.ToString("###0") & ":" & dtElapsedTime.Minutes.ToString("00") & ":" & dtElapsedTime.Seconds.ToString("00")

        End Function

        ''' <summary>
        ''' Sets return message from analysis error and cleans working directory
        ''' </summary>
        ''' <param name="OopsMessage">Message to include in job comment field</param>
        ''' <remarks></remarks>
        Protected Overridable Sub CleanupFailedJob(ByVal OopsMessage As String)

            m_message = AppendToComment(m_message, OopsMessage)
            CleanWorkDir(m_WorkDir)

        End Sub

        ''' <summary>
        ''' Adds manager assembly data to job summary file
        ''' </summary>
        ''' <param name="OutputPath">Path to summary file</param>
        ''' <remarks></remarks>
        Protected Sub OutputSummary(ByVal OutputPath As String)

            'Saves the summary file in the results folder

            clsAssemblyTools.GetComponentFileVersionInfo()
            clsSummaryFile.SaveSummaryFile(System.IO.Path.Combine(OutputPath, m_jobParams.GetParam("StepTool") & "_AnalysisSummary.txt"))

        End Sub

        ''' <summary>
        ''' Makes multiple tries to delete specified file
        ''' </summary>
        ''' <param name="FileNamePath">Full path to file for deletion</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks>Raises exception if error occurs</remarks>
        Public Overridable Function DeleteFileWithRetries(ByVal FileNamePath As String) As Boolean

            Dim RetryCount As Integer = 0
            Dim ErrType As AMFileNotDeletedAfterRetryException.RetryExceptionType

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.DeleteFileWithRetries, executing method")
            End If

            'Verify specified file exists
            If Not File.Exists(FileNamePath) Then
                'Throw an exception
                Throw New AMFileNotFoundException(FileNamePath, "Specified file not found")
                Return False
            End If

            While RetryCount < 3
                Try
                    File.Delete(FileNamePath)
                    If m_DebugLevel > 4 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.DeleteFileWithRetries, normal exit")
                    End If
                    Return True
                Catch Err1 As UnauthorizedAccessException
                    'File may be read-only. Clear read-only flag and try again
                    If m_DebugLevel > 0 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "File " & FileNamePath & " exception ERR1: " & Err1.Message)
                        If Not Err1.InnerException Is Nothing Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inner exception: " & Err1.InnerException.Message)
                        End If
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "File " & FileNamePath & " may be read-only, attribute reset attempt #" & _
                         RetryCount.ToString)
                    End If
                    File.SetAttributes(FileNamePath, File.GetAttributes(FileNamePath) And (Not FileAttributes.ReadOnly))
                    ErrType = AMFileNotDeletedAfterRetryException.RetryExceptionType.Unauthorized_Access_Exception
                    RetryCount += 1
                Catch Err2 As IOException
                    'If problem is locked file, attempt to fix lock and retry
                    If m_DebugLevel > 0 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "File " & FileNamePath & " exception ERR2: " & Err2.Message)
                        If Not Err2.InnerException Is Nothing Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inner exception: " & Err2.InnerException.Message)
                        End If
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Error deleting file " & FileNamePath & ", attempt #" & RetryCount.ToString)
                    End If
                    ErrType = AMFileNotDeletedAfterRetryException.RetryExceptionType.IO_Exception
                    'Delay 5 seconds
                    System.Threading.Thread.Sleep(5000)
                    'Do a garbage collection in case something is hanging onto the file that has been closed, but not GC'd 
                    GC.Collect()
                    GC.WaitForPendingFinalizers()
                    RetryCount += 1
                Catch Err3 As Exception
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting file, exception ERR3 " & FileNamePath & Err3.Message)
                    Throw New AMFileNotDeletedException(FileNamePath, Err3.Message)
                    Return False
                End Try
            End While

            'If we got to here, then we've exceeded the max retry limit
            Throw New AMFileNotDeletedAfterRetryException(FileNamePath, ErrType, "Unable to delete or move file after multiple retries")
            Return False

        End Function

        'TODO: Is this really necessary now?
        Public Sub SetResourcerDataFileList(ByVal DataFileList() As String) Implements IToolRunner.SetResourcerDataFileList
            If DataFileList Is Nothing Then
                ReDim m_ResourcerDataFileList(-1)
            Else
                ReDim m_ResourcerDataFileList(DataFileList.Length - 1)
                Array.Copy(DataFileList, m_ResourcerDataFileList, DataFileList.Length)
            End If
        End Sub

        ''' <summary>
        ''' Copies the files from the results folder to the transfer folder on the server
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Overridable Function CopyResultsFolderToServer() As IJobParams.CloseOutType

            Dim SourceFolderPath As String = String.Empty
            Dim TransferFolderPath As String = String.Empty
            Dim TargetFolderPath As String = String.Empty
            Dim DatasetName As String = String.Empty
            Dim ResultsFolderName As String = String.Empty

            Dim objSourceFolderInfo As System.IO.DirectoryInfo
            Dim objSourceFile As System.IO.FileInfo
            Dim objTargetFile As System.IO.FileInfo

            Dim htFilesToOverwrite As System.Collections.Hashtable
            Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)

            Dim strMessage As String
            Dim blnErrorEncountered As Boolean = False

            Try
                m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.DELIVERING_RESULTS, 0)

                ResultsFolderName = m_jobParams.GetParam("OutputFolderName")
                If ResultsFolderName Is Nothing OrElse ResultsFolderName.Length = 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Results folder name is not defined, job " & m_jobParams.GetParam("Job"))
                    m_message = "Results folder not found"
                    'TODO: Handle errors
                    ' Without a source folder; there isn't much we can do
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                SourceFolderPath = System.IO.Path.Combine(m_WorkDir, ResultsFolderName)

                'Verify the source folder exists
                If Not System.IO.Directory.Exists(SourceFolderPath) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Results folder not found, job " & m_jobParams.GetParam("Job") & ", folder " & SourceFolderPath)
                    m_message = "Results folder not found"
                    'TODO: Handle errors
                    ' Without a source folder; there isn't much we can do
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                TransferFolderPath = m_jobParams.GetParam("transferFolderPath")

                'Verify transfer directory exists
                If TransferFolderPath Is Nothing OrElse TransferFolderPath.Length = 0 OrElse _
                   Not System.IO.Directory.Exists(TransferFolderPath) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Transfer folder not found, job " & m_jobParams.GetParam("Job"))
                    m_message = "Transfer folder not found"
                    objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                'Determine if dataset folder in transfer directory already exists; make directory if it doesn't exist
                DatasetName = m_jobParams.GetParam("DatasetNum")
                If DatasetName Is Nothing OrElse DatasetName.Length = 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Dataset name is undefined, job " & m_jobParams.GetParam("Job"))
                    m_message = "Dataset name is undefined"
                    objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                TargetFolderPath = System.IO.Path.Combine(TransferFolderPath, DatasetName)
                If Not System.IO.Directory.Exists(TargetFolderPath) Then
                    'Make the dataset folder
                    Try
                        objAnalysisResults.CreateFolderWithRetry(TargetFolderPath)
                    Catch ex As Exception
                        m_message = AppendToComment(m_message, "Error creating results folder on " & System.IO.Path.GetPathRoot(TargetFolderPath)) & ": " & ex.Message
                        objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    End Try
                End If

                ' Now append the output folder name to TargetFolderPath
                TargetFolderPath = System.IO.Path.Combine(TargetFolderPath, ResultsFolderName)

            Catch ex As Exception
                m_message = AppendToComment(m_message, "Error creating results folder: " & ex.Message)
                If SourceFolderPath.Length > 0 Then
                    objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
                End If

                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try

            Try
                htFilesToOverwrite = New System.Collections.Hashtable
                htFilesToOverwrite.Clear()

                If System.IO.Directory.Exists(TargetFolderPath) Then
                    ' The xfer folder already exists

                    ' Examine the files in the results folder to see if any of the files already exist in the xfer folder
                    ' If they do, compare the file modification dates and post a warning if a file will be overwritten (because the file on the local computer is newer)

                    objSourceFolderInfo = New System.IO.DirectoryInfo(SourceFolderPath)
                    For Each objSourceFile In objSourceFolderInfo.GetFiles()
                        If System.IO.File.Exists(System.IO.Path.Combine(TargetFolderPath, objSourceFile.Name)) Then
                            objTargetFile = New System.IO.FileInfo(System.IO.Path.Combine(TargetFolderPath, objSourceFile.Name))

                            If objSourceFile.LastWriteTime > objTargetFile.LastWriteTime Then
                                strMessage = "File in transfer folder on server will be overwritten by newer file in results folder: " & objSourceFile.Name & "; new file date: " & objSourceFile.LastWriteTime.ToString & "; old file date: " & objTargetFile.LastWriteTime.ToString
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)

                                htFilesToOverwrite.Add(objSourceFile.Name.ToLower, 1)
                            End If
                        End If
                    Next
                Else
                    ' Need to create the xfer folder
                    Try
                        objAnalysisResults.CreateFolderWithRetry(TargetFolderPath)
                    Catch ex As Exception
                        m_message = AppendToComment(m_message, "Error creating results folder on " & System.IO.Path.GetPathRoot(TargetFolderPath)) & ": " & ex.Message
                        objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    End Try
                End If

            Catch ex As Exception
                m_message = AppendToComment(m_message, "Error comparing files in source folder to " & TargetFolderPath & ": " & ex.Message)
                objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try

            ' Copy results folder to xfer folder
            ' Existing files will be overwritten if they exist in htFilesToOverwrite (with the assumption that the files created by this manager are newer, and thus supersede existing files)
            Try

                ' Gather all the files in the local result folder
                Dim ResultFiles() As String
                Dim strSourceFileName As String
                Dim strTargetPath As String

                ' Note: Entries in ResultFiles will have full file paths, not just file names
                ResultFiles = System.IO.Directory.GetFiles(SourceFolderPath, "*.*")
                For Each FileToCopy As String In ResultFiles
                    strSourceFileName = System.IO.Path.GetFileName(FileToCopy)
                    strTargetPath = System.IO.Path.Combine(TargetFolderPath, strSourceFileName)

                    Try
                        If htFilesToOverwrite.Count > 0 AndAlso htFilesToOverwrite.Contains(strSourceFileName.ToLower) Then
                            ' Copy file and overwrite existing
                            objAnalysisResults.CopyFileWithRetry(FileToCopy, strTargetPath, True)
                        Else
                            ' Copy file only if it doesn't currently exist
                            If Not System.IO.File.Exists(strTargetPath) Then
                                objAnalysisResults.CopyFileWithRetry(FileToCopy, strTargetPath, True, 3, 10)
                            End If
                        End If
                    Catch ex As Exception
                        ' Continue copying files; we'll fail the results at the end of this function
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, " CopyResultsFolderToServer: error copying " & System.IO.Path.GetFileName(FileToCopy) & " to " & strTargetPath & ": " & ex.Message)
                        blnErrorEncountered = True
                    End Try
                Next

            Catch ex As Exception
                m_message = AppendToComment(m_message, "Error copying results folder to " & System.IO.Path.GetPathRoot(TargetFolderPath) & " : " & ex.Message)
                blnErrorEncountered = True
            End Try

            If blnErrorEncountered Then
                objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            Else
                Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            End If

        End Function
#End Region

    End Class

End Namespace
