Option Strict On

'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerDtaRefinery
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running DTA_Refinery analysis
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_DTA_REFINERY_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_PEPTIDEHIT_START As Single = 95
    Protected Const PROGRESS_PCT_PEPTIDEHIT_COMPLETE As Single = 99

    Protected m_Dataset As String = String.Empty

    Protected WithEvents CmdRunner As clsRunDosProgram
    '--------------------------------------------------------------------------------------------
    'Future section to monitor DTA_Refinery log file for progress determination
    '--------------------------------------------------------------------------------------------
    'Dim WithEvents m_StatFileWatch As FileSystemWatcher
    'Protected m_XtSetupFile As String = "default_input.xml"
    '--------------------------------------------------------------------------------------------
    'End future section
    '--------------------------------------------------------------------------------------------
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs DTA_Refinery tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType
        Dim OrgDBName As String = m_jobParams.GetParam("generatedFastaName")
        Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        m_Dataset = m_jobParams.GetParam("datasetNum")


        ' Make sure the _DTA.txt file is valid
        If Not ValidateCDTAFile() Then
            Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running DTA_Refinery")

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDtaRefinery.RunTool(): Enter")
        End If

        ' verify that program file exists
        ' DTARefineryLoc will be something like this: "c:\dms_programs\DTARefinery\dta_refinery.exe"
        Dim progLoc As String = m_mgrParams.GetParam("DTARefineryLoc")
        If Not System.IO.File.Exists(progLoc) Then
            If progLoc.Length = 0 Then progLoc = "Parameter 'DTARefineryLoc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find DTA_Refinery program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim CmdStr As String
        CmdStr = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("DTARefineryXMLFile"))
        CmdStr &= " " & System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt")
        CmdStr &= " " & System.IO.Path.Combine(LocalOrgDBFolder, OrgDBName)


        If Not CmdRunner.RunProgram(progLoc, CmdStr, "DTARefinery", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running DTARefinery, job " & m_JobNum)

            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging DTA_Refinery problems
            CopyFailedResultsToArchiveFolder()

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Stop the job timer
        m_StopTime = System.DateTime.Now

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

        'Zip the output file
        result = ZipMainOutputFile()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging DTA_Refinery problems
            CopyFailedResultsToArchiveFolder()
            Return result
        End If

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrEmpty(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        Try
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.zip"))
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt"))
        Catch ex As Exception
            ' Ignore errors here
        End Try

        ' Make the results folder
        result = MakeResultsFolder()
        If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the result files into the result folder
            result = MoveResultFiles()
            If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Move was a success; update strFolderPathToArchive
                strFolderPathToArchive = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)


    End Sub

    ''' <summary>
    ''' Make sure the _DTA.txt file exists and has at lease one spectrum in it
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ValidateCDTAFile() As Boolean
        Dim strInputFilePath As String
        Dim srReader As System.IO.StreamReader

        Dim blnDataFound As Boolean = False

        Try
            strInputFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt")

            If Not System.IO.File.Exists(strInputFilePath) Then
                m_message = "_DTA.txt file not found: " & strInputFilePath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            srReader = New System.IO.StreamReader(New System.IO.FileStream(strInputFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

            Do While srReader.Peek >= 0
                If srReader.ReadLine.Trim.Length > 0 Then
                    blnDataFound = True
                    Exit Do
                End If
            Loop

            srReader.Close()

            If Not blnDataFound Then
                m_message = "The _DTA.txt file is empty"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            End If

        Catch ex As Exception
            m_message = "Exception in ValidateCDTAFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Return blnDataFound

    End Function

    ''' <summary>
    ''' Zips concatenated XML output file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function ZipMainOutputFile() As IJobParams.CloseOutType

        Dim ioWorkDirectory As System.IO.DirectoryInfo
        Dim ioFiles() As System.IO.FileInfo
        Dim ioFile As System.IO.FileInfo
        Dim strFixedDTAFilePath As String

        'Do we want to zip these output files?  Just zipping the fixed dta file.
        '* _dta_DtaRefineryLog.txt 
        '* _dta_SETTINGS.xml
        '* _FIXED_dta.txt
        '* _HIST.png
        '* _HIST.txt
        ' * scan number: _scanNum.png
        ' * m/z: _mz.png
        ' * log10 of ion intensity in the ICR/Orbitrap cell: _logTrappedIonInt.png
        ' * total ion current in the ICR/Orbitrap cell: _trappedIonsTIC.png



        'Delete the original DTA files
        Try
            ioWorkDirectory = New System.IO.DirectoryInfo(m_WorkDir)
            ioFiles = ioWorkDirectory.GetFiles("*_dta.*")

            For Each ioFile In ioFiles
                If Not ioFile.Name.ToUpper.EndsWith("_FIXED_dta.txt".ToUpper) Then
                    ioFile.Attributes = ioFile.Attributes And (Not System.IO.FileAttributes.ReadOnly)
                    ioFile.Delete()
                End If
            Next

        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDtaRefinery.ZipMainOutputFile, Error deleting _om.omx file, job " & m_JobNum & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Try
            strFixedDTAFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_FIXED_dta.txt")
            ioFile = New System.IO.FileInfo(strFixedDTAFilePath)

            If Not ioFile.Exists Then
                Dim Msg As String = "DTARefinery output file not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ": " & ioFile.Name)
                m_message = AppendToComment(m_message, Msg)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ioFile.MoveTo(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt"))

            Try
                If Not MyBase.ZipFile(ioFile.FullName, True) Then
                    Dim Msg As String = "Error zipping DTARefinery output file"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ": " & ioFile.FullName)
                    m_message = AppendToComment(m_message, Msg)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

            Catch ex As Exception
                Dim Msg As String = "clsAnalysisToolRunnerDtaRefinery.ZipMainOutputFile, Error zipping DTARefinery output file: " & ex.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                m_message = AppendToComment(m_message, "Error zipping DTARefinery output file")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try

        Catch ex As Exception
            Dim Msg As String = "clsAnalysisToolRunnerDtaRefinery.ZipMainOutputFile, Error renaming DTARefinery output file: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AppendToComment(m_message, "Error renaming DTARefinery output file")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.Now

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.Now
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_DTA_REFINERY_RUNNING, 0, "", "", "", False)
        End If

    End Sub

    '--------------------------------------------------------------------------------------------
    'Future section to monitor log file for progress determination
    '--------------------------------------------------------------------------------------------
    '	Private Sub StartFileWatcher(ByVal DirToWatch As String, ByVal FileToWatch As String)

    ''Watches the DTA_Refinery status file and reports changes

    ''Setup
    'm_StatFileWatch = New FileSystemWatcher
    'With m_StatFileWatch
    '	.BeginInit()
    '	.Path = DirToWatch
    '	.IncludeSubdirectories = False
    '	.Filter = FileToWatch
    '	.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
    '	.EndInit()
    'End With

    ''Start monitoring
    'm_StatFileWatch.EnableRaisingEvents = True

    '	End Sub
    '--------------------------------------------------------------------------------------------
    'End future section
    '--------------------------------------------------------------------------------------------
#End Region

End Class
