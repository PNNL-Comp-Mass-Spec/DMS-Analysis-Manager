Imports PRISM.Logging
Imports System.IO
Imports PRISM.Files.clsFileTools
Imports System.Text.RegularExpressions
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerDeNovoID
    Inherits clsAnalysisToolRunnerSeqBase

    Public Sub New()
    End Sub

    Public Overrides Function RunTool() As IJobParams.CloseOutType

        'Runs the sequest analysis tool. Most functions used are inherited from base class(es)
        Dim StepResult As IJobParams.CloseOutType

        'Get the settings file info via the base class
        If Not MyBase.RunTool() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Make the dta's
        m_logger.PostEntry("Making DTA files, job " & m_JobNum, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
        Try
            'TODO: Fix for final version
            'StepResult = MyBase.MakeDTAFiles()
            If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return StepResult
            End If
        Catch Err As Exception
            m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunTool(), Exception making DTA files, " & Err.Message, _
                ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Me.CalculateNewStatus()
        m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, m_progress, m_DtaCount)

        'TODO: Fix for final version
        'Delete stray files with non-DOS names (lcq_dta bug)
        'StepResult = Me.DeleteNonDosFiles()
        'If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
        '    Return StepResult
        'End If

        Me.CalculateNewStatus()
        m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, m_progress, m_DtaCount)
        'Run the denovo analysis
        m_logger.PostEntry("Running denovo analysis, job " & m_JobNum, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
        Try
            StepResult = Me.PerformDeNovoAnalysis()
            If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return StepResult
            End If
        Catch Err As Exception
            m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunTool(), Exception running denova program, " & Err.Message, _
                ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Me.CalculateNewStatus()
        m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, m_progress, m_DtaCount)

        'run the packager
        m_logger.PostEntry("Packaging analysis results, job " & m_JobNum, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
        Try
            StepResult = Me.PkgResults()
            If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return StepResult
            End If
        Catch Err As Exception
            m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunTool(), Exception packaging results, " & err.Message, _
                ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Stop the job timer
        m_StopTime = System.DateTime.UtcNow

        'Make sure all files have released locks
        PRISM.Processes.clsProgRunner.GarbageCollectNow()
        System.Threading.Thread.Sleep(1000)

        'Get rid of raw data file
        Try
            StepResult = DeleteDataFile()
            If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return StepResult
            End If
        Catch Err As Exception
            m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunTool(), Exception while deleting data file, " & err.Message, _
                 ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'make results folder
        Try
            StepResult = MakeResultsFolder("Seq")
            If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return StepResult
            End If
        Catch Err As Exception
            m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunTool(), Exception making results folder, " & err.Message, _
                ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Try
            If Not UpdateSummaryFile() Then
                m_logger.PostEntry("Error creating summary file, job " & m_JobNum, _
                                            ILogger.logMsgType.logWarning, LOG_DATABASE)
            End If
        Catch Err As Exception
            m_logger.PostEntry("Error creating summary file, job " & m_JobNum, _
                     ILogger.logMsgType.logWarning, LOG_DATABASE)
        End Try

    End Function

    Protected Overrides Sub CalculateNewStatus()

        'Calculates percent complete for job. Denovo processing generates two output files for each .dta, 
        '	so total non-dta file count is divided by two for percentage calculation

        Dim FileArray() As String
        Dim AnnFileCount As Integer
        Dim FasFileCount As Integer

        'Get DTA count
        m_workdir = CheckTerminator(m_workdir)
        FileArray = Directory.GetFiles(m_workdir, "*.dta")
        m_DtaCount = FileArray.GetLength(0)

        'Get ANN file count
        FileArray = Directory.GetFiles(m_workdir, "*.ann")
        AnnFileCount = FileArray.GetLength(0)

        'Get ANN file count
        FileArray = Directory.GetFiles(m_workdir, "*.fas")
        FasFileCount = FileArray.GetLength(0)

        'Calculate % complete
        If m_dtacount > 0 Then
            m_progress = 100.0! * CSng((AnnFileCount + FasFileCount) / (2 * m_dtacount))
        Else
            m_progress = 0
        End If

    End Sub

    Protected Function PerformDeNovoAnalysis() As IJobParams.CloseOutType

        'Runs a denovo analysis using BSI PEAKS software
        '	PEAKS software must be installed and registered on analysis machine

        Dim CmdStr As String
        Dim OutputPath As String
        Dim DumFileCnt As Integer
        Dim MaxScanInFile As Integer
        Dim NumCPUs As String = m_mgrParams.GetParam("denovoid", "numberofprocessors")
        Dim ErrTol As String = m_settingsFileParams.GetParam("denovo", "errtolerance", 0.25).ToString
        Dim TopNum As String = m_settingsFileParams.GetParam("denovo", "topnumber", 10).ToString
        Dim ParFileSection As String = m_settingsFileParams.GetParam("denovo", "parfilesection", """Trypsin""")
        Dim JarFileNamePath As String = m_mgrParams.GetParam("denovoid", "jarloc")
        Dim FilesFound() As String

        OutputPath = CheckTerminator(m_WorkDir, False)

        'Verify PEAKS software is installed
        If Not File.Exists(Replace(JarFileNamePath, """", "")) Then
            m_logger.PostEntry("Denovo program not found, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            m_message = AppendToComment(m_message, "Denovo program not found")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up command string for executing denovo calc
        CmdStr = " -Xmx256M -jar " & JarFileNamePath & " " & OutputPath & " " & OutputPath
        CmdStr &= " " & Path.Combine(m_WorkDir, m_JobParams.GetParam("parmFileName")) & " "
        CmdStr &= ParFileSection & " " & ErrTol & " " & TopNum & " " & NumCPUs

        'Run the denovo program
        'TODO: Fix for final version
        'If Not RunProgram("java", CmdStr, "DeNovoID", True) Then
        '	m_logger.PostEntry("Error running denovo calc, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
        '	m_message = AppendToComment(m_message, "Error running denovo calc")
        '	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        'End If

        'Verify at least one .ann file has been created
        FilesFound = Directory.GetFiles(m_workdir, "*.ann")
        If FilesFound.GetLength(0) < 1 Then
            m_logger.PostEntry("No .ann files created, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            m_message = AppendToComment(m_message, "No .ann files created")
            Return IJobParams.CloseOutType.CLOSEOUT_NO_ANN_FILES
        End If

        'Verify at least one .fas file has been created
        FilesFound = Directory.GetFiles(m_workdir, "*.fas")
        If FilesFound.GetLength(0) < 1 Then
            m_logger.PostEntry("No .fas files created, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            m_message = AppendToComment(m_message, "No .fas files created")
            Return IJobParams.CloseOutType.CLOSEOUT_NO_FAS_FILES
        End If

        'We got this far, everything must have worked
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Overrides Function PkgResults() As IJobParams.CloseOutType

        'Run Eric's summary program, then pack dta's and zip .ann and .fas files
        Dim Zipper As PRISM.Files.ZipTools
        Dim FoundFiles() As String
        Dim TempFile As String
        Dim CmdStr As String

        'Run Eric's summary program
        CmdStr = m_WorkDir
        'TODO: Fix for final version
        'If Not RunProgram(m_mgrParams.GetParam("denovoid", "denovosummary"), CmdStr, "Summary", True) Then
        '	m_logger.PostEntry("Error packaging results, job " & m_jobnum, ILogger.logMsgType.logError, LOG_DATABASE)
        '	m_message = AppendToComment(m_message, "Error packaging results")
        '	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        'End If

        'Run Gary packager to produce concatenated dta file
        CmdStr = "-d " & CheckTerminator(m_WorkDir) & " -r " & m_JobParams.GetParam("datasetNum") & " -c dta"
        'TODO: Fix for final version
        'If Not RunProgram(m_mgrParams.GetParam("commonfileandfolderlocations", "packerloc"), CmdStr, "Pkgr", True) Then
        '	m_logger.PostEntry("Error packaging results, job " & m_jobnum, ILogger.logMsgType.logError, LOG_DATABASE)
        '	m_message = AppendToComment(m_message, "Error packaging results")
        '	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        'End If

        'Setup zipper
        Zipper = New PRISM.Files.ZipTools(m_WorkDir, m_mgrParams.GetParam("commonfileandfolderlocations", "zipprogram"))

        'Zip the .ann files
        If Not Zipper.MakeZipFile("-speed", Path.Combine(m_workdir, "Ann.zip"), Path.Combine(m_workdir, "*.ann")) Then
            m_logger.PostEntry("Error zipping .ann files, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            m_message = AppendToComment(m_message, "Error zipping .ann files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Zip the .fas files
        If Not Zipper.MakeZipFile("-speed", Path.Combine(m_workdir, "Fas.zip"), Path.Combine(m_workdir, "*.fas")) Then
            m_logger.PostEntry("Error zipping .fas files, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            m_message = AppendToComment(m_message, "Error zipping .fas files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Delete the unneeded files
        Try
            'Delete .ann files
            FoundFiles = Directory.GetFiles(m_workdir, "*.ann")
            For Each TempFile In FoundFiles
                DeleteFileWithRetries(TempFile)
            Next
            'Delete .fas files
            FoundFiles = Directory.GetFiles(m_workdir, "*.fas")
            For Each TempFile In FoundFiles
                DeleteFileWithRetries(TempFile)
            Next
            'Delete .dta files
            FoundFiles = Directory.GetFiles(m_workdir, "*.dta")
            For Each TempFile In FoundFiles
                DeleteFileWithRetries(TempFile)
            Next
        Catch Err As Exception
            m_logger.PostEntry("Error deleting files, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            m_message = AppendToComment(m_message, "Error deleting files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Got to here, so everything's OK
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    'TODO: Fix for final version
    'Protected Overrides Function DeleteNonDosFiles() As IJobParams.CloseOutType

    '    'Overrides base class to give more restrictive file deletion criteria
    '    Dim WorkDir As New DirectoryInfo(m_WorkDir)
    '    Dim TestFile As FileInfo
    '    Dim TestStr As String = ".dta$|.raw$|.xml$"

    '    For Each TestFile In WorkDir.GetFiles
    '        If Not Regex.IsMatch(TestFile.Extension, TestStr, RegexOptions.IgnoreCase) Then
    '            Try
    '                TestFile.Delete()
    '            Catch err As Exception
    '                m_logger.PostError("Error removing non-DOS files, job " & m_JobNum, err, LOG_DATABASE)
    '                m_message = AppendToComment(m_message, "Error removing non_DOS files")
    '                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
    '            End Try
    '        End If
    '    Next

    '    Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS


    'End Function

End Class

