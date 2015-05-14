' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/29/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Threading
Imports System.Data.SqlClient


Public Class clsAnalysisToolRunnerGlyQIQ
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running the GlyQ-IQ
    '*********************************************************************************************************

#Region "Constants and Enums"

    Protected Const PROGRESS_PCT_STARTING As Single = 1
    Protected Const PROGRESS_PCT_COMPLETE As Single = 99

    Protected Const USE_THREADING As Boolean = True

    Protected Const STORE_JOB_PSM_RESULTS_SP_NAME As String = "StoreJobPSMStats"
    
#End Region

#Region "Structures"

    Protected Structure udtPSMStatsType
        Public TotalPSMs As Integer
        Public UniquePeptideCount As Integer
        Public UniqueProteinCount As Integer
        Public Sub Clear()
            TotalPSMs = 0
            UniquePeptideCount = 0
            UniqueProteinCount = 0
        End Sub
    End Structure

#End Region

#Region "Module Variables"

    Protected mCoreCount As Integer

    Protected mSpectraSearched As Integer

    ''' <summary>
    ''' Dictionary of GlyQIqRunner instances
    ''' </summary>
    ''' <remarks>Key is core number (1 through NumCores), value is the instance</remarks>
    Protected mGlyQRunners As Dictionary(Of Integer, clsGlyQIqRunner)

    Private WithEvents mThermoFileReader As ThermoRawFileReaderDLL.FinniganFileIO.XRawFileIO
    Private WithEvents mStoredProcedureExecutor As PRISM.DataBase.clsExecuteDatabaseSP

#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs GlyQ-IQ
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        Try
            'Call base class for initial setup
            If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerGlyQIQ.RunTool(): Enter")
            End If

            ' Determine the path to the IQGlyQ program
            Dim progLoc As String
            progLoc = DetermineProgramLocation("GlyQIQ", "GlyQIQProgLoc", "IQGlyQ_Console.exe")

            If String.IsNullOrWhiteSpace(progLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Store the GlyQ-IQ version info in the database            
            If Not StoreToolVersionInfo(progLoc) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error determining GlyQ-IQ version"
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Run GlyQ-IQ
            Dim blnSuccess = RunGlyQIQ()

            If blnSuccess Then
                blnSuccess = CombineResultFiles()
            End If

            ' Zip up the settings files and batch files so we have a record of them
            PackageResults()

            m_progress = PROGRESS_PCT_COMPLETE

            'Stop the job timer
            m_StopTime = DateTime.UtcNow

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            Threading.Thread.Sleep(500)        ' 500 msec delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If Not blnSuccess Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MoveResultFiles()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                m_message = "Error moving files into results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' It is now safe to delete the _peaks.txt file that is in the transfer folder
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting the _peaks.txt file from the Results Transfer folder")
            End If

            RemoveNonResultServerFiles()

        Catch ex As Exception
            m_message = "Error in GlyQIQ->RunTool"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function CombineResultFiles() As Boolean

        Dim reFutureTarget = New Regex("\tFutureTarget\t", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

        Try

            ' Combine the results files
            Dim diResultsFolder = New DirectoryInfo(Path.Combine(m_WorkDir, "Results_" & m_Dataset))
            If Not diResultsFolder.Exists Then
                m_message = "Results folder not found: " & diResultsFolder.FullName
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            Dim blnSuccess = True

            Dim fiUnfilteredResults = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_iqResults_Unfiltered.txt"))
            Dim fiFilteredResults = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_iqResults.txt"))

            Using swUnfiltered = New StreamWriter(New FileStream(fiUnfilteredResults.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
                Using swFiltered = New StreamWriter(New FileStream(fiFilteredResults.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))

                    For core = 1 To mCoreCount
                        Dim fiResultFile = New FileInfo(Path.Combine(diResultsFolder.FullName, m_Dataset & "_iqResults_" & core & ".txt"))

                        If Not fiResultFile.Exists Then
                            If String.IsNullOrEmpty(m_message) Then
                                m_message = "Result file not found: " & fiResultFile.Name
                            End If
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Result file not found: " & fiResultFile.FullName)
                            blnSuccess = False
                            Continue For
                        End If

                        Dim linesRead = 0
                        Using srReader = New StreamReader(New FileStream(fiResultFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                            While Not srReader.EndOfStream
                                Dim lineIn = srReader.ReadLine()
                                linesRead += 1

                                If linesRead = 1 AndAlso core > 1 Then
                                    ' This is the header line from a core 2 or later file
                                    ' Skip it
                                    Continue While
                                End If

                                swUnfiltered.WriteLine(lineIn)

                                ' Write lines that do not contain "FutureTarget" to the _iqResults.txt file
                                If Not reFutureTarget.IsMatch(lineIn) Then
                                    swFiltered.WriteLine(lineIn)
                                End If

                            End While

                        End Using

                    Next

                End Using

            End Using

            Thread.Sleep(250)

            ' Zip the unfiltered results
            ZipFile(fiUnfilteredResults.FullName, True)

            ' Parse the filtered results to count the number of identified glycans
            blnSuccess = ExamineFilteredResults(fiFilteredResults)

            Return blnSuccess

        Catch ex As Exception
            m_message = "Exception in CombineResultFiles: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return False
        End Try


    End Function

    Private Function CountMsMsSpectra(ByVal rawFilePath As String) As Integer

        Try
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Counting the number of MS/MS spectra in " + Path.GetFileName(rawFilePath))
            End If

            mThermoFileReader = New ThermoRawFileReaderDLL.FinniganFileIO.XRawFileIO()

            If Not mThermoFileReader.OpenRawFile(rawFilePath) Then
                m_message = "Error opening the Thermo Raw file to count the MS/MS spectra"
                Return 0
            End If

            Dim scanCount = mThermoFileReader.GetNumScans

            Dim ms1ScanCount = 0
            Dim ms2ScanCount = 0

            For scan = 1 To scanCount
                Dim scanInfo As ThermoRawFileReaderDLL.clsScanInfo = Nothing

                If mThermoFileReader.GetScanInfo(scan, scanInfo) Then
                    If scanInfo.MSLevel > 1 Then
                        ms2ScanCount += 1
                    Else
                        ms1ScanCount += 1
                    End If
                End If

            Next

            mThermoFileReader.CloseRawFile()

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... MS1 spectra: " & ms1ScanCount)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... MS2 spectra: " & ms2ScanCount)
            End If

            If ms2ScanCount > 0 Then
                Return ms2ScanCount
            Else
                Return ms1ScanCount
            End If

        Catch ex As Exception
            m_message = "Exception in CountMsMsSpectra: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return 0
        End Try

    End Function

    Protected Function ExamineFilteredResults(ByVal fiResultsFile As FileInfo) As Boolean
        Dim job As Integer
        If Not Integer.TryParse(m_JobNum, job) Then
            m_message = "Unable to determine job number since '" & m_JobNum & "' is not numeric"
            Return False
        End If

        Return ExamineFilteredResults(fiResultsFile, job, String.Empty)

    End Function

    ''' <summary>
    ''' Examine the GlyQ-IQ results in the given file to count the number of PSMs and unique number of glycans
    ''' Post the results to DMS using jobNumber
    ''' </summary>
    ''' <param name="fiResultsFile"></param>
    ''' <param name="jobNumber"></param>
    ''' <param name="dmsConnectionStringOverride">Optional: DMS5 connection string</param>
    ''' <returns></returns>
    ''' <remarks>If dmsConnectionStringOverride is empty then PostJobResults will use the Manager Parameters (m_mgrParams)</remarks>
    Public Function ExamineFilteredResults(
      ByVal fiResultsFile As FileInfo,
      ByVal jobNumber As Integer,
      ByVal dmsConnectionStringOverride As String) As Boolean

        Try

            Dim headerSkipped As Boolean

            Dim totalPSMs = 0
            Dim uniqueCodeFormulaCombos = New SortedSet(Of String)
            Dim uniqueCodes = New SortedSet(Of String)

            Using srResults = New StreamReader(New FileStream(fiResultsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                While Not srResults.EndOfStream
                    Dim lineIn = srResults.ReadLine
                    Dim dataColumns = lineIn.Split(ControlChars.Tab)

                    If dataColumns Is Nothing OrElse dataColumns.Count < 3 Then
                        Continue While
                    End If

                    Dim compoundCode = dataColumns(1)
                    Dim empiricalFormula = dataColumns(2)

                    If Not headerSkipped Then
                        If String.Compare(compoundCode, "Code", True) <> 0 Then
                            m_message = "3rd column in the glycan result file is not Code"
                            Return False
                        End If

                        If String.Compare(empiricalFormula, "EmpiricalFormula", True) <> 0 Then
                            m_message = "3rd column in the glycan result file is not EmpiricalFormula"
                            Return False
                        End If

                        headerSkipped = True
                        Continue While
                    End If

                    Dim codePlusFormula = compoundCode & "_" & empiricalFormula

                    If Not uniqueCodeFormulaCombos.Contains(codePlusFormula) Then
                        uniqueCodeFormulaCombos.Add(codePlusFormula)
                    End If

                    If Not uniqueCodes.Contains(compoundCode) Then
                        uniqueCodes.Add(compoundCode)
                    End If

                    totalPSMs += 1

                End While
            End Using

            Dim udtPSMStats As udtPSMStatsType
            udtPSMStats.Clear()
            udtPSMStats.TotalPSMs = totalPSMs
            udtPSMStats.UniquePeptideCount = uniqueCodeFormulaCombos.Count
            udtPSMStats.UniqueProteinCount = uniqueCodes.Count

            ' Store the results in the database
            PostJobResults(jobNumber, udtPSMStats, dmsConnectionStringOverride)

            Return True

        Catch ex As Exception
            m_message = "Exception in ExamineFilteredResults"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function PackageResults() As Boolean

        Dim diTempZipFolder = New DirectoryInfo(Path.Combine(m_WorkDir, "FilesToZip"))

        Try

            If Not diTempZipFolder.Exists Then
                diTempZipFolder.Create()
            End If

            ' Move the batch files and console ouput files into the FilesToZip folder
            Dim diWorkDir = New DirectoryInfo(m_WorkDir)
            Dim lstFilesToMove = New List(Of FileInfo)

            Dim lstFiles = diWorkDir.GetFiles("*.bat")
            lstFilesToMove.AddRange(lstFiles)

            ' We don't keep the entire ConsoleOutput file
            ' Instead, just keep a trimmed version of the original, removing extraneous log messages
            For Each fiConsoleOutputFile In diWorkDir.GetFiles(clsGlyQIqRunner.GLYQ_IQ_CONSOLE_OUTPUT_PREFIX & "*.txt")
                PruneConsoleOutputFiles(fiConsoleOutputFile, diTempZipFolder)
            Next

            lstFilesToMove.AddRange(lstFiles)

            For Each fiFile In lstFilesToMove
                fiFile.MoveTo(Path.Combine(diTempZipFolder.FullName, fiFile.Name))
            Next

            ' Move selected files from the first WorkingParameters folder

            ' We just need to copy files from the first core's WorkingParameters folder
            Dim diWorkingParamsSource = New DirectoryInfo(Path.Combine(m_WorkDir, "WorkingParametersCore1"))

            Dim diWorkingParamsTarget = New DirectoryInfo(Path.Combine(diTempZipFolder.FullName, "WorkingParameters"))
            If Not diWorkingParamsTarget.Exists Then
                diWorkingParamsTarget.Create()
            End If

            Dim iqParamFileName = m_jobParams.GetJobParameter("ParmFileName", "")
            For Each fiFile In diWorkingParamsSource.GetFiles()
                Dim blnMoveFile = False

                If String.Compare(fiFile.Name, iqParamFileName, True) = 0 Then
                    blnMoveFile = True
                ElseIf fiFile.Name.StartsWith(clsAnalysisResourcesGlyQIQ.GLYQIQ_PARAMS_FILE_PREFIX) Then
                    blnMoveFile = True
                ElseIf fiFile.Name.StartsWith(clsAnalysisResourcesGlyQIQ.ALIGNMENT_PARAMETERS_FILENAME) Then
                    blnMoveFile = True
                ElseIf fiFile.Name.StartsWith(clsAnalysisResourcesGlyQIQ.EXECUTOR_PARAMETERS_FILE) Then
                    blnMoveFile = True
                End If

                If blnMoveFile Then
                    fiFile.MoveTo(Path.Combine(diWorkingParamsTarget.FullName, fiFile.Name))
                End If
            Next


            Dim strZipFilePath = Path.Combine(m_WorkDir, "GlyQIq_Automation_Files.zip")

            m_IonicZipTools.ZipDirectory(diTempZipFolder.FullName, strZipFilePath)

        Catch ex As Exception
            m_message = "Exception creating GlyQIq_Automation_Files.zip"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Try
            ' Clear the TempZipFolder
            Threading.Thread.Sleep(250)
            diTempZipFolder.Delete(True)

            Threading.Thread.Sleep(250)
            diTempZipFolder.Create()

        Catch ex As Exception
            ' This error can be safely ignored
        End Try

        Return True

    End Function

    Protected Function PostJobResults(
      ByVal jobNumber As Integer,
      ByVal udtPSMStats As udtPSMStatsType,
      ByVal dmsConnectionStringOverride As String) As Boolean

        Const MAX_RETRY_COUNT As Integer = 3

        Dim objCommand As SqlCommand

        Dim blnSuccess As Boolean

        Try

            ' Call stored procedure StoreJobPSMStats in DMS5

            objCommand = New SqlCommand()

            With objCommand
                .CommandType = CommandType.StoredProcedure
                .CommandText = STORE_JOB_PSM_RESULTS_SP_NAME

                .Parameters.Add(New SqlParameter("@Return", SqlDbType.Int))
                .Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

                .Parameters.Add(New SqlParameter("@Job", SqlDbType.Int))
                .Parameters.Item("@Job").Direction = ParameterDirection.Input
                .Parameters.Item("@Job").Value = jobNumber

                .Parameters.Add(New SqlParameter("@MSGFThreshold", SqlDbType.Float))
                .Parameters.Item("@MSGFThreshold").Direction = ParameterDirection.Input

                .Parameters.Item("@MSGFThreshold").Value = 1

                .Parameters.Add(New SqlParameter("@FDRThreshold", SqlDbType.Float))
                .Parameters.Item("@FDRThreshold").Direction = ParameterDirection.Input
                .Parameters.Item("@FDRThreshold").Value = 0.25

                .Parameters.Add(New SqlParameter("@SpectraSearched", SqlDbType.Int))
                .Parameters.Item("@SpectraSearched").Direction = ParameterDirection.Input
                .Parameters.Item("@SpectraSearched").Value = mSpectraSearched

                .Parameters.Add(New SqlParameter("@TotalPSMs", SqlDbType.Int))
                .Parameters.Item("@TotalPSMs").Direction = ParameterDirection.Input
                .Parameters.Item("@TotalPSMs").Value = udtPSMStats.TotalPSMs

                .Parameters.Add(New SqlParameter("@UniquePeptides", SqlDbType.Int))
                .Parameters.Item("@UniquePeptides").Direction = ParameterDirection.Input
                .Parameters.Item("@UniquePeptides").Value = udtPSMStats.UniquePeptideCount

                .Parameters.Add(New SqlParameter("@UniqueProteins", SqlDbType.Int))
                .Parameters.Item("@UniqueProteins").Direction = ParameterDirection.Input
                .Parameters.Item("@UniqueProteins").Value = udtPSMStats.UniqueProteinCount

                .Parameters.Add(New SqlParameter("@TotalPSMsFDRFilter", SqlDbType.Int))
                .Parameters.Item("@TotalPSMsFDRFilter").Direction = ParameterDirection.Input
                .Parameters.Item("@TotalPSMsFDRFilter").Value = udtPSMStats.TotalPSMs

                .Parameters.Add(New SqlParameter("@UniquePeptidesFDRFilter", SqlDbType.Int))
                .Parameters.Item("@UniquePeptidesFDRFilter").Direction = ParameterDirection.Input
                .Parameters.Item("@UniquePeptidesFDRFilter").Value = udtPSMStats.UniquePeptideCount

                .Parameters.Add(New SqlParameter("@UniqueProteinsFDRFilter", SqlDbType.Int))
                .Parameters.Item("@UniqueProteinsFDRFilter").Direction = ParameterDirection.Input
                .Parameters.Item("@UniqueProteinsFDRFilter").Value = udtPSMStats.UniqueProteinCount

                .Parameters.Add(New SqlParameter("@MSGFThresholdIsEValue", SqlDbType.TinyInt))
                .Parameters.Item("@MSGFThresholdIsEValue").Direction = ParameterDirection.Input

                .Parameters.Item("@MSGFThresholdIsEValue").Value = 0

            End With

            If mStoredProcedureExecutor Is Nothing OrElse Not String.IsNullOrWhiteSpace(dmsConnectionStringOverride) Then

                Dim strConnectionString As String

                If String.IsNullOrWhiteSpace(dmsConnectionStringOverride) Then
                    If m_mgrParams Is Nothing Then
                        Throw New Exception("m_mgrParams object has not been initialized")
                    End If

                    ' Gigasax.DMS5
                    strConnectionString = m_mgrParams.GetParam("connectionstring")
                Else
                    strConnectionString = dmsConnectionStringOverride
                End If

                mStoredProcedureExecutor = New PRISM.DataBase.clsExecuteDatabaseSP(strConnectionString)
            End If


            'Execute the SP (retry the call up to 3 times)
            Dim ResCode As Integer
            Dim strErrorMessage As String = String.Empty
            ResCode = mStoredProcedureExecutor.ExecuteSP(objCommand, MAX_RETRY_COUNT, strErrorMessage)

            If ResCode = 0 Then
                blnSuccess = True
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error storing PSM Results in database, " & STORE_JOB_PSM_RESULTS_SP_NAME & " returned " & ResCode)
                clsGlobal.AppendToComment(m_message, "Error storing PSM Results in database")

                blnSuccess = False
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception storing PSM Results in database: " & ex.Message)
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    Protected Sub PruneConsoleOutputFiles(ByVal fiConsoleOutputFile As FileInfo, ByVal diTargetFolder As DirectoryInfo)

        If fiConsoleOutputFile.Directory.FullName = diTargetFolder.FullName Then
            Throw New Exception("The Source console output file cannot reside in the Target Folder: " & fiConsoleOutputFile.FullName & " vs. " & diTargetFolder.FullName)
        End If

        Try

            Dim lstLinesToPrune = New List(Of String) From {
              "LC Peaks To Analyze:",
              "Best:",
              "Next Lc peak",
              "Next Peak Quality, we have",
              "No isotpe profile was found using the IterativelyFindMSFeature",
              "Peak Finished Procssing",
              "PostProccessing info adding",
              "Pre MS Processor... Press Key",
              "the old scan Range is",
              "The time is ",
              "BreakOut",
              "Loading",
              "      Fit Seed",
              "       LM Worked "
            }

            Dim reNumericLine = New Regex("^[0-9.]+$", RegexOptions.Compiled)

            Dim consoleOutputFilePruned = Path.Combine(diTargetFolder.FullName, fiConsoleOutputFile.Name)

            Using srInFile = New StreamReader(New FileStream(fiConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Using swOutfile = New StreamWriter(New FileStream(consoleOutputFilePruned, FileMode.Create, FileAccess.Write, FileShare.Read))
                    While Not srInFile.EndOfStream
                        Dim strLineIn = srInFile.ReadLine()

                        If strLineIn.StartsWith("start post run") Then
                            ' Ignore everthing after this point
                            Exit While
                        End If

                        For Each textToFind In lstLinesToPrune
                            If strLineIn.StartsWith(textToFind) Then
                                ' Skip this line
                                Continue While
                            End If
                        Next

                        If reNumericLine.IsMatch(strLineIn) Then
                            ' Skip this line
                            Continue While
                        End If

                        swOutfile.WriteLine(strLineIn)
                    End While
                End Using
            End Using

            ' Make sure that we don't keep the original, non-pruned file 
            ' The pruned file was created in diTargetFolder and will get included in GlyQIq_Automation_Files.zip
            '
            m_jobParams.AddResultFileToSkip(fiConsoleOutputFile.Name)

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in PruneConsoleOutputFiles: " & ex.Message)
        End Try

    End Sub

    Protected Function RunGlyQIQ() As Boolean

        Dim blnSuccess As Boolean
        Dim currentTask = "Initializing"

        Try

            mCoreCount = m_jobParams.GetJobParameter(clsAnalysisResourcesGlyQIQ.JOB_PARAM_ACTUAL_CORE_COUNT, 0)
            If mCoreCount < 1 Then
                m_message = "Core count reported by " & clsAnalysisResourcesGlyQIQ.JOB_PARAM_ACTUAL_CORE_COUNT & " is 0; unable to continue"
                Return False
            End If

            Dim rawDataType As String = m_jobParams.GetParam("RawDataType")
            Dim eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType)

            If eRawDataType = clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
                m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)
            Else
                m_message = "GlyQ-IQ presently only supports Thermo .Raw files"
                Return False
            End If

            ' Determine the number of MS/MS spectra in the .Raw file (required for PostJobResults)
            Dim rawFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
            mSpectraSearched = CountMsMsSpectra(rawFilePath)

            ' Set up and execute a program runner to run each batch file that launches GlyQ-IQ

            m_progress = PROGRESS_PCT_STARTING

            mGlyQRunners = New Dictionary(Of Integer, clsGlyQIqRunner)()
            Dim lstThreads As New List(Of Thread)

            For core = 1 To mCoreCount

                Dim batchFilePath = Path.Combine(m_WorkDir, clsAnalysisResourcesGlyQIQ.START_PROGRAM_BATCH_FILE_PREFIX & core & ".bat")

                currentTask = "Launching GlyQ-IQ, core " & core
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, currentTask & ": " & batchFilePath)

                Dim glyQRunner = New clsGlyQIqRunner(m_WorkDir, core, batchFilePath)
                AddHandler glyQRunner.CmdRunnerWaiting, AddressOf CmdRunner_LoopWaiting
                mGlyQRunners.Add(core, glyQRunner)

                If USE_THREADING Then
                    Dim newThread As New Thread(New ThreadStart(AddressOf glyQRunner.StartAnalysis))
                    newThread.Priority = Threading.ThreadPriority.Normal
                    newThread.Start()
                    lstThreads.Add(newThread)
                Else
                    glyQRunner.StartAnalysis()

                    If glyQRunner.Status = clsGlyQIqRunner.GlyQIqRunnerStatusCodes.Failure Then
                        m_message = "Error running " & Path.GetFileName(batchFilePath)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                        Return False
                    End If
                End If
            Next

            If USE_THREADING Then
                ' Wait for all of the threads to exit
                ' Run for a maximum of 14 days

                currentTask = "Waiting for all of the threads to exit"

                Dim dtStartTime = DateTime.UtcNow
                Dim completedCores As New SortedSet(Of Integer)

                While True

                    ' Poll the status of each of the threads

                    Dim stepsComplete = 0
                    Dim progressSum As Double = 0

                    For Each glyQRunner In mGlyQRunners
                        Dim eStatus = glyQRunner.Value.Status
                        If eStatus >= clsGlyQIqRunner.GlyQIqRunnerStatusCodes.Success Then
                            ' Analysis completed (or failed)
                            stepsComplete += 1

                            If Not completedCores.Contains(glyQRunner.Key) Then
                                completedCores.Add(glyQRunner.Key)
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "GlyQ-IQ processing core " & glyQRunner.Key & " is now complete")
                            End If

                        End If

                        progressSum += glyQRunner.Value.Progress
                    Next

                    Dim subTaskProgress = CSng(progressSum / mGlyQRunners.Count)
                    Dim updatedProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, subTaskProgress)
                    If updatedProgress > m_progress Then
                        ' This progress will get written to the status file and sent to the messaging queue by UpdateStatusRunning()
                        m_progress = updatedProgress
                    End If

                    If stepsComplete >= mGlyQRunners.Count Then
                        ' All threads are done
                        Exit While
                    End If

                    Thread.Sleep(2000)

                    If DateTime.UtcNow.Subtract(dtStartTime).TotalDays > 14 Then
                        m_message = "GlyQ-IQ ran for over 14 days; aborting"

                        For Each glyQRunner In mGlyQRunners
                            glyQRunner.Value.AbortProcessingNow()
                        Next

                        Return False
                    End If
                End While
            End If

            blnSuccess = True
            Dim exitCode As Integer = 0

            currentTask = "Looking for console output error messages"

            ' Look for any console output error messages
            For Each glyQRunner In mGlyQRunners

                Dim progRunner = glyQRunner.Value.ProgRunner

                If progRunner Is Nothing Then Continue For

                For Each cachedError In progRunner.CachedConsoleErrors
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Core " & glyQRunner.Key & ": " & cachedError)
                    blnSuccess = False
                Next

                If progRunner.ExitCode <> 0 AndAlso exitCode = 0 Then
                    exitCode = progRunner.ExitCode
                End If

            Next

            If Not blnSuccess Then
                Dim Msg As String
                Msg = "Error running GlyQ-IQ"
                m_message = clsGlobal.AppendToComment(m_message, Msg)

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

                If exitCode <> 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "GlyQ-IQ returned a non-zero exit code: " & exitCode.ToString())
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to GlyQ-IQ failed (but exit code is 0)")
                End If

                Return False
            End If

            m_progress = PROGRESS_PCT_COMPLETE

            m_StatusTools.UpdateAndWrite(m_progress)
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "GlyQ-IQ Analysis Complete")
            End If

            Return True

        Catch ex As Exception
            m_message = "Error in RunGlyQIQ while " & currentTask
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo(ByVal strProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim blnSuccess As Boolean

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        Dim fiProgram = New FileInfo(strProgLoc)
        If Not fiProgram.Exists Then
            Try
                strToolVersionInfo = "Unknown"
                Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo), blnSaveToolVersionTextFile:=False)
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
                Return False
            End Try

        End If

        ' Lookup the version of the .NET application

        ' One method is to call MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, fiProgram.FullName)
        ' Second method is to call StoreToolVersionInfoOneFile64Bit
        ' But those both fail; directly call the one that works:
        blnSuccess = StoreToolVersionInfoViaSystemDiagnostics(strToolVersionInfo, fiProgram.FullName)

        If Not blnSuccess Then Return False

        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles = New List(Of FileInfo)
        ioToolFiles.Add(fiProgram)

        ioToolFiles.Add(New FileInfo(Path.Combine(fiProgram.Directory.FullName, "IQGlyQ.dll")))
        ioToolFiles.Add(New FileInfo(Path.Combine(fiProgram.Directory.FullName, "IQ2_x64.dll")))
        ioToolFiles.Add(New FileInfo(Path.Combine(fiProgram.Directory.FullName, "Run64.dll")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Private Sub UpdateStatusRunning(ByVal sngPercentComplete As Single)
        m_progress = sngPercentComplete
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
    End Sub

#End Region

#Region "Event Handlers"

    Private Sub m_ExecuteSP_DebugEvent(ByVal errorMessage As String) Handles mStoredProcedureExecutor.DebugEvent
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "StoredProcedureExecutor: " & errorMessage)

    End Sub

    Private Sub m_ExecuteSP_DBErrorEvent(ByVal errorMessage As String) Handles mStoredProcedureExecutor.DBErrorEvent
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "StoredProcedureExecutor: " & errorMessage)

        If Message.Contains("permission was denied") Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Message)
        End If
    End Sub

    Private Sub mThermoFileReader_ReportError(strMessage As String) Handles mThermoFileReader.ReportError
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Message)
    End Sub

    Private Sub mThermoFileReader_ReportWarning(strMessage As String) Handles mThermoFileReader.ReportWarning
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Message)
    End Sub

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting()

        Static dtLastStatusUpdate As DateTime = DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        ' Update the status file (limit the updates to every 5 seconds)
        If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = DateTime.UtcNow
            UpdateStatusRunning(m_progress)
        End If
    End Sub

#End Region

End Class
