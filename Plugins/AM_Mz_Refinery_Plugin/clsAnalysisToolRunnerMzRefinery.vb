'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 09/05/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Runtime.InteropServices
Imports AnalysisManagerMSGFDBPlugIn
Imports System.Text.RegularExpressions

''' <summary>
''' Class for running Mz Refinery to recalibrate m/z values in a .mzML file
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisToolRunnerMzRefinery
	Inherits clsAnalysisToolRunnerBase

#Region "Constants and Enums"
	Protected Const PROGRESS_PCT_STARTING As Single = 1
    Protected Const PROGRESS_PCT_MZREFINERY_COMPLETE As Single = 97
	Protected Const PROGRESS_PCT_PLOTS_GENERATED As Single = 98
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

    Protected Const MZ_REFINERY_CONSOLE_OUTPUT As String = "MSConvert_MzRefinery_ConsoleOutput.txt"
	Protected Const ERROR_CHARTER_CONSOLE_OUTPUT_FILE As String = "PPMErrorCharter_ConsoleOutput.txt"

	Public Const MSGFPLUS_MZID_SUFFIX As String = "_msgfplus.mzid"
#End Region

#Region "Module Variables"

	Protected mToolVersionWritten As Boolean

	Protected mConsoleOutputErrorMsg As String

	Protected mMSGFDbProgLoc As String
	Protected mMSConvertProgLoc As String
	Protected mPpmErrorCharterProgLoc As String

	Protected mMSGFPlusResultsFilePath As String

	Protected mRunningMSGFPlus As Boolean
	Protected mRunningzRefinerWithMSConvert As Boolean

	Protected mMSGFPlusComplete As Boolean
	Protected mMSGFPlusCompletionTime As DateTime

    Protected mSkipMzRefinery As Boolean

    Protected mMzRefineryCorrectionMode As String
    Protected mMzRefinerGoodDataPoints As Integer
    Protected mMzRefinerSpecEValueThreshold As Double

    Protected WithEvents mMSGFDBUtils As clsMSGFDBUtils

    Protected mMSXmlCacheFolder As DirectoryInfo

    Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"

    ''' <summary>
    ''' Runs MSGF+ then runs MSConvert with the MzRefiner filter
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        Try
            ' Call base class for initial setup
            If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMzRefinery.RunTool(): Enter")
            End If

            ' Initialize class-wide variables that will be updated later
            mMzRefineryCorrectionMode = String.Empty
            mMzRefinerGoodDataPoints = 0
            mMzRefinerSpecEValueThreshold = 0.0000000001

            ' Verify that program files exist

            ' Determine the path to the customized version of MSConvert that includes the MzRefiner filter
            mMSConvertProgLoc = DetermineProgramLocation("MzRefinery", "MzRefineryProgLoc", "msconvert.exe")

            If String.IsNullOrWhiteSpace(mMSConvertProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Determine the path to the PPM error charter program
            mPpmErrorCharterProgLoc = DetermineProgramLocation("MzRefinery", "MzRefineryProgLoc", "PPMErrorCharter.exe")

            If String.IsNullOrWhiteSpace(mPpmErrorCharterProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' javaProgLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
            Dim javaProgLoc = GetJavaProgLoc()
            If String.IsNullOrEmpty(javaProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Dim msXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)
            mMSXmlCacheFolder = New DirectoryInfo(msXMLCacheFolderPath)

            If Not mMSXmlCacheFolder.Exists Then
                LogError("MSXmlCache folder not found: " & msXMLCacheFolderPath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Look for existing MSGF+ results (which would have been retrieved by clsAnalysisResourcesMzRefinery)

            Dim fiMSGFPlusResults = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & MSGFPLUS_MZID_SUFFIX))
            Dim skippedMSGFPlus = False

            If fiMSGFPlusResults.Exists Then
                result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS
                skippedMSGFPlus = True
                m_jobParams.AddResultFileToSkip(fiMSGFPlusResults.Name)
            Else
                ' Run MSGF+ (includes indexing the fasta file)
                result = RunMSGFPlus(javaProgLoc, fiMSGFPlusResults)
            End If

            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Unknown error running MSGF+ prior to running MzRefiner"
                End If
                Return result
            End If

            CmdRunner = Nothing

            Dim blnSuccess As Boolean
            Dim processingError As Boolean = False

            Dim fiOriginalMzMLFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION))
            Dim fiFixedMzMLFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_FIXED" & clsAnalysisResources.DOT_MZML_EXTENSION))

            m_jobParams.AddResultFileToSkip(fiOriginalMzMLFile.Name)
            m_jobParams.AddResultFileToSkip(fiFixedMzMLFile.Name)

            If mSkipMzRefinery Then
                ' Rename the original file to have the expected name of the fixed mzML file
                ' Required for PostProcessMzRefineryResults to work properly
                fiOriginalMzMLFile.MoveTo(fiFixedMzMLFile.FullName)
            Else
                ' Run MSConvert with the MzRefiner filter
                blnSuccess = StartMzRefinery(fiOriginalMzMLFile, fiMSGFPlusResults)

                If Not blnSuccess Then
                    processingError = True
                Else
                    If mMzRefineryCorrectionMode.StartsWith("Chose no shift") Then
                        ' No valid peak was found; a result file may not exist
                        fiFixedMzMLFile.Refresh()
                        If Not fiFixedMzMLFile.Exists Then

                            ' Rename the original file to have the expected name of the fixed mzML file
                            ' Required for PostProcessMzRefineryResults to work properly
                            fiOriginalMzMLFile.MoveTo(fiFixedMzMLFile.FullName)
                        End If
                    End If
                End If

            End If

            ' Look for the results file
            fiFixedMzMLFile.Refresh()
            If fiFixedMzMLFile.Exists AndAlso Not processingError Then
                blnSuccess = PostProcessMzRefinerResults(fiMSGFPlusResults, fiFixedMzMLFile)
                If Not blnSuccess Then processingError = True
            Else
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "MzRefinery results file not found: " & fiFixedMzMLFile.Name
                    processingError = True
                End If
            End If

            m_progress = PROGRESS_PCT_COMPLETE

            'Stop the job timer
            m_StopTime = DateTime.UtcNow

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            CmdRunner = Nothing

            ' Make sure objects are released
            Threading.Thread.Sleep(2000)        '2 second delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If processingError Then
                Dim msgfPlusResultsExist = False

                If Not fiMSGFPlusResults Is Nothing AndAlso fiMSGFPlusResults.Exists() Then
                    ' MSGF+ succeeded but MzRefinery or PostProcessing failed
                    ' We will mark the job as failed, but we want to move the MSGF+ results into the transfer folder

                    If skippedMSGFPlus Then
                        msgfPlusResultsExist = True
                    Else
                        msgfPlusResultsExist = CompressMSGFPlusResults(fiMSGFPlusResults)
                    End If

                End If

                If Not msgfPlusResultsExist Then
                    ' Move the source files and any results to the Failed Job folder
                    ' Useful for debugging problems
                    CopyFailedResultsToArchiveFolder()
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

            End If

            result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' MakeResultsFolder handles posting to local log, so set database error message and exit
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

            If processingError Then
                ' If we get here, MSGF+ succeeded, but MzRefinery or PostProcessing failed
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing failed; see results at " & m_jobParams.GetParam("transferFolderPath"))
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            m_message = "Error in MzRefineryPlugin->RunTool"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Index the Fasta file (if needed) then run MSGF+
    ''' </summary>
    ''' <param name="javaProgLoc">Path to Java</param>
    ''' <param name="fiMSGFPlusResults">Output: MSGF+ results file</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function RunMSGFPlus(
      ByVal javaProgLoc As String,
      <Out()> ByRef fiMSGFPlusResults As FileInfo) As IJobParams.CloseOutType

        Const strMSGFJarfile As String = clsMSGFDBUtils.MSGFPLUS_JAR_NAME
        Const strSearchEngineName = "MSGF+"

        fiMSGFPlusResults = Nothing

        ' Determine the path to MSGF+
        ' It is important that you pass "MSGFDB" to this function because the 
        ' PeptideHitResultsProcessor (and possibly other software) expects the Tool Version file to be named Tool_Version_Info_MSGFDB.txt
        mMSGFDbProgLoc = DetermineProgramLocation("MSGFDB", "MSGFDbProgLoc", strMSGFJarfile)

        If String.IsNullOrWhiteSpace(mMSGFDbProgLoc) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Note: we will store the MSGF+ version info in the database after the first line is written to file MSGFDB_ConsoleOutput.txt
        mToolVersionWritten = False

        mMSGFPlusComplete = False

        ' These two variables are required for the call to ParseMSGFDBParameterFile
        ' They are blank because the source file is a mzML file, and that file includes scan type information
        Dim strScanTypeFilePath As String = String.Empty
        Dim strAssumedScanType As String = String.Empty

        ' Initialize mMSGFDBUtils
        mMSGFDBUtils = New clsMSGFDBUtils(m_mgrParams, m_jobParams, m_JobNum, m_WorkDir, m_DebugLevel, blnMSGFPlus:=True)

        ' Get the FASTA file and index it if necessary
        ' Note: if the fasta file is over 50 MB in size, then only use the first 50 MB

        ' Passing in the path to the parameter file so we can look for TDA=0 when using large .Fasta files
        Dim strParameterFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetJobParameter("MzRefParamFile", String.Empty))
        Dim javaExePath = String.Copy(javaProgLoc)
        Dim msgfdbJarFilePath = String.Copy(mMSGFDbProgLoc)

        Dim fastaFilePath As String = String.Empty
        Dim fastaFileSizeKB As Single
        Dim fastaFileIsDecoy As Boolean

        Dim udtHPCOptions As clsAnalysisResources.udtHPCOptionsType = clsAnalysisResources.GetHPCOptions(m_jobParams, m_MachName)

        Const maxFastaFileSizeMB As Integer = 50

        ' Initialize the fasta file; truncating it if it is over 50 MB in size
        Dim result = mMSGFDBUtils.InitializeFastaFile(
          javaExePath, msgfdbJarFilePath,
          fastaFileSizeKB, fastaFileIsDecoy, fastaFilePath,
          strParameterFilePath, udtHPCOptions, maxFastaFileSizeMB)

        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        Dim strInstrumentGroup As String = m_jobParams.GetJobParameter("JobParameters", "InstrumentGroup", String.Empty)

        ' Read the MSGF+ Parameter File
        Dim strMSGFDbCmdLineOptions = String.Empty

        result = mMSGFDBUtils.ParseMSGFDBParameterFile(fastaFileSizeKB, fastaFileIsDecoy, strAssumedScanType, strScanTypeFilePath, strInstrumentGroup, strParameterFilePath, udtHPCOptions, strMSGFDbCmdLineOptions)
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        ElseIf String.IsNullOrEmpty(strMSGFDbCmdLineOptions) Then
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Problem parsing MzRef parameter file to extract MGSF+ options"
            End If
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Look for extra parameters specific to MZRefiner
        Dim success = ExtractMzRefinerOptionsFromParameterFile(strParameterFilePath)
        If Not success Then
            m_message = "Error extracting MzRefinery options from parameter file " & Path.GetFileName(strParameterFilePath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim resultsFileName = m_Dataset & MSGFPLUS_MZID_SUFFIX
        fiMSGFPlusResults = New FileInfo(Path.Combine(m_WorkDir, resultsFileName))

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running " & strSearchEngineName)

        ' If an MSGF+ analysis crashes with an "out-of-memory" error, then we need to reserve more memory for Java 
        ' The amount of memory required depends on both the fasta file size and the size of the input .mzML file, since data from all spectra are cached in memory
        ' Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file 
        Dim intJavaMemorySize = m_jobParams.GetJobParameter("MzRefMSGFPlusJavaMemorySize", 1500)
        If intJavaMemorySize < 512 Then intJavaMemorySize = 512

        'Set up and execute a program runner to run MSGF+
        Dim cmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & msgfdbJarFilePath

        ' Define the input file, output file, and fasta file
        cmdStr &= " -s " & m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION

        cmdStr &= " -o " & fiMSGFPlusResults.Name
        cmdStr &= " -d " & PossiblyQuotePath(fastaFilePath)

        ' Append the remaining options loaded from the parameter file
        cmdStr &= " " & strMSGFDbCmdLineOptions

        ' Make sure the machine has enough free memory to run MSGF+
        Dim blnLogFreeMemoryOnSuccess = Not m_DebugLevel < 1

        If Not clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySize, strSearchEngineName, blnLogFreeMemoryOnSuccess) Then
            m_message = "Not enough free memory to run " & strSearchEngineName
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        success = StartMSGFPlus(javaExePath, strSearchEngineName, cmdStr)

        If Not success And String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
            ' Parse the console output file one more time in hopes of finding an error message
            ParseMSGFPlusConsoleOutputFile(m_WorkDir)
        End If

        If Not mToolVersionWritten Then
            If String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFDbVersion) Then
                ParseMSGFPlusConsoleOutputFile(m_WorkDir)
            End If
            mToolVersionWritten = StoreToolVersionInfo()
        End If

        If Not String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMSGFDBUtils.ConsoleOutputErrorMsg)
        End If

        Dim blnProcessingError As Boolean = False

        If success Then
            If Not mMSGFPlusComplete Then
                mMSGFPlusComplete = True
                mMSGFPlusCompletionTime = DateTime.UtcNow
            End If
        Else
            Dim msg As String
            If mMSGFPlusComplete Then
                msg = strSearchEngineName & " log file reported it was complete, but aborted the ProgRunner since Java was frozen"
            Else
                msg = "Error running " & strSearchEngineName
            End If
            m_message = clsGlobal.AppendToComment(m_message, msg)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg & ", job " & m_JobNum)

            If mMSGFPlusComplete Then
                ' Don't treat this as a fatal error
                blnProcessingError = False
                m_EvalMessage = String.Copy(m_message)
                m_message = String.Empty
            Else
                blnProcessingError = True
            End If

            If Not mMSGFPlusComplete Then
                If CmdRunner.ExitCode <> 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strSearchEngineName & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to " & strSearchEngineName & " failed (but exit code is 0)")
                End If
            End If

        End If

        If mMSGFPlusComplete Then
            m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE
            m_StatusTools.UpdateAndWrite(m_progress)
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGF+ Search Complete")
            End If
        End If

        ' Look for the .mzid file
        fiMSGFPlusResults.Refresh()

        If Not fiMSGFPlusResults.Exists Then
            If String.IsNullOrEmpty(m_message) Then
                m_message = "MSGF+ results file not found: " & resultsFileName
            End If
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        m_jobParams.AddResultFileToSkip(clsMSGFDBUtils.MOD_FILE_NAME)

        If blnProcessingError Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If

    End Function

    Protected Function StartMSGFPlus(ByVal javaExePath As String, ByVal strSearchEngineName As String, ByVal CmdStr As String) As Boolean

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, javaExePath & " " & CmdStr)
        End If

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(m_WorkDir, clsMSGFDBUtils.MSGFDB_CONSOLE_OUTPUT_FILE)
        End With

        m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_STARTING

        mRunningMSGFPlus = True

        ' Start MSGF+ and wait for it to exit
        Dim blnSuccess = CmdRunner.RunProgram(javaExePath, CmdStr, strSearchEngineName, True)

        mRunningMSGFPlus = False

        Return blnSuccess

    End Function

    Private Function CompressMSGFPlusResults(ByVal fiMSGFPlusResults As FileInfo) As Boolean

        Try

            ' Compress the MSGF+ .mzID file
            Dim blnSuccess = m_IonicZipTools.GZipFile(fiMSGFPlusResults.FullName, True)

            If Not blnSuccess Then
                m_message = m_IonicZipTools.Message
                Return False
            End If

            m_jobParams.AddResultFileToSkip(fiMSGFPlusResults.Name)
            m_jobParams.AddResultFileToKeep(fiMSGFPlusResults.Name & clsAnalysisResources.DOT_GZ_EXTENSION)

        Catch ex As Exception
            m_message = "Error compressing the .mzID file"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Return True
    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete any .mzML files first)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        Try
            Dim fiFiles = New DirectoryInfo(m_WorkDir).GetFiles("*" & clsAnalysisResources.DOT_MZML_EXTENSION)
            For Each fiFileToDelete In fiFiles
                fiFileToDelete.Delete()
            Next
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
                strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

    End Sub

    Private Function ExtractMzRefinerOptionsFromParameterFile(ByVal strParameterFilePath As String) As Boolean

        mSkipMzRefinery = False

        Try
            Using srParamFile As StreamReader = New StreamReader(New FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                Do While srParamFile.Peek > -1
                    Dim strLineIn = srParamFile.ReadLine()

                    Dim kvSetting = clsGlobal.GetKeyValueSetting(strLineIn)

                    If Not String.IsNullOrWhiteSpace(kvSetting.Key) Then

                        Dim blnValue As Boolean

                        Select Case kvSetting.Key
                            Case "SkipMzRefinery"
                                Dim strValue As String = kvSetting.Value
                                If Boolean.TryParse(strValue, blnValue) Then
                                    mSkipMzRefinery = blnValue
                                End If
                        End Select

                    End If
                Loop
            End Using

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ExtractMzRefinerOptionsFromParameterFile", ex)
            Return False
        End Try

        Return True

    End Function

    Private Sub MonitorProgress()

        Static dtLastStatusUpdate As DateTime = DateTime.UtcNow
        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 15 seconds)
        If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 15 Then
            dtLastStatusUpdate = DateTime.UtcNow
            UpdateStatusRunning(m_progress)
        End If

        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 30 Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            If mRunningMSGFPlus Then

                ParseMSGFPlusConsoleOutputFile(m_WorkDir)
                If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFDbVersion) Then
                    mToolVersionWritten = StoreToolVersionInfo()
                End If

                If m_progress >= clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE Then
                    If Not mMSGFPlusComplete Then
                        mMSGFPlusComplete = True
                        mMSGFPlusCompletionTime = DateTime.UtcNow
                    Else
                        If DateTime.UtcNow.Subtract(mMSGFPlusCompletionTime).TotalMinutes >= 5 Then
                            ' MSGF+ is stuck at 96% complete and has been that way for 5 minutes
                            ' Java is likely frozen and thus the process should be aborted
                            Dim warningMessage = "MSGF+ has been stuck at " & clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE.ToString("0") & "% complete for 5 minutes; aborting since Java appears frozen"
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)

                            ' Bump up mMSGFPlusCompletionTime by one hour
                            ' This will prevent this function from logging the above message every 30 seconds if the .abort command fails
                            mMSGFPlusCompletionTime = mMSGFPlusCompletionTime.AddHours(1)

                            CmdRunner.AbortProgramNow()

                        End If
                    End If
                End If

            ElseIf mRunningzRefinerWithMSConvert Then
                ParseMSConvertConsoleOutputfile(Path.Combine(m_WorkDir, MZ_REFINERY_CONSOLE_OUTPUT))
            End If

        End If
    End Sub


    ''' <summary>
    ''' Parse the MSGF+ console output file to determine the MSGF+ version and to track the search progress
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub ParseMSGFPlusConsoleOutputFile(ByVal workingDirectory As String)

        Try
            If Not mMSGFDBUtils Is Nothing Then
                Dim msgfPlusProgress = mMSGFDBUtils.ParseMSGFDBConsoleOutputFile(workingDirectory)
                UpdateProgress(msgfPlusProgress)
            End If

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing MSGF+ console output file: " & ex.Message)
            End If
        End Try

    End Sub

    ''' <summary>
    ''' Parse the MSConvert console output file to look for errors from MzRefiner
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseMSConvertConsoleOutputfile(ByVal strConsoleOutputFilePath As String)

        ' Example console output
        '
        ' format: mzML 
        '     m/z: Compression-None, 32-bit
        '     intensity: Compression-None, 32-bit
        '     rt: Compression-None, 32-bit
        ' ByteOrder_LittleEndian
        '  indexed="true"
        ' outputPath: .
        ' extension: .mzML
        ' contactFilename: 
        ' filters:
        '   mzRefiner E:\DMS_WorkDir\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06_msgfplus.mzid thresholdValue=-1e-10 thresholdStep=10 maxSteps=3
        '   
        ' filenames:
        '   E:\DMS_WorkDir\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06.mzML
        '   
        ' processing file: E:\DMS_WorkDir\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06.mzML
        ' Reading file "E:\DMS_WorkDir\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06_msgfplus.mzid"...
        ' Adjusted filters: 
        ' 	Old: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-010
        ' 	New: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-009
        ' Adjusted filters: 
        ' 	Old: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-009
        ' 	New: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-008
        ' Adjusted filters: 
        ' 	Old: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-008
        ' 	New: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-007
        ' 	Filtered out 13014 identifications because of score.
        ' 	Filtered out 128 identifications because of mass error.
        ' 	Good data points:                                 839
        ' 	Average: global ppm Errors:                       0.77106
        ' 	Systematic Drift (mode):                          -11.5
        ' 	Systematic Drift (median):                        -11.5
        ' 	Measurement Precision (stdev ppm):                26.0205
        ' 	Measurement Precision (stdev(mode) ppm):          28.7674
        ' 	Measurement Precision (stdev(median) ppm):        28.7674
        ' 	Average BinWise stdev (scan):                     24.6157
        ' 	Expected % Improvement (scan):                    5.39302
        ' 	Expected % Improvement (scan)(mode):              14.4319
        ' 	Expected % Improvement (scan)(median):            5.39907
        ' 	Average BinWise stdev (smoothed scan):            25.4906
        ' 	Expected % Improvement (smoothed scan):           2.03045
        ' 	Expected % Improvement (smoothed scan)(mode):     11.3906
        ' 	Expected % Improvement (smoothed scan)(median):   2.03672
        ' 	Average BinWise stdev (mz):                       23.4562
        ' 	Expected % Improvement (mz):                      9.84931
        ' 	Expected % Improvement (mz)(mode):                18.4625
        ' 	Expected % Improvement (mz)(median):              9.85509
        ' 	Average BinWise stdev (smoothed mz):              25.5205
        ' 	Expected % Improvement (smoothed mz):             1.91564
        ' 	Expected % Improvement (smoothed mz)(mode):       11.2868
        ' 	Expected % Improvement (smoothed mz)(median):     1.92192
        ' Chose global shift...
        ' 	Estimated final stDev:                            26.0205
        ' 	Estimated tolerance for 99%: 0 +/-                78.0616
        ' writing output file: .\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06_FIXED.mzML


        ' Example warning for sparse data file
        ' Low number of good identifications found. Will not perform dependent shifts.
        '         Less than 500 (123) results after filtering.
        '         Filtered out 6830 identifications because of score.

        ' Example error for really sparse data file
        ' Excluding file ".\mzmlRefineryData\Cyanothece_bad\Cyano_GC_07_10_25Aug09_Draco_09-05-02.mzid" from data set
        '         Less than 100 (16) results after filtering.
        '         Filtered out 4208 identifications because of score.
        '         Filtered out 0 identifications because of mass error.

        Dim reResultsAfterFiltering = New Regex("Less than \d+ \(\d+\) results after filtering", RegexOptions.Compiled)

        Dim reGoodDataPoints = New Regex("Good data points:[^\d]+(\d+)", RegexOptions.Compiled)
        Dim reSpecEValueThreshold = New Regex("New: MS-GF:SpecEValue;.+value <= ([^ ]+)", RegexOptions.Compiled)

        Try
            If Not File.Exists(strConsoleOutputFilePath) Then
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
                End If

                Exit Sub
            End If

            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
            End If

            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While srInFile.Peek() >= 0
                    Dim strDataLine = srInFile.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strDataLine) Then

                        Dim strDataLineLCase = strDataLine.Trim().ToLower()

                        If strDataLineLCase.StartsWith("error:") OrElse strDataLineLCase.Contains("unhandled exception") Then
                            If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
                                mConsoleOutputErrorMsg = "Error running MzRefinery: " & strDataLine
                            Else
                                mConsoleOutputErrorMsg &= "; " & strDataLine
                            End If

                        ElseIf strDataLine.StartsWith("Chose ") Then
                            mMzRefineryCorrectionMode = String.Copy(strDataLine)
                        ElseIf strDataLine.StartsWith("Low number of good identifications found") Then
                            m_EvalMessage = strDataLine
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "MzRefinery warning: " & strDataLine)
                        ElseIf strDataLine.StartsWith("Excluding file") AndAlso strDataLine.EndsWith("from data set") Then
                            m_message = "Fewer than 100 matches after filtering; cannot use MzRefinery on this dataset"
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                        Else
                            Dim reMatch = reResultsAfterFiltering.Match(strDataLine)

                            If reMatch.Success Then
                                m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, strDataLine.Trim())
                            End If

                            reMatch = reGoodDataPoints.Match(strDataLine)
                            If reMatch.Success Then
                                Dim dataPoints As Integer
                                If Integer.TryParse(reMatch.Groups(1).Value, dataPoints) Then
                                    mMzRefinerGoodDataPoints = dataPoints
                                End If
                            End If

                            reMatch = reSpecEValueThreshold.Match(strDataLine)
                            If reMatch.Success Then
                                Dim specEValueThreshold As Double
                                If Double.TryParse(reMatch.Groups(1).Value, specEValueThreshold) Then
                                    mMzRefinerSpecEValueThreshold = specEValueThreshold
                                End If
                            End If
                        End If

                    End If
                Loop

            End Using

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing MzRefinery console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
            End If
        End Try

    End Sub

    Private Function PostProcessMzRefinerResults(ByVal fiMSGFPlusResults As FileInfo, ByVal fiFixedMzMLFile As FileInfo) As Boolean

        Dim strOriginalMzMLFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION)

        Try
            ' Create the plots
            Dim blnSuccess = StartPpmErrorCharter(fiMSGFPlusResults)

            If Not blnSuccess Then
                Return False
            End If

            If Not mSkipMzRefinery Then
                ' Store the PPM Mass Errors in the database
                blnSuccess = StorePPMErrorStatsInDB()
                If Not blnSuccess Then
                    Return False
                End If
            End If

        Catch ex As Exception
            m_message = "Error creating PPM Error charters"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Try

            If File.Exists(strOriginalMzMLFilePath) Then
                ' Delete the original .mzML file
                DeleteFileWithRetries(strOriginalMzMLFilePath, m_DebugLevel, 2)
            End If

        Catch ex As Exception
            m_message = "Error replacing the original .mzML file with the updated version; cannot delete original"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Try

            ' Rename the fixed mzML file
            fiFixedMzMLFile.MoveTo(strOriginalMzMLFilePath)

        Catch ex As Exception
            m_message = "Error replacing the original .mzML file with the updated version; cannot rename the fixed file"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Try

            ' Compress the .mzML file
            Dim blnSuccess = m_IonicZipTools.GZipFile(fiFixedMzMLFile.FullName, True)

            If Not blnSuccess Then
                m_message = m_IonicZipTools.Message
                Return False
            End If

        Catch ex As Exception
            m_message = "Error compressing the fixed .mzML file"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Try

            Dim fiMzRefFileGzipped = New FileInfo(fiFixedMzMLFile.FullName & clsAnalysisResources.DOT_GZ_EXTENSION)

            ' Copy the .mzML.gz file to the cache
            Dim remoteCachefilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, fiMzRefFileGzipped.FullName, purgeOldFilesIfNeeded:=True)

            If String.IsNullOrEmpty(remoteCachefilePath) Then
                If String.IsNullOrEmpty(m_message) Then
                    LogError("CopyFileToServerCache returned false for " & fiMzRefFileGzipped.Name)
                End If
                Return False
            End If

            ' Create the _CacheInfo.txt file
            Dim cacheInfoFilePath = fiMzRefFileGzipped.FullName & "_CacheInfo.txt"
            Using swOutFile = New StreamWriter(New FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swOutFile.WriteLine(remoteCachefilePath)
            End Using

            m_jobParams.AddResultFileToSkip(fiMzRefFileGzipped.Name)

        Catch ex As Exception
            m_message = "Error copying the .mzML.gz file to the remote cache folder"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        ' Compress the MSGF+ .mzID file
        Dim success = CompressMSGFPlusResults(fiMSGFPlusResults)

        Return success

    End Function

    Protected Function StartMzRefinery(ByVal fiOriginalMzMLFile As FileInfo, ByVal fiMSGFPlusResults As FileInfo) As Boolean

        mConsoleOutputErrorMsg = String.Empty

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MzRefinery using MSConvert")

        ' Set up and execute a program runner to run MSConvert
        ' Provide the path to the .mzML file plus the --filter switch with the information required to run MzRefiner

        Dim cmdStr = " "
        cmdStr &= fiOriginalMzMLFile.FullName
        cmdStr &= " --outfile " & Path.GetFileNameWithoutExtension(fiOriginalMzMLFile.Name) & "_FIXED.mzML"
        cmdStr &= " --filter ""mzRefiner " & fiMSGFPlusResults.FullName

        ' MzRefiner will perform a segmented correction if there are at least 500 matches; it will perform a global shift if between 100 and 500 matches
        ' The data is initially filtered by MSGF SpecProb <= 1e-10
        ' The reason that we prepend "1e-10" with a dash is to indicate a range of "-infinity to 1e-10"
        cmdStr &= " thresholdValue=-1e-10"

        ' If there are not 500 matches with 1e-10, then the threshold value is multiplied by the thresholdStep value
        ' This process is continued at most maxSteps times
        ' Thus, using 10 and 2 means the thresholds that will be considered are 1e-10, 1e-9, and 1e-8
        cmdStr &= " thresholdStep=10"
        cmdStr &= " maxSteps=2"""

        ' These switches assure that the output file is a 32-bit mzML file
        cmdStr &= " --32 --mzML"

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mMSConvertProgLoc & cmdStr)
        End If

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True

            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(m_WorkDir, MZ_REFINERY_CONSOLE_OUTPUT)
        End With

        m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE

        mRunningzRefinerWithMSConvert = True

        ' Start MSConvert and wait for it to exit
        Dim blnSuccess = CmdRunner.RunProgram(mMSConvertProgLoc, cmdStr, "MSConvert_MzRefinery", True)

        mRunningzRefinerWithMSConvert = False

        If Not CmdRunner.WriteConsoleOutputToFile Then
            ' Write the console output to a text file
            System.Threading.Thread.Sleep(250)

            Dim swConsoleOutputfile = New StreamWriter(New FileStream(CmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            swConsoleOutputfile.WriteLine(CmdRunner.CachedConsoleOutput)
            swConsoleOutputfile.Close()
        End If

        If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
        End If

        ' Parse the console output file one more time to check for errors and to make sure mMzRefineryCorrectionMode is up-to-date
        ' We will also extract out the final MS-GF:SpecEValue used for filtering the data
        Threading.Thread.Sleep(250)
        ParseMSConvertConsoleOutputfile(CmdRunner.ConsoleOutputFilePath)

        If Not String.IsNullOrEmpty(mMzRefineryCorrectionMode) Then

            Dim logMessage = "MzRefinery " & mMzRefineryCorrectionMode.Replace("...", "").TrimEnd("."c)
            logMessage &= ", " & mMzRefinerGoodDataPoints & " points had SpecEValue <= " & mMzRefinerSpecEValueThreshold.ToString("0.###E+00")

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, logMessage)
        End If

        If Not String.IsNullOrWhiteSpace(CmdRunner.CachedConsoleErrors) Then
            Dim consoleError = "Console error: " & CmdRunner.CachedConsoleErrors.Replace(Environment.NewLine, "; ")            
            If String.IsNullOrWhiteSpace(mConsoleOutputErrorMsg) Then
                mConsoleOutputErrorMsg = consoleError
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, consoleError)
            End If
            blnSuccess = False
        End If

        If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
            If mConsoleOutputErrorMsg.Contains("No high-resolution data in input file") Then
                m_message = "No high-resolution data in input file; cannot use MzRefinery on this dataset"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                blnSuccess = False
            End If
        End If

        If Not blnSuccess Then
            Dim Msg As String
            Msg = "Error running MSConvert/MzRefinery"
            If String.IsNullOrEmpty(m_message) Then
                m_message = Msg
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

            If CmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSConvert/MzRefinery returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MSConvert/MzRefinery failed (but exit code is 0)")
            End If

            Return False

        End If

        m_progress = PROGRESS_PCT_MZREFINERY_COMPLETE
        m_StatusTools.UpdateAndWrite(m_progress)
        If m_DebugLevel >= 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MzRefinery Complete")
        End If

        Return True

    End Function

    Protected Function StartPpmErrorCharter(ByVal fiMSGFPlusResults As FileInfo) As Boolean

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running PPMErrorCharter")

        ' Set up and execute a program runner to run the PPMErrorCharter
        Dim cmdStr = " " & fiMSGFPlusResults.FullName & " " & mMzRefinerSpecEValueThreshold.ToString("0.###E+00")

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mPpmErrorCharterProgLoc & cmdStr)
        End If

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(m_WorkDir, ERROR_CHARTER_CONSOLE_OUTPUT_FILE)
        End With

        ' Start the PPM Error Chararter and wait for it to exit
        Dim blnSuccess = CmdRunner.RunProgram(mPpmErrorCharterProgLoc, cmdStr, "PPMErrorCharter", True)

        If Not blnSuccess Then
            Dim Msg As String
            Msg = "Error running PPMErrorCharter"
            m_message = clsGlobal.AppendToComment(m_message, Msg)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

            If CmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "PPMErrorCharter returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to PPMErrorCharter failed (but exit code is 0)")
            End If

            Return False

        End If

        ' Make sure the plots were created
        Dim lstCharts = New List(Of FileInfo)

        lstCharts.Add(New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_MZRefinery_MassErrors.png")))
        lstCharts.Add(New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_MZRefinery_Histograms.png")))

        For Each fiChart In lstCharts
            If Not fiChart.Exists Then
                m_message = "PPMError chart not found: " & fiChart.Name
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If
        Next

        m_progress = PROGRESS_PCT_PLOTS_GENERATED
        m_StatusTools.UpdateAndWrite(m_progress)
        If m_DebugLevel >= 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "PPMErrorCharter Complete")
        End If

        Return True

    End Function

    Private Function StorePPMErrorStatsInDB() As Boolean

        Dim oMassErrorExtractor = New clsMzRefineryMassErrorStatsExtractor(m_mgrParams, m_WorkDir, m_DebugLevel, blnPostResultsToDB:=True)
        Dim blnSuccess As Boolean

        Dim intDatasetID As Integer = m_jobParams.GetJobParameter("DatasetID", 0)
        Dim intJob As Integer
        Integer.TryParse(m_JobNum, intJob)

        Dim consoleOutputFilePath = Path.Combine(m_WorkDir, ERROR_CHARTER_CONSOLE_OUTPUT_FILE)
        blnSuccess = oMassErrorExtractor.ParsePPMErrorCharterOutput(m_Dataset, intDatasetID, intJob, consoleOutputFilePath)

        If Not blnSuccess Then
            If String.IsNullOrEmpty(oMassErrorExtractor.ErrorMessage) Then
                m_message = "Error parsing PMM Error Charter output to extract mass error stats"
            Else
                m_message = oMassErrorExtractor.ErrorMessage
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
        End If

        m_jobParams.AddResultFileToSkip(ERROR_CHARTER_CONSOLE_OUTPUT_FILE)

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        strToolVersionInfo = String.Copy(mMSGFDBUtils.MSGFDbVersion)

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(New FileInfo(mMSGFDbProgLoc))
        ioToolFiles.Add(New FileInfo(mMSConvertProgLoc))
        ioToolFiles.Add(New FileInfo(mPpmErrorCharterProgLoc))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Private Sub UpdateProgress(
      ByVal currentTaskProgressAtStart As Single,
      ByVal currentTaskProgressAtEnd As Single,
      ByVal subTaskProgress As Single)

        Dim progressCompleteOverall = ComputeIncrementalProgress(currentTaskProgressAtStart, currentTaskProgressAtEnd, subTaskProgress)

        UpdateProgress(progressCompleteOverall)

    End Sub

    Private Sub UpdateProgress(ByVal progressCompleteOverall As Single)

        Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow

        If m_progress < progressCompleteOverall Then
            m_progress = progressCompleteOverall

            If m_DebugLevel >= 3 OrElse DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
                dtLastProgressWriteTime = DateTime.UtcNow
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "% complete")
            End If
        End If

    End Sub

    Private Sub UpdateStatusRunning(ByVal sngPercentComplete As Single)
        m_progress = sngPercentComplete
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
    End Sub

#End Region

#Region "Event Handlers"

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting

		MonitorProgress()

	End Sub

	Private Sub mMSGFDBUtils_ErrorEvent(ByVal errorMessage As String, ByVal detailedMessage As String) Handles mMSGFDBUtils.ErrorEvent
		m_message = String.Copy(errorMessage)
		If String.IsNullOrEmpty(detailedMessage) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
		Else
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, detailedMessage)
		End If

	End Sub

	Private Sub mMSGFDBUtils_IgnorePreviousErrorEvent() Handles mMSGFDBUtils.IgnorePreviousErrorEvent
		m_message = String.Empty
	End Sub

	Private Sub mMSGFDBUtils_MessageEvent(messageText As String) Handles mMSGFDBUtils.MessageEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, messageText)
	End Sub

	Private Sub mMSGFDBUtils_WarningEvent(warningMessage As String) Handles mMSGFDBUtils.WarningEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)
	End Sub

#End Region

End Class
