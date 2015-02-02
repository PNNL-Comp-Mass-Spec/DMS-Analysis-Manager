'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 01/30/2015
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Runtime.InteropServices

Public Class clsAnalysisToolRunnerProMex
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running ProMex to deisotope high resolution spectra
    '*********************************************************************************************************

#Region "Constants and Enums"
    Protected Const PROMEX_CONSOLE_OUTPUT As String = "ProMex_ConsoleOutput.txt"

    Protected Const PROGRESS_PCT_STARTING As Single = 1
    Protected Const PROGRESS_PCT_COMPLETE As Single = 99

#End Region

#Region "Module Variables"

    Protected mConsoleOutputErrorMsg As String

    Protected mProMexResultsFilePath As String

    Protected WithEvents mCmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs ProMex
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerProMex.RunTool(): Enter")
            End If

            ' Determine the path to the ProMex program
            Dim progLoc As String
            progLoc = DetermineProgramLocation("ProMex", "ProMexProgLoc", "ProMex.exe")

            If String.IsNullOrWhiteSpace(progLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Store the ProMex version info in the database
            If Not StoreToolVersionInfo(progLoc) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
                m_message = "Error determining ProMex version"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Run ProMex
            Dim blnSuccess = StartProMex(progLoc)

            If blnSuccess Then
                ' Look for the results file

                Dim fiResultsFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MS1FT_EXTENSION))

                If fiResultsFile.Exists Then
                    blnSuccess = PostProcessProMexResults(fiResultsFile)
                    If Not blnSuccess Then
                        If String.IsNullOrEmpty(m_message) Then
                            m_message = "Unknown error post-processing the ProMex results"
                        End If
                    End If

                Else
                    If String.IsNullOrEmpty(m_message) Then
                        m_message = "ProMex results file not found: " & fiResultsFile.Name
                        blnSuccess = False
                    End If
                End If
            End If

            m_progress = PROGRESS_PCT_COMPLETE

            'Stop the job timer
            m_StopTime = DateTime.UtcNow

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            mCmdRunner = Nothing

            'Make sure objects are released
            Threading.Thread.Sleep(2000)        '2 second delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If Not blnSuccess Then
                ' Move the source files and any results to the Failed Job folder
                ' Useful for debugging problems
                CopyFailedResultsToArchiveFolder()
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

        Catch ex As Exception
            m_message = "Error in ProMexPlugin->RunTool"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the .mzXML file first)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        Try
            File.Delete(Path.Combine(m_WorkDir, m_Dataset & ".mzXML"))
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

    Protected Function GetProMexParameterNames() As Dictionary(Of String, String)

        Dim dctParamNames = New Dictionary(Of String, String)(25, StringComparer.CurrentCultureIgnoreCase)

        dctParamNames.Add("MinCharge", "minCharge")
        dctParamNames.Add("MaxCharge", "maxCharge")

        dctParamNames.Add("MinMass", "minMass")
        dctParamNames.Add("MaxMass", "maxMass")

        dctParamNames.Add("MassCollapse", "massCollapse")
        dctParamNames.Add("Score", "score")
        dctParamNames.Add("Csv", "csv")
        dctParamNames.Add("MinProbability", "minProbability")
        dctParamNames.Add("MaxThreads", "maxThreads")

        Return dctParamNames

    End Function

    ''' <summary>
    ''' Parse the ProMex console output file to track the search progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

        ' Example Console output
        '
        ' ****** ProMex   ver. 1.0 (Jan 29, 2014) ************
        ' -i      CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ.pbf
        ' -minCharge      2
        ' -maxCharge      60
        ' -minMass        3000.0
        ' -maxMass        50000.0
        ' -massCollapse   n
        ' -score  n
        ' -csv    y
        ' -minProbability 0.1
        ' -maxThreads     0
        ' Start loading MS1 data from CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ.pbf
        ' Complete loading MS1 data. Elapsed Time = 17.514 sec
        ' Start MS1 feature extracting...
        ' Mass Range 3000 - 50000
        ' Charge Range 2 - 60
        ' Output File     CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ.ms1ft
        ' Csv Output File CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ_ms1ft.csv
        ' Processing 2.25 % of mass bins (3187.563 Da); Elapsed Time = 16.035 sec; # of features = 283
        ' Processing 4.51 % of mass bins (3375.063 Da); Elapsed Time = 26.839 sec; # of features = 770
        ' Processing 6.76 % of mass bins (3562.563 Da); Elapsed Time = 40.169 sec; # of features = 1426
        ' Processing 9.02 % of mass bins (3750.063 Da); Elapsed Time = 51.633 sec; # of features = 2154

        Const REGEX_ProMex_PROGRESS As String = "Processing ([0-9.]+) \%"
        Static reCheckProgress As New Regex(REGEX_ProMex_PROGRESS, RegexOptions.Compiled Or RegexOptions.IgnoreCase)
        Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow

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

            ' Value between 0 and 100
            Dim progressComplete As Single = 0

            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While Not srInFile.EndOfStream
                    Dim strLineIn = srInFile.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then

                        Dim strLineInLCase = strLineIn.ToLower()

                        If strLineInLCase.StartsWith("error:") OrElse strLineInLCase.Contains("unhandled exception") Then
                            If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
                                mConsoleOutputErrorMsg = "Error running ProMex:"
                            End If
                            mConsoleOutputErrorMsg &= "; " & strLineIn
                            Continue Do

                        Else
                            Dim oMatch As Match = reCheckProgress.Match(strLineIn)
                            If oMatch.Success Then
                                Single.TryParse(oMatch.Groups(1).ToString(), progressComplete)
                                Continue Do
                            End If

                        End If

                    End If
                Loop

            End Using

            If m_progress < progressComplete OrElse DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 60 Then
                m_progress = progressComplete

                If m_DebugLevel >= 3 OrElse DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
                    dtLastProgressWriteTime = DateTime.UtcNow
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "% complete")
                End If
            End If

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
            End If
        End Try

    End Sub

    ''' <summary>
    ''' Read the ProMex options file and convert the options to command line switches
    ''' </summary>
    ''' <param name="strCmdLineOptions">Output: MSGFDb command line arguments</param>
    ''' <returns>Options string if success; empty string if an error</returns>
    ''' <remarks></remarks>
    Public Function ParseProMexParameterFile(<Out()> ByRef strCmdLineOptions As String) As IJobParams.CloseOutType

        strCmdLineOptions = String.Empty

        Dim strParameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("ProMexParamFile"))

        If Not File.Exists(strParameterFilePath) Then
            LogError("Parameter file not found", "Parameter file not found: " & strParameterFilePath)
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If

        Dim sbOptions = New Text.StringBuilder(500)

        Try

            ' Initialize the Param Name dictionary
            Dim dctParamNames = GetProMexParameterNames()

            Using srParamFile = New StreamReader(New FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                Do While Not srParamFile.EndOfStream
                    Dim strLineIn = srParamFile.ReadLine()

                    Dim kvSetting = clsGlobal.GetKeyValueSetting(strLineIn)

                    If Not String.IsNullOrWhiteSpace(kvSetting.Key) Then

                        Dim strValue As String = kvSetting.Value

                        Dim strArgumentSwitch As String = String.Empty

                        ' Check whether kvSetting.key is one of the standard keys defined in dctParamNames
                        If dctParamNames.TryGetValue(kvSetting.Key, strArgumentSwitch) Then

                            sbOptions.Append(" -" & strArgumentSwitch & " " & strValue)

                        End If

                    End If
                Loop

            End Using

        Catch ex As Exception
            m_message = "Exception reading ProMex parameter file"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        strCmdLineOptions = sbOptions.ToString()

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Validates that the modification definition text
    ''' </summary>
    ''' <param name="strMod">Modification definition</param>
    ''' <param name="strModClean">Cleaned-up modification definition (output param)</param>
    ''' <returns>True if valid; false if invalid</returns>
    ''' <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
    Protected Function ParseProMexValidateMod(ByVal strMod As String, <Out()> ByRef strModClean As String) As Boolean

        Dim intPoundIndex As Integer
        Dim strSplitMod() As String

        Dim strComment As String = String.Empty

        strModClean = String.Empty

        intPoundIndex = strMod.IndexOf("#"c)
        If intPoundIndex > 0 Then
            strComment = strMod.Substring(intPoundIndex)
            strMod = strMod.Substring(0, intPoundIndex - 1).Trim
        End If

        strSplitMod = strMod.Split(","c)

        If strSplitMod.Length < 5 Then
            ' Invalid mod definition; must have 5 sections
            LogError("Invalid modification string; must have 5 sections: " & strMod)
            Return False
        End If

        ' Make sure mod does not have both * and any
        If strSplitMod(1).Trim() = "*" AndAlso strSplitMod(3).ToLower().Trim() = "any" Then
            LogError("Modification cannot contain both * and any: " & strMod)
            Return False
        End If

        ' Reconstruct the mod definition, making sure there is no whitespace
        strModClean = strSplitMod(0).Trim()
        For intIndex As Integer = 1 To strSplitMod.Length - 1
            strModClean &= "," & strSplitMod(intIndex).Trim()
        Next

        If Not String.IsNullOrWhiteSpace(strComment) Then
            ' As of August 12, 2011, the comment cannot contain a comma
            ' Sangtae Kim has promised to fix this, but for now, we'll replace commas with semicolons
            strComment = strComment.Replace(",", ";")
            strModClean &= "     " & strComment
        End If

        Return True

    End Function

    Private Function PostProcessProMexResults(ByVal fiResultsFile As FileInfo) As Boolean

        ' Make sure there are at least two features in the .ms1ft file

        Try
            Using resultsReader = New System.IO.StreamReader(New FileStream(fiResultsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                Dim lineCount = 0
                While Not resultsReader.EndOfStream
                    Dim lineIn = resultsReader.ReadLine()
                    If Not String.IsNullOrEmpty(lineIn) Then
                        lineCount += 1
                        If lineCount > 2 Then
                            Return True
                        End If
                    End If
                End While
            End Using

            Return False

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception examinin the ms1ft file: " & ex.Message)
            Return False
        End Try

    End Function

    Protected Function StartProMex(ByVal progLoc As String) As Boolean

        Dim CmdStr As String
        Dim blnSuccess As Boolean

        mConsoleOutputErrorMsg = String.Empty

        ' Read the ProMex Parameter File
        ' The parameter file name specifies the mass modifications to consider, plus also the analysis parameters

        Dim strCmdLineOptions As String = String.Empty

        Dim eResult = ParseProMexParameterFile(strCmdLineOptions)

        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return False
        ElseIf String.IsNullOrEmpty(strCmdLineOptions) Then
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Problem parsing ProMex parameter file"
            End If
            Return False
        End If

        Dim pbfFilePath As String = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_PBF_EXTENSION)

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running ProMex")

        'Set up and execute a program runner to run ProMex
        ' Prior to January 2015 ProMex read PBF files; it now reads .ms1ft files from ProMex

        CmdStr = " -i " & pbfFilePath
        CmdStr &= " " & strCmdLineOptions

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & CmdStr)
        End If

        mCmdRunner = New clsRunDosProgram(m_WorkDir)

        With mCmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(m_WorkDir, PROMEX_CONSOLE_OUTPUT)
        End With

        m_progress = PROGRESS_PCT_STARTING

        blnSuccess = mCmdRunner.RunProgram(progLoc, CmdStr, "ProMex", True)

        If Not mCmdRunner.WriteConsoleOutputToFile Then
            ' Write the console output to a text file
            System.Threading.Thread.Sleep(250)

            Dim swConsoleOutputfile = New StreamWriter(New FileStream(mCmdRunner.ConsoleOutputFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))
            swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput)
            swConsoleOutputfile.Close()
        End If

        If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
        End If

        ' Parse the console output file one more time to check for errors
        System.Threading.Thread.Sleep(250)
        ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath)

        If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
        End If

        If Not blnSuccess Then
            Dim Msg As String
            Msg = "Error running ProMex"
            m_message = clsGlobal.AppendToComment(m_message, Msg)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

            If mCmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "ProMex returned a non-zero exit code: " & mCmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to ProMex failed (but exit code is 0)")
            End If

            Return False

        End If

        m_progress = PROGRESS_PCT_COMPLETE
        m_StatusTools.UpdateAndWrite(m_progress)
        If m_DebugLevel >= 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "ProMex Search Complete")
        End If

        Return True

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
        blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, fiProgram.FullName)
        If Not blnSuccess Then Return False


        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles = New List(Of FileInfo)
        ioToolFiles.Add(fiProgram)

        ioToolFiles.Add(New FileInfo(Path.Combine(fiProgram.Directory.FullName, "InformedProteomics.Backend.dll")))

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

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles mCmdRunner.LoopWaiting
        Static dtLastStatusUpdate As DateTime = DateTime.UtcNow
        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        ' Update the status file (limit the updates to every 5 seconds)
        If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = DateTime.UtcNow
            UpdateStatusRunning(m_progress)
        End If

        ' Parse the console output file every 15 seconds
        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, ProMex_CONSOLE_OUTPUT))

        End If

    End Sub

#End Region

End Class
