Option Strict On

'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 09/14/2006
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO
Imports System.Collections.Generic
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerDecon2ls
    Inherits clsAnalysisToolRunnerBase

#Region "Constants"
    Private Const DECON2LS_SCANS_FILE_SUFFIX As String = "_scans.csv"
    Private Const DECON2LS_ISOS_FILE_SUFFIX As String = "_isos.csv"
    Private Const DECON2LS_PEAKS_FILE_SUFFIX As String = "_peaks.txt"

#End Region

#Region "Module variables"

    Private mRawDataType As clsAnalysisResources.eRawDataTypeConstants = clsAnalysisResources.eRawDataTypeConstants.Unknown
    Private mRawDataTypeName As String = String.Empty

    Private mInputFilePath As String = String.Empty

    Private mDeconConsoleBuild As Integer = 0

    Private mDeconToolsExceptionThrown As Boolean
    Private mDeconToolsFinishedDespiteProgRunnerError As Boolean

    Private mDeconToolsStatus As udtDeconToolsStatusType

    Private mCmdRunner As clsRunDosProgram

#End Region

#Region "Enums and Structures"

    Private Enum DeconToolsStateType
        Idle = 0
        Running = 1
        Complete = 2
        ErrorCode = 3
        BadErrorLogFile = 4
    End Enum

    Private Enum DeconToolsFileTypeConstants
        Undefined = 0
        Agilent_WIFF = 1
        Agilent_D = 2
        Ascii = 3
        Bruker = 4
        Bruker_Ascii = 5
        Finnigan = 6
        ICR2LS_Rawdata = 7
        Micromass_Rawdata = 8
        MZXML_Rawdata = 9
        PNNL_IMS = 10
        PNNL_UIMF = 11
        SUNEXTREL = 12
    End Enum

    Private Structure udtDeconToolsStatusType
        Public CurrentLCScan As Integer     ' LC Scan number or IMS Frame Number
        Public PercentComplete As Single
        Public IsUIMF As Boolean
        Public Sub Clear()
            CurrentLCScan = 0
            PercentComplete = 0
            IsUIMF = False
        End Sub

    End Structure
#End Region

#Region "Methods"
    Public Sub New()
    End Sub

    ''' <summary>
    ''' Validate the result files
    ''' (legacy code would assemble result files from looping, but that code has been removed)
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function AssembleResults(oDeconToolsParamFileReader As clsXMLParamFileReader) As CloseOutType

        Dim ScansFilePath As String
        Dim IsosFilePath As String
        Dim PeaksFilePath As String
        Dim blnDotDFolder = False

        Try

            ScansFilePath = Path.Combine(m_WorkDir, m_Dataset & DECON2LS_SCANS_FILE_SUFFIX)
            IsosFilePath = Path.Combine(m_WorkDir, m_Dataset & DECON2LS_ISOS_FILE_SUFFIX)
            PeaksFilePath = Path.Combine(m_WorkDir, m_Dataset & DECON2LS_PEAKS_FILE_SUFFIX)

            Select Case mRawDataType
                Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf
                    ' As of 11/19/2010, the Decon2LS output files are created inside the .D folder
                    ' Still true as of 5/18/2012
                    blnDotDFolder = True
                Case Else
                    If Not File.Exists(IsosFilePath) And Not File.Exists(ScansFilePath) Then
                        If mInputFilePath.ToLower().EndsWith(".d") Then
                            blnDotDFolder = True
                        End If
                    End If
            End Select

            If blnDotDFolder AndAlso Not File.Exists(IsosFilePath) AndAlso Not File.Exists(ScansFilePath) Then
                ' Copy the files from the .D folder to the work directory

                Dim fiSrcFilePath As FileInfo

                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying Decon2LS result files from the .D folder to the working directory")
                End If

                fiSrcFilePath = New FileInfo(Path.Combine(mInputFilePath, m_Dataset & DECON2LS_SCANS_FILE_SUFFIX))
                If fiSrcFilePath.Exists Then
                    fiSrcFilePath.CopyTo(ScansFilePath)
                End If

                fiSrcFilePath = New FileInfo(Path.Combine(mInputFilePath, m_Dataset & DECON2LS_ISOS_FILE_SUFFIX))
                If fiSrcFilePath.Exists Then
                    fiSrcFilePath.CopyTo(IsosFilePath)
                End If

                fiSrcFilePath = New FileInfo(Path.Combine(mInputFilePath, m_Dataset & DECON2LS_PEAKS_FILE_SUFFIX))
                If fiSrcFilePath.Exists Then
                    fiSrcFilePath.CopyTo(PeaksFilePath)
                End If

            End If

            m_jobParams.AddResultFileToKeep(ScansFilePath)
            m_jobParams.AddResultFileToKeep(IsosFilePath)

            Dim blnWritePeaksToTextFile As Boolean = oDeconToolsParamFileReader.GetParameter("WritePeaksToTextFile", False)

            ' Examine the Peaks File to check whether it only has a header line, or it has multiple data lines
            If Not ResultsFileHasData(PeaksFilePath) Then
                ' The file does not have any data lines
                ' Raise an error if it should have had data
                If blnWritePeaksToTextFile Then
                    m_EvalMessage = "Warning: no results in DeconTools Peaks.txt file"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
                Else
                    ' Superfluous file; delete it
                    Try
                        If File.Exists(PeaksFilePath) Then
                            File.Delete(PeaksFilePath)
                        End If
                    Catch ex As Exception
                        ' Ignore errors here
                    End Try
                End If
            End If

            Dim strDeconvolutionType As String = oDeconToolsParamFileReader.GetParameter("DeconvolutionType", String.Empty)
            Dim blnEmptyIsosFileExpected As Boolean

            If strDeconvolutionType = "None" Then
                blnEmptyIsosFileExpected = True
            End If

            If blnEmptyIsosFileExpected Then
                ' The _isos.csv file should be empty; delete it
                If Not ResultsFileHasData(IsosFilePath) Then
                    ' The file does not have any data lines
                    Try
                        If File.Exists(IsosFilePath) Then
                            File.Delete(IsosFilePath)
                        End If
                    Catch ex As Exception
                        ' Ignore errors here
                    End Try
                End If

            Else
                ' Make sure the Isos File exists
                If Not File.Exists(IsosFilePath) Then
                    m_message = "DeconTools Isos file Not Found"
                    LogError(m_message, m_message & ": " & IsosFilePath)
                    Return CloseOutType.CLOSEOUT_NO_OUT_FILES
                End If

                ' Make sure the Isos file contains at least one row of data
                If Not IsosFileHasData(IsosFilePath) Then
                    LogError("No results in DeconTools Isos file")
                    Return CloseOutType.CLOSEOUT_NO_DATA
                End If

            End If


        Catch ex As Exception
            LogError("AssembleResults error", "clsAnalysisToolRunnerDecon2lsBase.AssembleResults, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & ": " & ex.Message)
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function CacheDeconToolsParamFile(strParamFilePath As String) As clsXMLParamFileReader

        Dim oDeconToolsParamFileReader As clsXMLParamFileReader

        Try
            oDeconToolsParamFileReader = New clsXMLParamFileReader(strParamFilePath)

            If oDeconToolsParamFileReader.ParameterCount = 0 Then
                LogError("DeconTools parameter file is empty (or could not be parsed)")
                Return Nothing
            End If

        Catch ex As Exception
            LogError("Error parsing parameter file", ex)
            Return Nothing
        End Try

        Return oDeconToolsParamFileReader

    End Function

    ''' <summary>
    ''' Use MSFileInfoScanner to create QC Plots
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function CreateQCPlots() As CloseOutType

        Dim blnSuccess As Boolean

        Try

            Dim strInputFilePath = Path.Combine(m_WorkDir, m_Dataset & DECON2LS_ISOS_FILE_SUFFIX)
            If Not File.Exists(strInputFilePath) Then
                ' Do not treat this as a fatal error
                ' It's possible that this analysis job used a parameter file that only picks peaks but doesn't deisotope, e.g. PeakPicking_NonThresholded_PeakBR2_SN7.xml
                Return CloseOutType.CLOSEOUT_SUCCESS
            End If

            Dim strMSFileInfoScannerDir = m_mgrParams.GetParam("MSFileInfoScannerDir")
            If String.IsNullOrEmpty(strMSFileInfoScannerDir) Then
                m_message = "Manager parameter 'MSFileInfoScannerDir' is not defined"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CreateQCPlots: " + m_message)
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            Dim strMSFileInfoScannerDLLPath = Path.Combine(strMSFileInfoScannerDir, "MSFileInfoScanner.dll")
            If Not File.Exists(strMSFileInfoScannerDLLPath) Then
                m_message = "File Not Found: " + strMSFileInfoScannerDLLPath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CreateQCPlots: " + m_message)
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            Dim objQCPlotGenerator = New clsDeconToolsQCPlotsGenerator(strMSFileInfoScannerDLLPath, m_DebugLevel)

            ' Create the QC Plot .png files and associated Index.html file
            blnSuccess = objQCPlotGenerator.CreateQCPlots(strInputFilePath, m_WorkDir)

            If blnSuccess Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Generated QC Plots file using " + strInputFilePath)
                End If

            Else
                m_message = "Error generating QC Plots files with clsDeconToolsQCPlotsGenerator"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, objQCPlotGenerator.ErrorMessage)
                If objQCPlotGenerator.MSFileInfoScannerErrorCount > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSFileInfoScanner encountered " + objQCPlotGenerator.MSFileInfoScannerErrorCount.ToString() + " errors")
                End If
            End If

        Catch ex As Exception
            LogError("Error in CreateQCPlots", ex)
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        If blnSuccess Then
            Return CloseOutType.CLOSEOUT_SUCCESS
        Else
            Return (CloseOutType.CLOSEOUT_FAILED)
        End If


    End Function

    ''' <summary>
    ''' Examines IsosFilePath to look for data lines (does not read the entire file, just the first two lines)
    ''' </summary>
    ''' <param name="IsosFilePath"></param>
    ''' <returns>True if it has one or more lines of data, otherwise, returns False</returns>
    ''' <remarks></remarks>
    Private Function IsosFileHasData(IsosFilePath As String) As Boolean
        Dim intDataLineCount As Integer
        Return IsosFileHasData(IsosFilePath, intDataLineCount, False)
    End Function

    ''' <summary>
    ''' Examines IsosFilePath to look for data lines 
    ''' </summary>
    ''' <param name="IsosFilePath"></param>
    ''' <param name="intDataLineCount">Output parameter: total data line count</param>
    ''' <param name="blnCountTotalDataLines">True to count all of the data lines; false to just look for the first data line</param>
    ''' <returns>True if it has one or more lines of data, otherwise, returns False</returns>
    ''' <remarks></remarks>
    Private Function IsosFileHasData(IsosFilePath As String, ByRef intDataLineCount As Integer, blnCountTotalDataLines As Boolean) As Boolean

        Dim srInFile As StreamReader

        Dim strLineIn As String
        Dim blnHeaderLineProcessed As Boolean

        intDataLineCount = 0
        blnHeaderLineProcessed = False

        Try

            If File.Exists(IsosFilePath) Then
                srInFile = New StreamReader(New FileStream(IsosFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine

                    If String.IsNullOrEmpty(strLineIn) Then Continue Do

                    If blnHeaderLineProcessed Then
                        ' This is a data line
                        If blnCountTotalDataLines Then
                            intDataLineCount += 1
                        Else
                            intDataLineCount = 1
                            Exit Do
                        End If

                    Else
                        blnHeaderLineProcessed = True
                    End If

                Loop

                srInFile.Close()
            End If

        Catch ex As Exception
            ' Ignore errors here
        End Try

        If intDataLineCount > 0 Then
            Return True
        Else
            Return False
        End If

    End Function

    ''' <summary>
    ''' Runs the Decon2LS analysis tool. The actual tool version details (deconvolute or TIC) will be handled by a subclass
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As CloseOutType

        Dim eResult As CloseOutType
        Dim eReturnCode As CloseOutType
        Dim errorMessage As String = Nothing

        'Do the base class stuff
        If Not MyBase.RunTool = CloseOutType.CLOSEOUT_SUCCESS Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2ls.RunTool(): Enter")
        End If

        mRawDataTypeName = clsAnalysisResources.GetRawDataTypeName(m_jobParams, errorMessage)

        If String.IsNullOrWhiteSpace(mRawDataTypeName) Then
            If String.IsNullOrWhiteSpace(errorMessage) Then
                LogError("Unable to determine the instrument data type using GetRawDataTypeName")
            Else
                LogError(errorMessage)
            End If

            Return CloseOutType.CLOSEOUT_FAILED
        End If

        mRawDataType = clsAnalysisResources.GetRawDataType(mRawDataTypeName)

        ' Set this to success for now
        eReturnCode = CloseOutType.CLOSEOUT_SUCCESS

        ' Run Decon2LS
        eResult = RunDecon2Ls()
        If eResult <> CloseOutType.CLOSEOUT_SUCCESS Then
            ' Something went wrong
            ' In order to help diagnose things, we will move whatever files were created into the eResult folder, 
            '  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Error running Decon2LS"
            End If

            If eResult = CloseOutType.CLOSEOUT_NO_DATA Then
                eReturnCode = eResult
                If String.IsNullOrWhiteSpace(m_message) Then
                    m_message = "No results in DeconTools Isos file"
                End If
            Else
                eReturnCode = CloseOutType.CLOSEOUT_FAILED
            End If

        End If

        If eResult = CloseOutType.CLOSEOUT_SUCCESS Then
            ' Create the QC plots
            eReturnCode = CreateQCPlots()
        End If

        If m_jobParams.GetJobParameter(clsAnalysisResourcesDecon2ls.JOB_PARAM_PROCESSMSMS_AUTO_ENABLED, False) Then
            m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, "Note: auto-enabled ProcessMSMS in the parameter file")
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_EvalMessage)
        End If

        ' Zip the _Peaks.txt file (if it exists)
        ZipPeaksFile()

        ' Delete the raw data files
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Deleting raw data file")
        End If

        Dim messageSaved = String.Copy(m_message)

        If DeleteRawDataFiles(mRawDataType) <> CloseOutType.CLOSEOUT_SUCCESS Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Problem deleting raw data files: " & m_message)
            ' Don't treat this as a critical error; leave eReturnCode unchanged and restore m_message
            If Not clsGlobal.IsMatch(m_message, messageSaved) Then
                m_message = messageSaved
            End If
        End If

        ' Update the job summary file
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Updating summary file")
        End If
        UpdateSummaryFile()

        ' Make the results folder
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Making results folder")
        End If

        eResult = MakeResultsFolder()
        If eResult <> CloseOutType.CLOSEOUT_SUCCESS Then
            ' MakeResultsFolder handles posting to local log, so set database error message and exit
            m_message = "Error making results folder"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        eResult = MoveResultFiles()
        If eResult <> CloseOutType.CLOSEOUT_SUCCESS Then
            ' MoveResultFiles moves the eResult files to the eResult folder
            m_message = "Error moving files into results folder"
            eReturnCode = CloseOutType.CLOSEOUT_FAILED
        End If

        If eReturnCode = CloseOutType.CLOSEOUT_FAILED Then
            ' Try to save whatever files were moved into the results folder
            Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
            objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName))

            Return CloseOutType.CLOSEOUT_FAILED
        End If

        eResult = CopyResultsFolderToServer()
        If eResult <> CloseOutType.CLOSEOUT_SUCCESS Then
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return eResult
        End If

        ' If we get to here, return the return code
        Return eReturnCode

    End Function

    Private Function RunDecon2Ls() As CloseOutType

        Dim strParamFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("ParmFileName"))
        Dim blnDecon2LSError As Boolean

        ' Cache the parameters in the DeconTools parameter file

        Dim oDeconToolsParamFileReader As clsXMLParamFileReader
        oDeconToolsParamFileReader = CacheDeconToolsParamFile(strParamFilePath)

        If oDeconToolsParamFileReader Is Nothing Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Get file type of the raw data file
        Dim filetype As DeconToolsFileTypeConstants
        filetype = GetInputFileType(mRawDataType)

        If filetype = DeconToolsFileTypeConstants.Undefined Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specifed while getting file type: " & mRawDataType)
            m_message = "Invalid raw data type specified"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Specify Input file or folder
        mInputFilePath = GetInputFilePath(mRawDataType)
        If String.IsNullOrWhiteSpace(mInputFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specifed while input file name: " & mRawDataType)
            m_message = "Invalid raw data type specified"
            Return CloseOutType.CLOSEOUT_FAILED
        End If


        ' Determine the path to the DeconTools folder
        Dim progLoc As String
        progLoc = DetermineProgramLocation("DeconTools", "DeconToolsProgLoc", "DeconConsole.exe")

        If String.IsNullOrWhiteSpace(progLoc) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Store the DeconTools version info in the database
        m_message = String.Empty
        If Not StoreToolVersionInfo(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Error determining DeconTools version"
            End If
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Reset the log file tracking variables
        mDeconToolsExceptionThrown = False
        mDeconToolsFinishedDespiteProgRunnerError = False

        ' Reset the state variables
        mDeconToolsStatus.Clear()

        If filetype = DeconToolsFileTypeConstants.PNNL_UIMF Then
            mDeconToolsStatus.IsUIMF = True
        Else
            mDeconToolsStatus.IsUIMF = False
        End If

        ' Start Decon2LS and wait for it to finish
        Dim eDeconToolsStatus = StartDeconTools(progLoc, mInputFilePath, strParamFilePath, filetype)

        ' Stop the job timer
        m_StopTime = DateTime.UtcNow

        ' Make sure objects are released
        Threading.Thread.Sleep(1000)           '1 second delay
        PRISM.Processes.clsProgRunner.GarbageCollectNow()

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS finished")
        End If

        ' Determine reason for Decon2LS finish
        If mDeconToolsFinishedDespiteProgRunnerError And Not mDeconToolsExceptionThrown Then
            ' ProgRunner reported an error code
            ' However, the log file says things completed successfully
            ' We'll trust the log file
            blnDecon2LSError = False
        Else
            Select Case eDeconToolsStatus
                Case DeconToolsStateType.Complete
                    ' This is normal, do nothing else
                    blnDecon2LSError = False

                Case DeconToolsStateType.ErrorCode
                    m_message = "Decon2LS error"
                    blnDecon2LSError = True

                Case DeconToolsStateType.BadErrorLogFile
                    blnDecon2LSError = True

                    ' Sleep for 1 minute
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sleeping for 1 minute")
                    Threading.Thread.Sleep(60 * 1000)

                Case DeconToolsStateType.Idle
                    ' DeconTools never actually started
                    m_message = "Decon2LS error"
                    blnDecon2LSError = True

                Case DeconToolsStateType.Running
                    ' We probably shouldn't get here
                    ' But, we'll assume success
                    blnDecon2LSError = False
            End Select
        End If

        If Not blnDecon2LSError Then
            Dim eResult As CloseOutType
            eResult = AssembleResults(oDeconToolsParamFileReader)

            If eResult <> CloseOutType.CLOSEOUT_SUCCESS Then
                ' Check for no data first. If no data, then exit but still copy results to server
                If eResult = CloseOutType.CLOSEOUT_NO_DATA Then
                    Return eResult
                End If

                LogError("AssembleResults returned " & eResult.ToString)
                Return CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        If blnDecon2LSError Then
            Return CloseOutType.CLOSEOUT_FAILED
        Else
            Return CloseOutType.CLOSEOUT_SUCCESS
        End If


    End Function


    Private Function StartDeconTools(
      ProgLoc As String,
      strInputFilePath As String,
      strParamFilePath As String,
      eFileType As DeconToolsFileTypeConstants) As DeconToolsStateType

        Dim eDeconToolsStatus As DeconToolsStateType

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsDeIsotope.StartDeconTools(), Starting deconvolution")
        End If

        Try

            Dim CmdStr As String
            Dim strFileTypeText As String

            If eFileType = DeconToolsFileTypeConstants.Undefined Then
                m_message = "Undefined file type found in StartDeconTools"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return DeconToolsStateType.ErrorCode
            End If

            strFileTypeText = GetDeconFileTypeText(eFileType)

            ' Set up and execute a program runner to run DeconTools
            If mDeconConsoleBuild < 4400 Then
                CmdStr = strInputFilePath & " " & strFileTypeText & " " & strParamFilePath
            Else
                CmdStr = strInputFilePath & " " & strParamFilePath
            End If


            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, ProgLoc & " " & CmdStr)
            End If

            mCmdRunner = New clsRunDosProgram(m_WorkDir)
            RegisterEvents(mCmdRunner)
            AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

            With mCmdRunner
                .CreateNoWindow = True
                .CacheStandardOutput = True
                .EchoOutputToConsole = True

                ' We don't need to capture the console output since the DeconTools log file has very similar information
                .WriteConsoleOutputToFile = False
            End With

            eDeconToolsStatus = DeconToolsStateType.Running

            m_progress = 0
            ResetProgRunnerCpuUsage()

            ' Start the program and wait for it to finish
            ' However, while it's running, LoopWaiting will get called via events
            Dim success = mCmdRunner.RunProgram(ProgLoc, CmdStr, "DeconConsole", True)

            If Not success Then
                m_message = "Error running DeconTools"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
            End If

            ' Parse the DeconTools .Log file to see whether it contains message "Finished file processing"

            Dim dtFinishTime As DateTime
            Dim blnFinishedProcessing As Boolean

            ParseDeconToolsLogFile(blnFinishedProcessing, dtFinishTime)

            If mDeconToolsExceptionThrown Then
                eDeconToolsStatus = DeconToolsStateType.ErrorCode
            ElseIf success Then
                eDeconToolsStatus = DeconToolsStateType.Complete
            ElseIf blnFinishedProcessing Then
                mDeconToolsFinishedDespiteProgRunnerError = True
                eDeconToolsStatus = DeconToolsStateType.Complete
            Else
                eDeconToolsStatus = DeconToolsStateType.ErrorCode
            End If

            ' Look for file Dataset*BAD_ERROR_log.txt
            ' If it exists, an exception occurred
            Dim diWorkdir = New DirectoryInfo(Path.Combine(m_WorkDir))

            For Each fiFile As FileInfo In diWorkdir.GetFiles(m_Dataset & "*BAD_ERROR_log.txt")
                m_message = "Error running DeconTools; Bad_Error_log file exists"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & fiFile.Name)
                eDeconToolsStatus = DeconToolsStateType.BadErrorLogFile
                Exit For
            Next

        Catch ex As Exception
            LogError("Exception calling DeconConsole", ex)
            eDeconToolsStatus = DeconToolsStateType.ErrorCode
        End Try

        Return eDeconToolsStatus

    End Function


    Private Function GetDeconFileTypeText(eDeconFileType As DeconToolsFileTypeConstants) As String

        Select Case eDeconFileType
            Case DeconToolsFileTypeConstants.Agilent_WIFF : Return "Agilent_WIFF"
            Case DeconToolsFileTypeConstants.Agilent_D : Return "Agilent_D"
            Case DeconToolsFileTypeConstants.Ascii : Return "Ascii"
            Case DeconToolsFileTypeConstants.Bruker : Return "Bruker"
            Case DeconToolsFileTypeConstants.Bruker_Ascii : Return "Bruker_Ascii"
            Case DeconToolsFileTypeConstants.Finnigan : Return "Finnigan"
            Case DeconToolsFileTypeConstants.ICR2LS_Rawdata : Return "ICR2LS_Rawdata"
            Case DeconToolsFileTypeConstants.Micromass_Rawdata : Return "Micromass_Rawdata"
            Case DeconToolsFileTypeConstants.MZXML_Rawdata : Return "MZXML_Rawdata"
                ' Future: Case DeconToolsFileTypeConstants.MZML_Rawdata : Return "MZML_Rawdata"
            Case DeconToolsFileTypeConstants.PNNL_IMS : Return "PNNL_IMS"
            Case DeconToolsFileTypeConstants.PNNL_UIMF : Return "PNNL_UIMF"
            Case DeconToolsFileTypeConstants.SUNEXTREL : Return "SUNEXTREL"
            Case Else
                Return "Undefined"
        End Select

    End Function

    Private Function GetInputFileType(eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As DeconToolsFileTypeConstants

        Dim InstrumentClass As String = m_jobParams.GetParam("instClass")


        ' Gets the Decon2LS file type based on the input data type
        Select Case eRawDataType
            Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
                Return DeconToolsFileTypeConstants.Finnigan

            Case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile
                Return DeconToolsFileTypeConstants.Agilent_WIFF

            Case clsAnalysisResources.eRawDataTypeConstants.UIMF
                Return DeconToolsFileTypeConstants.PNNL_UIMF

            Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder
                Return DeconToolsFileTypeConstants.Agilent_D

            Case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder
                Return DeconToolsFileTypeConstants.Micromass_Rawdata

            Case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders
                If InstrumentClass.ToLower = "brukerftms" Then
                    ' Data from Bruker FTICR
                    Return DeconToolsFileTypeConstants.Bruker

                ElseIf InstrumentClass.ToLower = "finnigan_fticr" Then
                    ' Data from old Finnigan FTICR
                    Return DeconToolsFileTypeConstants.SUNEXTREL
                Else
                    ' Should never get here
                    Return DeconToolsFileTypeConstants.Undefined
                End If

            Case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf
                Return DeconToolsFileTypeConstants.Bruker

            Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot

                ' TODO: Add support for this after Decon2LS is updated
                ' Return DeconToolsFileTypeConstants.Bruker_15T

                LogError("Decon2LS_V2 does not yet support Bruker MALDI data (" & eRawDataType.ToString() & ")")
                Return DeconToolsFileTypeConstants.Undefined


            Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging

                ' TODO: Add support for this after Decon2LS is updated
                ' Return DeconToolsFileTypeConstants.Bruker_15T

                LogError("Decon2LS_V2 does not yet support Bruker MALDI data (" & eRawDataType.ToString() & ")")
                Return DeconToolsFileTypeConstants.Undefined

            Case clsAnalysisResources.eRawDataTypeConstants.mzXML
                Return DeconToolsFileTypeConstants.MZXML_Rawdata

            Case clsAnalysisResources.eRawDataTypeConstants.mzML
                ' TODO: Add support for this after Decon2LS is updated
                ' Return DeconToolsFileTypeConstants.MZML_Rawdata

                LogError("Decon2LS_V2 does not yet support mzML data")
                Return DeconToolsFileTypeConstants.Undefined

            Case Else
                ' Should never get this value
                Return DeconToolsFileTypeConstants.Undefined
        End Select

    End Function

    Private Sub ParseDeconToolsLogFile(ByRef blnFinishedProcessing As Boolean, ByRef dtFinishTime As DateTime)

        Dim fiFileInfo As FileInfo

        Dim strLogFilePath As String
        Dim strLineIn As String
        Dim blnDateValid As Boolean

        Dim intCharIndex As Integer

        Dim strScanFrameLine As String = String.Empty

        blnFinishedProcessing = False

        Try
            Select Case mRawDataType
                Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf
                    ' As of 11/19/2010, the _Log.txt file is created inside the .D folder
                    strLogFilePath = Path.Combine(mInputFilePath, m_Dataset) & "_log.txt"
                Case Else
                    strLogFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(mInputFilePath) & "_log.txt")
            End Select

            If File.Exists(strLogFilePath) Then

                Using srInFile = New StreamReader(New FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                    Do While Not srInFile.EndOfStream
                        strLineIn = srInFile.ReadLine

                        If String.IsNullOrWhiteSpace(strLineIn) Then Continue Do

                        intCharIndex = strLineIn.ToLower().IndexOf("finished file processing", StringComparison.Ordinal)
                        If intCharIndex >= 0 Then

                            blnDateValid = False
                            If intCharIndex > 1 Then
                                ' Parse out the date from strLineIn
                                If DateTime.TryParse(strLineIn.Substring(0, intCharIndex).Trim, dtFinishTime) Then
                                    blnDateValid = True
                                Else
                                    ' Unable to parse out the date
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to parse date from string '" & strLineIn.Substring(0, intCharIndex).Trim & "'; will use file modification date as the processing finish time")
                                End If
                            End If

                            If Not blnDateValid Then
                                fiFileInfo = New FileInfo(strLogFilePath)
                                dtFinishTime = fiFileInfo.LastWriteTime
                            End If

                            If m_DebugLevel >= 3 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DeconTools log file reports 'finished file processing' at " & dtFinishTime.ToString())
                            End If

                            blnFinishedProcessing = True
                            Exit Do
                        End If

                        intCharIndex = strLineIn.ToLower.IndexOf("scan/frame", StringComparison.Ordinal)
                        If intCharIndex >= 0 Then
                            strScanFrameLine = strLineIn.Substring(intCharIndex)
                        End If

                        intCharIndex = strLineIn.IndexOf("ERROR THROWN", StringComparison.Ordinal)
                        If intCharIndex > 0 Then
                            ' An exception was reported in the log file; treat this as a fatal error
                            m_message = "Error thrown by DeconTools"

                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeconTools reports " & strLineIn.Substring(intCharIndex))
                            mDeconToolsExceptionThrown = True

                        End If

                    Loop

                End Using
            End If

        Catch ex As Exception
            ' Ignore errors here		
            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Exception in ParseDeconToolsLogFile: " & ex.Message)
            End If

        End Try

        If Not String.IsNullOrWhiteSpace(strScanFrameLine) Then
            ' Parse strScanFrameLine
            ' It will look like:
            ' Scan/Frame= 347; PercentComplete= 2.7; AccumlatedFeatures= 614

            Dim kvStat As KeyValuePair(Of String, String)

            Dim strProgressStats = strScanFrameLine.Split(";"c)

            For i = 0 To strProgressStats.Length - 1
                kvStat = ParseKeyValue(strProgressStats(i))
                If Not String.IsNullOrWhiteSpace(kvStat.Key) Then
                    Select Case kvStat.Key
                        Case "Scan/Frame"
                            Integer.TryParse(kvStat.Value, mDeconToolsStatus.CurrentLCScan)
                        Case "PercentComplete"
                            Single.TryParse(kvStat.Value, mDeconToolsStatus.PercentComplete)
                        Case "AccumlatedFeatures"

                    End Select
                End If
            Next

            m_progress = mDeconToolsStatus.PercentComplete

        End If


    End Sub

    ''' <summary>
    ''' Looks for an equals sign in strData
    ''' Returns a KeyValuePair object with the text before the equals sign and the text after the equals sign
    ''' </summary>
    ''' <param name="strData"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function ParseKeyValue(strData As String) As KeyValuePair(Of String, String)
        Dim intCharIndex As Integer
        intCharIndex = strData.IndexOf("="c)

        If intCharIndex > 0 Then
            Try
                Return New KeyValuePair(Of String, String)(strData.Substring(0, intCharIndex).Trim(), strData.Substring(intCharIndex + 1).Trim())
            Catch ex As Exception
                ' Ignore errors here
            End Try
        End If

        Return New KeyValuePair(Of String, String)(String.Empty, String.Empty)

    End Function

    ''' <summary>
    ''' Opens the specified results file from DeconTools and looks for at least two non-blank lines
    ''' </summary>
    ''' <param name="strFilePath"></param>
    ''' <returns>True if two or more non-blank lines; otherwise false</returns>
    ''' <remarks></remarks>
    Private Function ResultsFileHasData(strFilePath As String) As Boolean

        If Not File.Exists(strFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DeconTools results file not found: " & strFilePath)
            Return False
        End If

        Dim intDataLineCount = 0

        ' Open the DeconTools results file
        ' The first line is the header lines
        ' Lines after that are data lines

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Opening the DeconTools results file: " & strFilePath)

        Using srReader = New StreamReader(New FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            While Not srReader.EndOfStream AndAlso intDataLineCount < 2

                Dim strLineIn As String = srReader.ReadLine()
                If Not String.IsNullOrWhiteSpace(strLineIn) Then
                    intDataLineCount += 1
                End If
            End While
        End Using

        If intDataLineCount >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DeconTools results file has at least two non-blank lines")
            Return True
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DeconTools results file is empty")
            Return False
        End If

    End Function

    Public Function GetInputFilePath(eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As String
        Return GetInputFilePath(m_WorkDir, m_Dataset, eRawDataType)
    End Function

    ''' <summary>
    ''' assembles a string telling Decon2LS the name of the input file or folder
    ''' </summary>
    ''' <param name="eRawDataType"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function GetInputFilePath(workDirPath As String, datasetName As String, eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As String

        Select Case eRawDataType
            Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
                Return Path.Combine(workDirPath, datasetName & clsAnalysisResources.DOT_RAW_EXTENSION)

            Case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile
                Return Path.Combine(workDirPath, datasetName & clsAnalysisResources.DOT_WIFF_EXTENSION)

            Case clsAnalysisResources.eRawDataTypeConstants.UIMF
                Return Path.Combine(workDirPath, datasetName & clsAnalysisResources.DOT_UIMF_EXTENSION)

            Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder
                Return Path.Combine(workDirPath, datasetName) & clsAnalysisResources.DOT_D_EXTENSION

            Case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder
                Return Path.Combine(workDirPath, datasetName) & clsAnalysisResources.DOT_RAW_EXTENSION & "/_FUNC001.DAT"

            Case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders
                Return Path.Combine(workDirPath, datasetName)

            Case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder
                ' Bruker_FT folders are actually .D folders
                Return Path.Combine(workDirPath, datasetName) & clsAnalysisResources.DOT_D_EXTENSION

            Case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf
                ' Bruker_TOFBaf folders are actually .D folders
                Return Path.Combine(workDirPath, datasetName) & clsAnalysisResources.DOT_D_EXTENSION

            Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot
                ''''''''''''''''''''''''''''''''''''
                ' TODO: Finalize this code
                '       DMS doesn't yet have a BrukerTOF dataset 
                '        so we don't know the official folder structure
                ''''''''''''''''''''''''''''''''''''
                Return Path.Combine(workDirPath, datasetName)

            Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging
                ''''''''''''''''''''''''''''''''''''
                ' TODO: Finalize this code
                '       DMS doesn't yet have a BrukerTOF dataset 
                '        so we don't know the official folder structure
                ''''''''''''''''''''''''''''''''''''
                Return Path.Combine(workDirPath, datasetName)

            Case clsAnalysisResources.eRawDataTypeConstants.mzXML
                Return Path.Combine(workDirPath, datasetName & clsAnalysisResources.DOT_MZXML_EXTENSION)

            Case clsAnalysisResources.eRawDataTypeConstants.mzML
                Return Path.Combine(workDirPath, datasetName & clsAnalysisResources.DOT_MZML_EXTENSION)

            Case Else
                ' Should never get this value
                Return String.Empty
        End Select

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Private Function StoreToolVersionInfo(strDeconToolsProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioDeconToolsInfo As FileInfo
        Dim blnSuccess As Boolean

        Dim reParseVersion As Regex
        Dim reMatch As Match

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ioDeconToolsInfo = New FileInfo(strDeconToolsProgLoc)
        If Not ioDeconToolsInfo.Exists Then
            Try
                strToolVersionInfo = "Unknown"
                Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo), blnSaveToolVersionTextFile:=False)
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
                Return False
            End Try

        End If

        ' Lookup the version of the DeconConsole application
        blnSuccess = MyBase.StoreToolVersionInfoViaSystemDiagnostics(strToolVersionInfo, ioDeconToolsInfo.FullName)
        If Not blnSuccess Then Return False

        ' Parse out the DeconConsole Build number using a RegEx
        ' strToolVersionInfo should look like: DeconConsole, Version=1.0.4400.22961

        mDeconConsoleBuild = 0
        reParseVersion = New Regex("Version=\d+\.\d+\.(\d+)")
        reMatch = reParseVersion.Match(strToolVersionInfo)
        If reMatch.Success Then
            If Not Integer.TryParse(reMatch.Groups.Item(1).Value, mDeconConsoleBuild) Then
                ' Error parsing out the version
                m_message = "Error determining DeconConsole version, cannot convert build to integer"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strToolVersionInfo)
                Return False
            End If
        Else
            m_message = "Error determining DeconConsole version, RegEx did not match"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strToolVersionInfo)
            Return False
        End If

        ' Lookup the version of the DeconTools Backend (in the DeconTools folder)
        Dim strDeconToolsBackendPath As String = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconTools.Backend.dll")
        blnSuccess = MyBase.StoreToolVersionInfoViaSystemDiagnostics(strToolVersionInfo, strDeconToolsBackendPath)
        If Not blnSuccess Then Return False

        ' Lookup the version of the UIMFLibrary (in the DeconTools folder)
        Dim strDLLPath As String = Path.Combine(ioDeconToolsInfo.DirectoryName, "UIMFLibrary.dll")
        blnSuccess = MyBase.StoreToolVersionInfoViaSystemDiagnostics(strToolVersionInfo, strDLLPath)
        If Not blnSuccess Then Return False

        ' Old: Lookup the version of DeconEngine (in the DeconTools folder)
        ' Disabled July 31, 2014 because support for Rapid was removed from DeconTools.Backend.dll and thus DeconEngine.dll is no longer required
        ' strDLLPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconEngine.dll")
        ' blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDLLPath)
        ' If Not blnSuccess Then Return False

        ' Old: Lookup the version of DeconEngineV2 (in the DeconTools folder)
        ' Disabled May 20, 2016 because the C++ code that was in DeconEngineV2.dll has been ported to C# and is now part of DeconTools.Backend
        ' See DeconTools.Backend\ProcessingTasks\Deconvoluters\HornDeconvolutor\ThrashV1\ThrashV1_Readme.txt    
        '
        ' strDLLPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconEngineV2.dll")
        ' blnSuccess = MyBase.StoreToolVersionInfoViaSystemDiagnostics(strToolVersionInfo, strDLLPath)
        ' If Not blnSuccess Then Return False

        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(New FileInfo(strDeconToolsProgLoc))
        ioToolFiles.Add(New FileInfo(strDeconToolsBackendPath))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
        Catch ex As Exception
            LogError("Exception calling SetStepTaskToolVersion", ex)
            Return False
        End Try

    End Function

    Private Sub ZipPeaksFile()

        Dim strPeaksFilePath As String
        Dim strZippedPeaksFilePath As String

        Try
            strPeaksFilePath = Path.Combine(m_WorkDir, m_Dataset & DECON2LS_PEAKS_FILE_SUFFIX)
            strZippedPeaksFilePath = Path.Combine(m_WorkDir, m_Dataset & "_peaks.zip")

            If File.Exists(strPeaksFilePath) Then

                If Not MyBase.ZipFile(strPeaksFilePath, False, strZippedPeaksFilePath) Then
                    Dim Msg As String = "Error zipping " & DECON2LS_PEAKS_FILE_SUFFIX & " file, job " & m_JobNum
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                    m_message = clsGlobal.AppendToComment(m_message, "Error zipping Peaks.txt file")
                    Return
                End If

                ' Add the _peaks.txt file to .FilesToDelete since we only want to keep the Zipped version
                m_jobParams.AddResultFileToSkip(Path.GetFileName(strPeaksFilePath))

            End If

        Catch ex As Exception
            LogError("Exception zipping Peaks.txt file", ex)
            Return
        End Try

        Return
    End Sub


#End Region

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting()

        Const SECONDS_BETWEEN_UPDATE = 30
        Static dtLastLogCheckTime As DateTime = DateTime.UtcNow

        UpdateStatusFile()

        ' Parse the log file every 30 seconds to determine the % complete
        If DateTime.UtcNow.Subtract(dtLastLogCheckTime).TotalSeconds < SECONDS_BETWEEN_UPDATE Then
            Return
        End If

        dtLastLogCheckTime = DateTime.UtcNow

        Dim dtFinishTime As DateTime
        Dim blnFinishedProcessing As Boolean

        ParseDeconToolsLogFile(blnFinishedProcessing, dtFinishTime)

        UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE)

        Dim strProgressMessage As String

        If mDeconToolsStatus.IsUIMF Then
            strProgressMessage = "Frame=" & mDeconToolsStatus.CurrentLCScan
        Else
            strProgressMessage = "Scan=" & mDeconToolsStatus.CurrentLCScan
        End If

        strProgressMessage = "DeconTools, " & strProgressMessage

        Dim logIntervalMinutes As Integer
        If m_DebugLevel >= 5 Then
            logIntervalMinutes = 1
        ElseIf m_DebugLevel >= 4 Then
            logIntervalMinutes = 2
        ElseIf m_DebugLevel >= 3 Then
            logIntervalMinutes = 5
        ElseIf m_DebugLevel >= 2 Then
            logIntervalMinutes = 10
        Else
            logIntervalMinutes = 15
        End If

        LogProgress(strProgressMessage, logIntervalMinutes)

        Const MAX_LOGFINISHED_WAITTIME_SECONDS = 120
        If blnFinishedProcessing Then
            ' The Decon2LS Log File reports that the task is complete
            ' If it finished over MAX_LOGFINISHED_WAITTIME_SECONDS seconds ago, then send an abort to the CmdRunner

            If DateTime.Now().Subtract(dtFinishTime).TotalSeconds >= MAX_LOGFINISHED_WAITTIME_SECONDS Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Note: Log file reports finished over " & MAX_LOGFINISHED_WAITTIME_SECONDS & " seconds ago, but the DeconTools CmdRunner is still active")

                mDeconToolsFinishedDespiteProgRunnerError = True

                ' Abort processing
                mCmdRunner.AbortProgramNow()

                Threading.Thread.Sleep(3000)
            End If
        End If

    End Sub

End Class
