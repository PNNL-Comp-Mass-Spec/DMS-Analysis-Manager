Option Strict On

Imports AnalysisManagerBase

' This class was converted to be loaded as a pluggable DLL into the New DMS 
' Analysis Tool Manager program.  The new ATM supports the mini-pipeline. It 
' uses class clsMsMsSpectrumFilter to filter the _DTA.txt file present in a given folder
'
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Copyright 2005, Battelle Memorial Institute
' Started October 13, 2005
' 
' Converted January 23, 2009 by JDS
' Updated July 2009 by MEM to process a _Dta.txt file instead of a folder of .Dta files
' Updated August 2009 by MEM to generate _ScanStats.txt files, if required

Public Class clsAnalysisToolRunnerMsMsSpectrumFilter
    Inherits clsAnalysisToolRunnerBase

    Protected WithEvents m_MsMsSpectrumFilter As clsMsMsSpectrumFilter
    Protected m_ErrMsg As String = ""
    Protected m_SettingsFileName As String = ""         'Handy place to store value so repeated calls to m_JobParams aren't required
    Protected m_Results As ISpectraFilter.ProcessResults
    Protected m_DSName As String = ""                               'Handy place to store value so repeated calls to m_JobParams aren't required
    Protected m_DTATextFileName As String = ""

    Protected m_thThread As System.Threading.Thread
    Protected Shadows m_Status As ISpectraFilter.ProcessStatus

#Region "Methods"
    Public Sub New()

    End Sub

    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        m_Status = ISpectraFilter.ProcessStatus.SFILT_STARTING

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "clsAnalysisToolRunnerMsMsSpectrumFilter.RunTool(), Filtering _Dta.txt file")

        'Verify necessary files are in specified locations
        If Not InitSetup() Then
            m_Results = ISpectraFilter.ProcessResults.SFILT_FAILURE
            m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Filter the spectra (the process runs in a separate thread)
        m_Status = FilterDTATextFile()

        If m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR Then
            m_Results = ISpectraFilter.ProcessResults.SFILT_FAILURE
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMsMsSpectrumFilter.RunTool(), Filtering complete")
        End If

        ' Zip the filtered _Dta.txt file
        result = ZipConcDtaFile()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            m_Results = ISpectraFilter.ProcessResults.SFILT_FAILURE
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Stop the job timer
        m_StopTime = Now

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make the results folder
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMsMsSpectrumFilter.RunTool(), Making results folder")
        End If

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MakeResultsFolder handles posting to local log, so set database error message and exit
            m_message = "Error making results folder"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MoveResultFiles moves the result files to the result folder
            m_message = "Error making results folder"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        If Not clsGlobal.RemoveNonResultFiles(m_WorkDir, m_DebugLevel) Then
            'TODO: Figure out what to do here
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function


    Protected Overridable Function CountDtaFiles(ByVal strDTATextFilePath As String) As Integer
        'Returns the number of dta files in the _dta.txt file

        ' This RegEx matches text of the form:
        ' =================================== "File.ScanStart.ScanEnd.Charge.dta" ==================================
        '
        ' For example:
        ' =================================== "QC_Shew_07_02-pt5-a_27Sep07_EARTH_07-08-15.351.351.1.dta" ==================================
        '
        ' It also can match lines where there is extra information associated with the charge state, for example:
        ' =================================== "QC_Shew_07_02-pt5-a_27Sep07_EARTH_07-08-15.351.351.1_1_2.dta" ==================================
        ' =================================== "vxl_VP2P74_B_4_F12_rn1_14May08_Falcon_080403-F4.1001.1001.2_1_2.dta" ==================================
        Const DTA_FILENAME_REGEX As String = "^\s*[=]{5,}\s+\""([^.]+)\.\d+\.\d+\..+dta"

        Dim intDTACount As Integer
        Dim strLineIn As String

        Dim srInFile As System.IO.StreamReader
        Dim reFind As System.Text.RegularExpressions.Regex

        Try
            reFind = New System.Text.RegularExpressions.Regex(DTA_FILENAME_REGEX, Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)

            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strDTATextFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

            intDTACount = 0
            Do While srInFile.Peek() >= 0

                strLineIn = srInFile.ReadLine

                If Not strLineIn Is Nothing Then
                    If reFind.Match(strLineIn).Success Then
                        intDTACount += 1
                    End If
                End If
            Loop

        Catch ex As Exception
            LogErrors("CountDtaFiles", "Error counting .Dta files in strDTATextFilePath", ex)
            m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR
        Finally
            If Not srInFile Is Nothing Then
                srInFile.Close()
            End If
        End Try

        Return intDTACount

    End Function

    Protected Overridable Function FilterDTATextFile() As ISpectraFilter.ProcessStatus
        ' Initializes m_MsMsSpectrumFilter, then starts a separate thread to filter the _Dta.txt file in the working folder
        ' If ScanStats files are required, will first call GenerateFinniganScanStatsFiles() to generate those files using the .Raw file

        Dim strParameterFilePath As String

        Try
            'Initialize MsMsSpectrumFilterDLL.dll
            m_MsMsSpectrumFilter = New clsMsMsSpectrumFilter

            ' Pre-read the parameter file now, so that we can override some of the settings
            strParameterFilePath = System.IO.Path.Combine(m_WorkDir, m_SettingsFileName)
            If Not m_MsMsSpectrumFilter.LoadParameterFileSettings(strParameterFilePath) Then
                m_ErrMsg = m_MsMsSpectrumFilter.GetErrorMessage
                If m_ErrMsg Is Nothing OrElse m_ErrMsg.Length = 0 Then
                    m_ErrMsg = "Parameter file load error: " & strParameterFilePath
                End If
                LogErrors("FilterDTATextFile", m_ErrMsg, Nothing)
                Return ISpectraFilter.ProcessStatus.SFILT_ERROR
            End If

            ' Set a few additional settings
            With m_MsMsSpectrumFilter
                .OverwriteExistingFiles = True
                .OverwriteReportFile = True
                .AutoCloseReportFile = False
            End With

            ' Determine if we need to generate a _ScanStats.txt file
            If m_MsMsSpectrumFilter.ScanStatsFileIsRequired Then
                If Not GenerateFinniganScanStatsFiles Then
                    m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR
                    Return m_Status
                End If
            End If

            'Instantiate the thread to run MsMsSpectraFilter
            m_thThread = New System.Threading.Thread(AddressOf FilterDTATextFileWork)
            m_thThread.Start()

            Do While m_thThread.IsAlive
                System.Threading.Thread.Sleep(2000)                 'Delay for 2 seconds
            Loop

            System.Threading.Thread.Sleep(5000)                    'Delay for 5 seconds
            GC.Collect()
            GC.WaitForPendingFinalizers()

            'Removes the MsMsSpectra Filter object
            If Not IsNothing(m_thThread) Then
                m_thThread.Abort()
                m_thThread = Nothing
            End If

            'If we reach here, everything must be good
            m_Status = ISpectraFilter.ProcessStatus.SFILT_COMPLETE

        Catch ex As Exception
            LogErrors("FilterDTAFilesInFolder", "Error initializing and running clsMsMsSpectrumFilter", ex)
            m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR
        End Try

        Return m_Status
    End Function

    Protected Overridable Sub FilterDTATextFileWork()

        Dim strInputFilePath As String
        Dim strBakFilePath As String

        Dim blnSuccess As Boolean

        Try

            ' Define the input file name
            strInputFilePath = System.IO.Path.Combine(m_WorkDir, m_DTATextFileName)

            blnSuccess = m_MsMsSpectrumFilter.ProcessFilesWildcard(strInputFilePath, m_WorkDir, "")

     
            Try

                If blnSuccess Then
                    ' Sort the report file (this also closes the file)
                    m_MsMsSpectrumFilter.SortSpectrumQualityTextFile()

                    ' Delete the _dta.txt.bak file
                    strBakFilePath = strInputFilePath & ".bak"
                    If System.IO.File.Exists(strBakFilePath) Then
                        System.IO.File.Delete(strBakFilePath)
                    End If

                    'Count the number of .Dta files remaining in the _dta.txt file
                    If Not VerifyDtaCreation(strInputFilePath) Then
                        m_Results = ISpectraFilter.ProcessResults.SFILT_NO_FILES_CREATED
                    Else
                        m_Results = ISpectraFilter.ProcessResults.SFILT_SUCCESS
                    End If

                    m_Status = ISpectraFilter.ProcessStatus.SFILT_COMPLETE
                Else
                    If m_MsMsSpectrumFilter.AbortProcessing Then
                        LogErrors("FilterDTATextFileWork", "Processing aborted", Nothing)
                        m_Results = ISpectraFilter.ProcessResults.SFILT_ABORTED
                        m_Status = ISpectraFilter.ProcessStatus.SFILT_ABORTING
                    Else
                        LogErrors("FilterDTATextFileWork", m_MsMsSpectrumFilter.GetErrorMessage(), Nothing)
                        m_Results = ISpectraFilter.ProcessResults.SFILT_FAILURE
                        m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR
                    End If
                End If

            Catch ex As Exception
                LogErrors("FilterDTATextFileWork", "Error performing tasks after m_MsMsSpectrumFilter.ProcessFilesWildcard completes", ex)
                m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR
            End Try

        Catch ex As Exception
            LogErrors("FilterDTATextFileWork", "Error calling m_MsMsSpectrumFilter.ProcessFilesWildcard", ex)
            m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR
        End Try


    End Sub

    Protected Function GenerateFinniganScanStatsFiles() As Boolean

        Dim strRawFileName As String
        Dim strFinniganRawFilePath As String

        Dim blnScanStatsFilesExist As Boolean

        Dim blnSuccess As Boolean

        Try
            ' Assume success for now
            blnSuccess = True

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Looking for the _ScanStats.txt files for dataset " & m_DSName)
            End If

            blnScanStatsFilesExist = clsMsMsSpectrumFilter.CheckForExistingScanStatsFiles(m_WorkDir, m_DSName)
            If blnScanStatsFilesExist Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "_ScanStats.txt files found for dataset " & m_DSName)
                End If
            Else

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating the _ScanStats.txt files for dataset " & m_DSName)

                ' Determine the path to the .Raw file
                strRawFileName = m_DSName & ".raw"
                strFinniganRawFilePath = AnalysisManagerBase.clsAnalysisResources.ResolveStoragePath(m_WorkDir, strRawFileName)

                If strFinniganRawFilePath Is Nothing OrElse strFinniganRawFilePath.Length = 0 Then
                    ' Unable to resolve the file path
                    m_ErrMsg = "Could not find " & strRawFileName & " or " & strRawFileName & AnalysisManagerBase.clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX & " in the working folder; unable to generate the ScanStats files"
                    LogErrors("GenerateFinniganScanStatsFiles", m_ErrMsg, Nothing)
                    Return False
                End If

                If Not System.IO.File.Exists(strFinniganRawFilePath) Then
                    ' File not found at the specified path
                    m_ErrMsg = "File not found: " & strFinniganRawFilePath & " -- unable to generate the ScanStats files"
                    LogErrors("GenerateFinniganScanStatsFiles", m_ErrMsg, Nothing)
                    blnSuccess = False
                Else
                    If m_DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Generating _ScanStats.txt file using " & strFinniganRawFilePath)
                    End If

                    If Not m_MsMsSpectrumFilter.GenerateFinniganScanStatsFiles(strFinniganRawFilePath, m_WorkDir) Then
                        m_ErrMsg = m_MsMsSpectrumFilter.GetErrorMessage()
                        If m_ErrMsg Is Nothing OrElse m_ErrMsg.Length = 0 Then
                            m_ErrMsg = "GenerateFinniganScanStatsFiles returned False; _ScanStats.txt files not generated"
                        End If

                        LogErrors("GenerateFinniganScanStatsFiles", m_ErrMsg, Nothing)
                        blnSuccess = False
                    End If
                End If

            End If

        Catch ex As Exception
            LogErrors("GenerateFinniganScanStatsFiles", "Error generating _ScanStats.txt files", ex)
        End Try

        Return blnSuccess

    End Function

    Private Sub HandleProgressUpdate(ByVal percentComplete As Single)
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.Now

        m_progress = percentComplete

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.Now
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, m_DtaCount, "", "", "", False)
        End If

    End Sub
   
    Protected Overridable Function InitSetup() As Boolean

        'Initializes module variables and verifies mandatory parameters have been propery specified

        'Manager parameters
        If m_mgrParams Is Nothing Then
            m_ErrMsg = "Manager parameters not specified"
            Return False
        End If

        'Job parameters
        If m_jobParams Is Nothing Then
            m_ErrMsg = "Job parameters not specified"
            Return False
        End If

        'Status tools
        If m_StatusTools Is Nothing Then
            m_ErrMsg = "Status tools object not set"
            Return False
        End If

        'Set dataset name
        m_DSName = m_jobParams.GetParam("datasetNum")

        'Set the _DTA.Txt file name
        m_DTATextFileName = m_DSName & "_dta.txt"

        'Set settings file name
        'This is the job parameters file that contains the settings information
        m_SettingsFileName = m_jobParams.GetParam("genJobParamsFilename")

        'Source folder name
        If m_WorkDir = "" Then
            m_ErrMsg = "m_WorkDir variable is empty"
            Return False
        End If

        'Source directory exist?
        If Not VerifyDirExists(m_WorkDir) Then Return False 'Error msg handled by VerifyDirExists

        'Settings file exist?
        Dim SettingsNamePath As String = System.IO.Path.Combine(m_WorkDir, m_SettingsFileName)
        If Not VerifyFileExists(SettingsNamePath) Then Return False 'Error msg handled by VerifyFileExists

        'If we got here, everything's OK
        Return True

    End Function

    Private Sub LogErrors(ByVal strSource As String, ByVal strMessage As String, ByVal ex As Exception, Optional ByVal blnLogLocalOnly As Boolean = True)

        m_ErrMsg = String.Copy(strMessage).Replace(ControlChars.NewLine, "; ")

        If ex Is Nothing Then
            ex = New System.Exception("Error")
        Else
            If Not ex.Message Is Nothing AndAlso ex.Message.Length > 0 Then
                m_ErrMsg &= "; " & ex.Message
            End If
        End If

        Trace.WriteLine(Now.ToLongTimeString & "; " & m_ErrMsg, strSource)
        Console.WriteLine(Now.ToLongTimeString & "; " & m_ErrMsg, strSource)

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg & ex.Message)

    End Sub

    Protected Overridable Function VerifyDirExists(ByVal TestDir As String) As Boolean

        'Verifies that the specified directory exists
        If System.IO.Directory.Exists(TestDir) Then
            m_ErrMsg = ""
            Return True
        Else
            m_ErrMsg = "Directory " & TestDir & " not found"
            Return False
        End If

    End Function

    
    Private Function VerifyDtaCreation(ByVal strDTATextFilePath As String) As Boolean

        'Verify at least one .dta file has been created
        If CountDtaFiles(strDTATextFilePath) < 1 Then
            m_ErrMsg = "No dta files remain after filtering"
            Return False
        Else
            Return True
        End If

    End Function

    Protected Overridable Function VerifyFileExists(ByVal TestFile As String) As Boolean
        'Verifies specified file exists
        If System.IO.File.Exists(TestFile) Then
            m_ErrMsg = ""
            Return True
        Else
            m_ErrMsg = "File " & TestFile & " not found"
            Return False
        End If

    End Function

    ''' <summary>
    ''' Zips concatenated DTA file to reduce size
    ''' </summary>
    ''' <returns>CloseoutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Protected Overridable Function ZipConcDtaFile() As IJobParams.CloseOutType

        'Zips the concatenated dta file
        Dim DtaFileName As String = m_jobParams.GetParam("datasetNum") & "_dta.txt"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Zipping concatenated spectra file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))

        'Verify file exists
        If Not System.IO.File.Exists(System.IO.Path.Combine(m_WorkDir, DtaFileName)) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to find concatenated dta file")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Zip the file
        Try
            Dim ZipProgramPath As String = m_mgrParams.GetParam("zipprogram")
            If ZipProgramPath Is Nothing Then ZipProgramPath = String.Empty
            If Not System.IO.File.Exists(ZipProgramPath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Zip program not found: " & ZipProgramPath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Dim Zipper As New PRISM.Files.ZipTools(m_WorkDir, ZipProgramPath)
            Dim ZipFileName As String = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileNameWithoutExtension(DtaFileName)) & ".zip"

            If System.IO.File.Exists(ZipFileName) Then
                ' Delete any existing .zip file
                System.IO.File.Delete(ZipFileName)
                System.Threading.Thread.Sleep(250)
            End If

            If Not Zipper.MakeZipFile("-fast", ZipFileName, DtaFileName) Then
                Dim Msg As String = "Error zipping concat dta file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Catch ex As Exception
            Dim Msg As String = "Exception zipping concat dta file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

#End Region

    Private Sub m_MsMsSpectrumFilter_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles m_MsMsSpectrumFilter.ProgressChanged
        HandleProgressUpdate(percentComplete)
    End Sub

    Private Sub m_MsMsSpectrumFilter_ProgressComplete() Handles m_MsMsSpectrumFilter.ProgressComplete
        HandleProgressUpdate(100)
    End Sub
End Class
