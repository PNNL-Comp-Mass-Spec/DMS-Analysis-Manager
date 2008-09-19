'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 07/25/2008
'
' 
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Logging
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports System.io
Imports AnalysisManagerMSMSBase
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerIN
    Inherits clsAnalysisToolRunnerMSMS

    '*********************************************************************************************************
    'Class for running InSpecT analysis
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_INSPECT_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_INSPECT_START As Single = 95
    Protected Const PROGRESS_PCT_INSPECT_COMPLETE As Single = 99

    Protected WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs InSpecT tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function OperateAnalysisTool() As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType
        Dim OrgDbName As String = m_jobParams.GetParam("organismDBName")

        m_logger.PostEntry("clsAnalysisToolRunnerIN.OperateAnalysisTool(): Enter " & OrgDbName, ILogger.logMsgType.logDebug, True)

        If CreateIndexedDbFiles() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            If CreatemzXMLFile() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                If RunInSpecT() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    If RunpValue() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                        result = RunSummary()
                    Else
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    End If
                Else
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Else
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        result = DeleteTempAnalFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        'Zip the output file
        Dim ZipResult As IJobParams.CloseOutType = ZipMainOutputFile()

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    ''' <summary>
    ''' Calls base class to make an InSpecT results folder
    ''' </summary>
    ''' <param name="AnalysisType">Analysis type prefix for results folder name</param>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Protected Overrides Function MakeResultsFolder(ByVal AnalysisType As String) As IJobParams.CloseOutType
        MyBase.MakeResultsFolder("INS")
    End Function
    ''' <summary>
    ''' Override the main function in the analysis manager to by-pass the call
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function CreateAndFilterMSMSSpectra() As IJobParams.CloseOutType
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    End Function
    ''' <summary>
    ''' Cleans up stray analysis files
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks>Not presently implemented</remarks>
    Protected Overrides Function DeleteTempAnalFiles() As IJobParams.CloseOutType
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        'get rid of .mzXML file
        Dim mzXMLFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & ".mzXML")
        File.Delete(mzXMLFilename)
        'get rid of files in ordbd local directory
        Dim OrgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        If Not CleanOrgDbDir(OrgDbDir) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
    ''' <summary>
    ''' Convert .Fasta file to indexed DB files
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function CreateIndexedDbFiles() As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim orgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        Dim dbFilename As String = Path.Combine(orgDbDir, m_jobParams.GetParam("LegacyFastaFileName"))
        Dim pythonProgLoc As String = m_mgrParams.GetParam("pythonprogloc")
        Dim m_workDirOrig As String = WorkingDir

        CmdRunner = New clsRunDosProgram(m_logger, InspectDir & "\")

        If m_DebugLevel > 4 Then
            m_logger.PostEntry("clsAnalysisToolRunnerIN.CreateIndexedDbFiles(): Enter", ILogger.logMsgType.logDebug, True)
        End If

        ' verify that program file exists
        Dim progLoc As String = pythonProgLoc
        If Not File.Exists(progLoc) Then
            m_logger.PostEntry("Cannot find python.exe program file", ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run PrepDB.py
        CmdStr = " " & InspectDir & "\PrepDB.py " & "FASTA " & dbFilename
        m_logger.PostEntry(progLoc & CmdStr, ILogger.logMsgType.logDebug, True)
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "PrepDB.py", True) Then
            m_logger.PostEntry("Error running PrepDB.py " & dbFilename & " : " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            m_WorkDir = m_workDirOrig
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
    ''' <summary>
    ''' Convert .Raw file to mzXML format
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function CreatemzXMLFile() As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim rawFile As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & ".raw")

        CmdRunner = New clsRunDosProgram(m_logger, InspectDir)

        If m_DebugLevel > 4 Then
            m_logger.PostEntry("clsAnalysisToolRunnerIN.CreatemzXMLFile(): Enter", ILogger.logMsgType.logDebug, True)
        End If

        ' verify that program file exists
        Dim progLoc As String = InspectDir & "\readw.exe"
        If Not File.Exists(progLoc) Then
            m_logger.PostEntry("Cannot find ReAdW.exe program file", ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run readw.exe
        CmdStr = " -c " & rawFile
        m_logger.PostEntry(progLoc & CmdStr, ILogger.logMsgType.logDebug, True)
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "readw.exe", True) Then
            m_logger.PostEntry("Error running PrepDB.py" & rawFile & " : " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
    ''' <summary>
    ''' Run InSpecT
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function RunInSpecT() As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim ParamFilePath As String = Path.Combine(WorkingDir, m_jobParams.GetParam("parmFileName"))
        Dim rawFile As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & ".raw")
        Dim OutputFilePath As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_inspect.txt")
        Dim errorFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_error.txt")
        Dim inputFilename As String = ""

        inputFilename = BuildInspectInputFile()
        If inputFilename = "" Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        CmdRunner = New clsRunDosProgram(m_logger, InspectDir)

        If m_DebugLevel > 4 Then
            m_logger.PostEntry("clsAnalysisToolRunnerIN.RunInSpecT(): Enter", ILogger.logMsgType.logDebug, True)
        End If

        ' verify that program file exists
        Dim progLoc As String = InspectDir & "\inspect.exe"
        If Not File.Exists(progLoc) Then
            m_logger.PostEntry("Cannot find inspect.exe program file", ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run PrepDB.py
        CmdStr = " -i " & inputFilename & " -o " & OutputFilePath & " -e " & errorFilename
        m_logger.PostEntry(progLoc & CmdStr, ILogger.logMsgType.logDebug, True)
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "PrepDB.py", True) Then
            m_logger.PostEntry("Error running PrepDB.py" & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
    ''' <summary>
    ''' Build inspect input file from base parameter file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function BuildInspectInputFile() As String
        Dim result As String = ""
        '        Dim inputFilename As String = ""
        ' set up input to reference spectra file, taxonomy file, and parameter file

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim ParamFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("parmFileName"))
        Dim orgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        Dim fastaFilename As String = Path.Combine(orgDbDir, m_jobParams.GetParam("LegacyFastaFileName"))
        Dim dbFilename As String = fastaFilename.Replace("fasta", "trie")
        Dim mzXMLFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & ".mzXML")
        Dim inputFilename As String = Path.Combine(WorkingDir, "inspect_input.txt")
        Try
            'add extra lines to the parameter files
            'the parameter file will become the input file for inspect
            Dim inputFile As StreamWriter = New StreamWriter(inputFilename)
            ' Create an instance of StreamReader to read from a file.
            Dim inputBase As StreamReader = New StreamReader(ParamFilename)
            Dim paramLine As String
            inputFile.WriteLine("#Spectrum file to search; preferred formats are .mzXML and .mgf")
            inputFile.WriteLine("spectra," & mzXMLFilename)
            inputFile.WriteLine("")
            inputFile.WriteLine("#Note: The fully qualified database (.trie file) filename")
            inputFile.WriteLine("DB," & dbFilename)
            ' Read and display the lines from the file until the end 
            ' of the file is reached.
            Do
                paramLine = inputBase.ReadLine()
                If paramLine Is Nothing Then
                    Exit Do
                End If
                inputFile.WriteLine(paramLine)
            Loop Until paramLine Is Nothing
            inputBase.Close()
            inputFile.Close()
        Catch E As Exception
            ' Let the user know what went wrong.
            m_logger.PostError("The file could not be read", E, True)
            Return ""
        End Try

        Return inputFilename

    End Function
    ''' <summary>
    ''' Run pValue program
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function RunpValue() As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim inspectFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_inspect.txt")
        Dim pvalDistributionFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_PValueDistribution.txt")
        Dim filteredGroupFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_inspect_FilteredGrouped.txt")
        Dim orgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        Dim fastaFilename As String = Path.Combine(orgDbDir, m_jobParams.GetParam("LegacyFastaFileName"))
        Dim dbFilename As String = fastaFilename.Replace("fasta", "trie")
        Dim pythonProgLoc As String = m_mgrParams.GetParam("pythonprogloc")
        Dim settingsFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("SettingsFileName"))
        Dim pthresh As String = getPthresh(settingsFilename)

        CmdRunner = New clsRunDosProgram(m_logger, InspectDir)

        If m_DebugLevel > 4 Then
            m_logger.PostEntry("clsAnalysisToolRunnerIN.RunpValue(): Enter", ILogger.logMsgType.logDebug, True)
        End If

        ' verify that program file exists
        Dim progLoc As String = pythonProgLoc
        If Not File.Exists(progLoc) Then
            m_logger.PostEntry("Cannot find python.exe program file", ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run PrepDB.py
        'to do get value from settings files
        CmdStr = " " & InspectDir & "\PValue.py -r " & inspectFilename & " -s " & pvalDistributionFilename & " -w " & filteredGroupFilename & " -p " & pthresh & " -i -a -d " & dbFilename
        m_logger.PostEntry(progLoc & CmdStr, ILogger.logMsgType.logDebug, True)
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "PValue.py", True) Then
            m_logger.PostEntry("Error running PrepDB.py" & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
    ''' <summary>
    ''' Run -p threshold value
    ''' </summary>
    ''' <returns>Value as a string or empty string means failure</returns>
    ''' <remarks></remarks>
    Private Function getPthresh(ByVal settingsFilename As String) As String
        Dim defPvalThresh As String = "0.1"
        If File.Exists(settingsFilename) Then
            Dim settings_ini As New PRISM.Files.IniFileReader(settingsFilename, True)
            Dim tmpPvalThresh As String = ""
            tmpPvalThresh = settings_ini.GetIniValue("InspectResultsFilter", "InspectPvalueThreshold")
            If tmpPvalThresh <> "" Then
                Return tmpPvalThresh 'return pValueThreshold value in settings file
            Else
                Return defPvalThresh 'if not found, return default of 0.1
            End If
        Else
            Return defPvalThresh 'if not found, return default of 0.1
        End If

    End Function

    ''' <summary>
    ''' Run Summary program
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function RunSummary() As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim filteredHtmlFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_inpsect_FilteredGrouped.html")
        Dim filteredGroupFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_inspect_FilteredGrouped.txt")
        Dim orgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        Dim fastaFilename As String = Path.Combine(orgDbDir, m_jobParams.GetParam("LegacyFastaFileName"))
        Dim dbFilename As String = fastaFilename.Replace("fasta", "trie")
        Dim pythonProgLoc As String = m_mgrParams.GetParam("pythonprogloc")

        CmdRunner = New clsRunDosProgram(m_logger, InspectDir)

        If m_DebugLevel > 4 Then
            m_logger.PostEntry("clsAnalysisToolRunnerIN.RunSummary(): Enter", ILogger.logMsgType.logDebug, True)
        End If

        ' verify that program file exists
        Dim progLoc As String = pythonProgLoc
        If Not File.Exists(progLoc) Then
            m_logger.PostEntry("Cannot find python.exe program file", ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run PrepDB.py
        CmdStr = " " & InspectDir & "\summary.py -r " & filteredGroupFilename & " -d " & dbFilename & " -p 0.1 -w " & filteredHtmlFilename
        m_logger.PostEntry(progLoc & CmdStr, ILogger.logMsgType.logDebug, True)
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "summary.py", True) Then
            m_logger.PostEntry("Error running summary.py" & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting

        'Update the status file
        m_StatusTools.UpdateAndWrite(PROGRESS_PCT_INSPECT_RUNNING)

    End Sub

    Private Function CleanOrgDbDir(ByVal CleanDir As String) As Boolean

        'Cleans all files out of the working directory
        Dim FoundFiles() As String
        Dim FoundFolders() As String
        Dim DumName As String

        CleanDir = CheckTerminator(CleanDir)

        FoundFiles = Directory.GetFiles(CleanDir)
        FoundFolders = Directory.GetDirectories(CleanDir)

        'Delete the files
        Try
            For Each DumName In FoundFiles
                'Verify file is not set to readonly
                File.SetAttributes(DumName, File.GetAttributes(DumName) And (Not FileAttributes.ReadOnly))
                File.Delete(DumName)
            Next
        Catch Ex As Exception
            m_logger.PostEntry("clsGlobal.CleanOrgDbDir(), Error deleting files in directory: " & Ex.Message, _
             ILogger.logMsgType.logError, True)
            Return False
        End Try

        'Delete the folders
        Try
            For Each DumName In FoundFolders
                Directory.Delete(DumName, True)
            Next
        Catch Ex As Exception
            m_logger.PostEntry("Error deleting folders in directory: " & Ex.Message, ILogger.logMsgType.logError, True)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Zips concatenated XML output file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function ZipMainOutputFile() As IJobParams.CloseOutType
        Dim TmpFile As String
        Dim FileList() As String
        Dim ZipFileName As String

        Try
            Dim Zipper As New ZipTools(m_WorkDir, m_mgrParams.GetParam("zipprogram"))
            FileList = Directory.GetFiles(m_WorkDir, "*_inspect.txt")
            For Each TmpFile In FileList
                ZipFileName = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(TmpFile)) & ".zip"
                If Not Zipper.MakeZipFile("-fast", ZipFileName, Path.GetFileName(TmpFile)) Then
                    Dim Msg As String = "Error zipping output files, job " & m_JobNum
                    m_logger.PostEntry(Msg, ILogger.logMsgType.logError, LOG_DATABASE)
                    m_message = AppendToComment(m_message, "Error zipping output files")
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Next
        Catch ex As Exception
            Dim Msg As String = "Exception zipping output files, job " & m_JobNum & ": " & ex.Message
            m_logger.PostEntry(Msg, ILogger.logMsgType.logError, LOG_DATABASE)
            m_message = AppendToComment(m_message, "Error zipping output files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Delete the XML output files
        Try
            FileList = Directory.GetFiles(m_WorkDir, "*_inspect.txt")
            For Each TmpFile In FileList
                File.SetAttributes(TmpFile, File.GetAttributes(TmpFile) And (Not FileAttributes.ReadOnly))
                File.Delete(TmpFile)
            Next
        Catch Err As Exception
            m_logger.PostError("Error deleting _inspect.txt file, job " & m_JobNum, Err, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

#End Region

End Class
