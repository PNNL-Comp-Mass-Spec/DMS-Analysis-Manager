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

#Region "Structures"
    Protected Structure udtModInfoType
        Public ModName As String
        Public ModMass As String           ' Storing as a string since reading from a text file and writing to a text file
        Public Residues As String
    End Structure

    Protected Structure udtCachedSpectraCountInfoType
        Public MostRecentSpectrumInfo As String
        Public MostRecentLineNumber As Integer
        Public CachedCount As Integer
    End Structure
#End Region

#Region "Module Variables"
    Protected Const PROGRESS_PCT_INSPECT_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_INSPECT_COMPLETE As Single = 95
    Protected Const PROGRESS_PCT_PROTEINMAPPING_COMPLETE As Single = 96
    Protected Const PROGRESS_PCT_PVALUE_COMPLETE As Single = 97
    Protected Const PROGRESS_PCT_SUMMARY_COMPLETE As Single = 98

    Dim m_UseMzXML As String

    Protected WithEvents CmdRunner As clsRunDosProgram

    Protected mDTADirectory As String
    Protected mSpectraProcessed As Integer = 0

    Protected mCachedSpectraCountInfo As udtCachedSpectraCountInfoType

    Protected mInspectCustomParamFileName As String
    Protected mInspectResultsFilePath As String
    Protected mInspectResultsTempFilePath As String

    Protected WithEvents mResultsFileWatcher As System.IO.FileSystemWatcher
#End Region

#Region "Methods"

    ''' <summary>
    ''' Build inspect input file from base parameter file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function BuildInspectInputFile() As String
        Dim result As String = String.Empty

        ' set up input to reference spectra file, and parameter file
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim ParamFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("parmFileName"))
        Dim orgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        Dim fastaFilename As String = Path.Combine(orgDbDir, m_jobParams.GetParam("generatedFastaName"))
        Dim dbFilename As String = fastaFilename.Replace("fasta", "trie")
        Dim mzXMLFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & ".mzXML")
        Dim Dta_Dir As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_dta")
        Dim inputFilename As String = Path.Combine(WorkingDir, "inspect_input.txt")

        Try
            'add extra lines to the parameter files
            'the parameter file will become the input file for inspect
            Dim inputFile As StreamWriter = New StreamWriter((New System.IO.FileStream(inputFilename, FileMode.Create, FileAccess.Write, FileShare.Read)))

            ' Create an instance of StreamReader to read from a file.
            Dim inputBase As StreamReader = New StreamReader((New System.IO.FileStream(ParamFilename, FileMode.Open, FileAccess.Read, FileShare.Read)))
            Dim paramLine As String

            inputFile.WriteLine("#Spectrum file to search; preferred formats are .mzXML and .mgf")
            If CBool(m_UseMzXML) Then
                inputFile.WriteLine("spectra," & mzXMLFilename)
            Else
                inputFile.WriteLine("spectra," & Dta_Dir)
            End If
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
            Return String.Empty
        End Try

        Return inputFilename

    End Function

    ''' <summary>
    ''' Reads strFilePath and counts the number of spectra files processed; results are returned ByRef in udtCachedSpectraCountInfo
    ''' </summary>
    ''' <param name="strFilePath"></param>
    ''' <param name="udtCachedSpectraCountInfo"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function CountsSpectraInInspectResultsFile(ByVal strFilePath As String, ByRef udtCachedSpectraCountInfo As udtCachedSpectraCountInfoType) As Boolean

        Static blnReadingFile As Boolean = False

        Dim srInFile As System.IO.StreamReader

        Dim strLineIn As String

        Dim intTabIndex1 As Integer
        Dim intTabIndex2 As Integer

        Dim intLinesRead As Integer

        Dim strSpectrumInfoNew As String

        If blnReadingFile Then
            Return True
        End If

        Try

            blnReadingFile = True

            ' Read the contents of strFilePath
            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

            intLinesRead = 0

            If udtCachedSpectraCountInfo.MostRecentLineNumber > 0 Then
                ' Read .MostRecentLineNumber lines
                Do While srInFile.Peek <> -1 AndAlso intLinesRead < udtCachedSpectraCountInfo.MostRecentLineNumber
                    strLineIn = srInFile.ReadLine
                    intLinesRead += 1
                Loop
            Else
                With udtCachedSpectraCountInfo
                    .MostRecentLineNumber = 0
                    .MostRecentSpectrumInfo = String.Empty
                    .CachedCount = 0
                End With
            End If

            Do While srInFile.Peek <> -1
                ' Read the next line
                strLineIn = srInFile.ReadLine

                If strLineIn.Length > 0 Then
                    If intLinesRead = 0 AndAlso strLineIn.StartsWith("#SpectrumFile") Then
                        ' Header line; skip it
                    Else
                        ' Find the second tab in strLineIn

                        intTabIndex1 = strLineIn.IndexOf(ControlChars.Tab)
                        If intTabIndex1 > 0 Then

                            intTabIndex2 = strLineIn.IndexOf(ControlChars.Tab, intTabIndex1 + 1)
                            If intTabIndex2 > 0 Then

                                strSpectrumInfoNew = strLineIn.Substring(0, intTabIndex2)

                                If strSpectrumInfoNew <> udtCachedSpectraCountInfo.MostRecentSpectrumInfo Then
                                    ' New spectrum found
                                    udtCachedSpectraCountInfo.MostRecentSpectrumInfo = String.Copy(strSpectrumInfoNew)
                                    udtCachedSpectraCountInfo.MostRecentLineNumber = intLinesRead
                                    udtCachedSpectraCountInfo.CachedCount += 1
                                End If
                            End If
                        End If
                    End If
                End If

                intLinesRead += 1
            Loop

            blnReadingFile = False

            Console.WriteLine()

        Catch ex As Exception
            ' Log the error, but don't abort processing
            m_logger.PostEntry("Error reading Inspect results file (" & System.IO.Path.GetFileName(strFilePath) & ")", ILogger.logMsgType.logError, True)
            Return False
        Finally
            If Not srInFile Is Nothing Then
                ' Close the input file
                srInFile.Close()
                blnReadingFile = False
            End If
        End Try

        Return True

    End Function
    ''' <summary>
    ''' Override the main function in the analysis manager to by-pass the call
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function CreateAndFilterMSMSSpectra() As IJobParams.CloseOutType
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim settingsFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("SettingsFileName"))
        Dim settings_ini As New PRISM.Files.IniFileReader(settingsFilename, True)
        Dim result As IJobParams.CloseOutType

        mDTADirectory = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_DTA")
        Dim dta_filename As String = ""

        Try
            m_logger.PostEntry("clsAnalysisToolRunnerIN.CreateAndFilterMSMSSpectra(): Use MzXml = " & m_UseMzXML, ILogger.logMsgType.logDebug, True)

            m_UseMzXML = settings_ini.GetIniValue("DtaGenerator", "UseMzXML")
            If m_UseMzXML <> "" Then
                If CBool(m_UseMzXML) = True Then
                    result = CreatemzXMLFile()
                    m_logger.PostEntry("clsAnalysisToolRunnerIN.CreateAndFilterMSMSSpectra(): MzXml file generated successfully", ILogger.logMsgType.logDebug, True)
                Else
                    'Create the DTA files using .CreateAndFilterMSMSSpectra
                    ' Afterwards, move them to a sub directory so Inspect can read them

                    ' Make the .DTAs
                    result = MyBase.CreateAndFilterMSMSSpectra()

                    'Make sure all files have released locks
                    GC.Collect()
                    GC.WaitForPendingFinalizers()
                    System.Threading.Thread.Sleep(250)

                    ' Move the .DTAs

                    Directory.CreateDirectory(mDTADirectory)
                    Dim di As New DirectoryInfo(WorkingDir)
                    Dim fi As FileInfo() = di.GetFiles("*.dta")

                    Dim fiTemp As FileInfo
                    For Each fiTemp In fi
                        Try
                            File.Move(Path.Combine(WorkingDir, fiTemp.Name), Path.Combine(mDTADirectory, fiTemp.Name)) 'dta_filename, destfile)
                        Catch ex As Exception
                            ' Sometimes the file cannot be moved due to an open handle; we'll copy it instead of moving it
                            ' Any straggler .Dta files will get deleted later

                            m_logger.PostEntry("clsGlobal.CreateAndFilterMSMSSpectra(), Error moving file; will try to copy instead", _
                             ILogger.logMsgType.logError, True)

                            File.Copy(Path.Combine(WorkingDir, fiTemp.Name), Path.Combine(mDTADirectory, fiTemp.Name)) 'dta_filename, destfile)
                        End Try
                    Next

                    ' Count the number of .DTAs in mDTADirectory
                    Dim strFiles() As String
                    strFiles = Directory.GetFiles(mDTADirectory, "*.dta")
                    m_DtaCount = strFiles.Length

                    m_logger.PostEntry("clsAnalysisToolRunnerIN.CreateAndFilterMSMSSpectra(): DTA Files generated and moved successfully", ILogger.logMsgType.logDebug, True)
                End If
            End If
        Catch ex As Exception
            m_logger.PostEntry("clsGlobal.CreateAndFilterMSMSSpectra(), Error generating inspect input files: " & ex.Message, _
             ILogger.logMsgType.logError, True)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return result

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
        Dim mzXMLFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & ".mzXML")

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
            m_logger.PostEntry("Error running ReadW.exe" & rawFile & " : " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Open the .mzXML file and determine the number of spectra
        m_DtaCount = ExtractScanCountValueFromMzXML(mzXMLFilename)

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function DeleteMatchingFiles(ByVal strFolder As String, ByVal strFileSpec As String) As Boolean

        ' Deletes .Index and .Trie files from the OrgDb folder

        Dim strFoundFiles() As String
        Dim strFile As String

        Try
            strFolder = CheckTerminator(strFolder)
            strFoundFiles = Directory.GetFiles(strFolder, strFileSpec)
            For Each strFile In strFoundFiles
                'Verify file is not set to readonly
                File.SetAttributes(strFile, File.GetAttributes(strFile) And (Not FileAttributes.ReadOnly))
                File.Delete(strFile)
            Next

        Catch Ex As Exception
            m_logger.PostEntry("clsGlobal.DeleteMatchingFiles(), Error deleting " & strFileSpec & " files in directory" & strFolder & ": " & Ex.Message, _
             ILogger.logMsgType.logError, True)
            Return False
        End Try

        Return True

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
        Dim dbFilename As String = Path.Combine(orgDbDir, m_jobParams.GetParam("generatedFastaName"))
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

    Private Function CreatePeptideToProteinMapping() As IJobParams.CloseOutType
        Dim objPeptideToProteinMapper As PeptideToProteinMapEngine.clsPeptideToProteinMapEngine

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim OrgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        Dim dbFilename As String = Path.Combine(OrgDbDir, m_jobParams.GetParam("generatedFastaName"))

        Dim blnSuccess As Boolean

        Try
            objPeptideToProteinMapper = New PeptideToProteinMapEngine.clsPeptideToProteinMapEngine

            With objPeptideToProteinMapper
                .DeleteInspectTempFiles = True
                .IgnoreILDifferences = False
                .InspectParameterFilePath = System.IO.Path.Combine(WorkingDir, mInspectCustomParamFileName)

                If m_DebugLevel > 2 Then
                    .LogMessagesToFile = True
                    .LogFolderPath = WorkingDir
                Else
                    .LogMessagesToFile = False
                End If

                .MatchPeptidePrefixAndSuffixToProtein = False
                .OutputProteinSequence = False
                .PeptideInputFileFormat = PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants.InspectResultsFile
                .PeptideFileSkipFirstLine = False
                .ProteinInputFilePath = System.IO.Path.Combine(OrgDbDir, dbFilename)
                .SaveProteinToPeptideMappingFile = True
                .SearchAllProteinsForPeptideSequence = True
                .SearchAllProteinsSkipCoverageComputationSteps = True
                .ShowMessages = False
            End With

            blnSuccess = objPeptideToProteinMapper.ProcessFile(mInspectResultsFilePath, WorkingDir, String.Empty, True)

            If Not blnSuccess Then
                m_logger.PostEntry("Error running clsPeptideToProteinMapEngine: " & objPeptideToProteinMapper.GetErrorMessage(), ILogger.logMsgType.logError, LOG_DATABASE)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            m_logger.PostEntry("Error running clsPeptideToProteinMapEngine : " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        m_progress = PROGRESS_PCT_PROTEINMAPPING_COMPLETE
        m_StatusTools.UpdateAndWrite(m_progress)

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
        If System.IO.File.Exists(mzXMLFilename) Then
            System.IO.File.Delete(mzXMLFilename)
        End If

        ' Get rid of Inspect-specific files in the ordbd local directory
        Dim OrgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        If Not DeleteMatchingFiles(OrgDbDir, "*.trie") Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not DeleteMatchingFiles(OrgDbDir, "*.index") Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


        ' Get rid of the _DTA directory
        ' ToDo: Verify that this works
        If Not mDTADirectory Is Nothing AndAlso mDTADirectory.Length > 0 Then
            If System.IO.Directory.Exists(mDTADirectory) Then
                System.IO.Directory.Delete(mDTADirectory, True)
            End If
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
    ''' <summary>
    ''' Reads the modification information defined in strInspectParameterFilePath, storing it in udtModList
    ''' </summary>
    ''' <param name="strInspectParameterFilePath"></param>
    ''' <param name="udtModList"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function ExtractModInfoFromInspectParamFile(ByVal strInspectParameterFilePath As String, ByRef udtModList() As udtModInfoType) As Boolean

        Dim srInFile As System.IO.StreamReader

        Dim strLineIn As String
        Dim strSplitLine As String()

        Dim intModCount As Integer

        Try
            ' Initialize udtModList
            intModCount = 0
            ReDim udtModList(-1)

            If m_DebugLevel > 4 Then
                m_logger.PostEntry("clsAnalysisToolRunnerIN.ExtractModInfoFromInspectParamFile(): Reading " & strInspectParameterFilePath, ILogger.logMsgType.logDebug, True)
            End If

            ' Read the contents of strProteinToPeptideMappingFilePath
            srInFile = New System.IO.StreamReader((New System.IO.FileStream(strInspectParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))

            Do While srInFile.Peek <> -1
                strLineIn = srInFile.ReadLine

                strLineIn = strLineIn.Trim

                If strLineIn.Length > 0 Then

                    If strLineIn.Chars(0) = "#"c Then
                        ' Comment line; skip it
                    ElseIf strLineIn.ToLower.StartsWith("mod") Then
                        ' Modification definition line

                        ' Split the line on commas
                        strSplitLine = strLineIn.Split(","c)

                        If strSplitLine.Length >= 5 AndAlso strSplitLine(0).ToLower.Trim = "mod" Then
                            If udtModList.Length = 0 Then
                                ReDim udtModList(0)
                            ElseIf intModCount >= udtModList.Length Then
                                ReDim Preserve udtModList(udtModList.Length * 2 - 1)
                            End If

                            With udtModList(intModCount)
                                .ModName = strSplitLine(4)
                                .ModMass = strSplitLine(1)
                                .Residues = strSplitLine(2)
                            End With

                            intModCount += 1
                        End If
                    End If
                End If
            Loop

            ' Shrink udtModList to the appropriate length
            ReDim Preserve udtModList(intModCount - 1)

            Console.WriteLine()

        Catch ex As Exception
            m_logger.PostEntry("Error reading the Inspect parameter file (" & System.IO.Path.GetFileName(strInspectParameterFilePath) & ")", ILogger.logMsgType.logError, True)
            Return False
        Finally
            If Not srInFile Is Nothing Then
                srInFile.Close()
            End If
        End Try

        Return True

    End Function

    Private Function ExtractScanCountValueFromMzXML(ByVal strMZXMLFilename As String) As Integer

        Dim intScanCount As Integer

        Dim objMZXmlFile As MSDataFileReader.clsMzXMLFileReader
        Dim objSpectrumInfo As MSDataFileReader.clsSpectrumInfo

        Try
            objMZXmlFile = New MSDataFileReader.clsMzXMLFileReader()

            ' Open the file
            objMZXmlFile.OpenFile(strMZXMLFilename)

            ' Read the first spectrum (required to determine the ScanCount)
            If objMZXmlFile.ReadNextSpectrum(objSpectrumInfo) Then
                intScanCount = objMZXmlFile.ScanCount
            End If

        Catch ex As Exception
            m_logger.PostEntry("Error determining the scan count in the .mzXML file: " & ex.Message, ILogger.logMsgType.logError, True)
            Return 0
        Finally
            If Not objMZXmlFile Is Nothing Then
                objMZXmlFile.CloseFile()
            End If
        End Try

        Return intScanCount

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

    Private Sub InitializeInspectResultsFileWatcher(ByVal strWorkingDir As String, ByVal strOutputFilePath As String)

        Dim strTempFileName As String
        strTempFileName = strOutputFilePath & ".tmp"

        mSpectraProcessed = 0
        With mCachedSpectraCountInfo
            .CachedCount = 0
            .MostRecentLineNumber = 0
            .MostRecentSpectrumInfo = String.Empty
        End With

        mInspectResultsTempFilePath = System.IO.Path.Combine(strWorkingDir, strTempFileName)
        mResultsFileWatcher = New System.IO.FileSystemWatcher()

        With mResultsFileWatcher
            .BeginInit()
            .Path = strWorkingDir
            .IncludeSubdirectories = False
            .Filter = System.IO.Path.GetFileName(strTempFileName)
            .NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
            .EndInit()
            .EnableRaisingEvents = True
        End With

    End Sub

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
    ''' Runs InSpecT tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function OperateAnalysisTool() As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType
        Dim OrgDbName As String = m_jobParams.GetParam("organismDBName")

        m_logger.PostEntry("clsAnalysisToolRunnerIN.OperateAnalysisTool(): Enter " & OrgDbName, ILogger.logMsgType.logDebug, True)

        If CreateIndexedDbFiles() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            If RunInSpecT() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Create the Peptide to Protein map file
                If CreatePeptideToProteinMapping() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
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
    ''' Looks for the inspect _errors.txt file in the working folder.  If present, reads and parses it
    ''' </summary>
    ''' <param name="WorkingDir"></param>
    ''' <param name="errorFilename"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ParseInspectErrorsFile(ByVal WorkingDir As String, ByVal errorFilename As String) As Boolean

        Dim srInFile As System.IO.StreamReader

        Dim strInputFilePath As String
        Dim strLineIn As String

        Dim htMessages As System.Collections.Hashtable

        Try

            If m_DebugLevel > 4 Then
                m_logger.PostEntry("clsAnalysisToolRunnerIN.ParseInspectErrorsFile(): Reading " & errorFilename, ILogger.logMsgType.logDebug, True)
            End If

            strInputFilePath = System.IO.Path.Combine(WorkingDir, errorFilename)

            If Not System.IO.File.Exists(strInputFilePath) Then
                ' File not found; that means no errors occurred
                Return True
            End If

            ' Initialize htMessages
            htMessages = New System.Collections.Hashtable

            ' Read the contents of strInputFilePath
            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strInputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

            Do While srInFile.Peek <> -1
                strLineIn = srInFile.ReadLine

                strLineIn = strLineIn.Trim

                If strLineIn.Length > 0 Then
                    If Not htMessages.Contains(strLineIn) Then
                        htMessages.Add(strLineIn, 1)

                        m_logger.PostEntry("Inspect warning/error: " & strLineIn, ILogger.logMsgType.logWarning, True)

                    End If
                End If
            Loop

            Console.WriteLine()

        Catch ex As Exception
            m_logger.PostEntry("Error reading the Inspect _errors.txt file (" & errorFilename & ")", ILogger.logMsgType.logError, True)
            Return False
        Finally
            If Not srInFile Is Nothing Then
                srInFile.Close()
            End If
        End Try

        Return True

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
        Dim errorFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_error.txt")

        Dim blnSuccess As Boolean = False

        mInspectResultsFilePath = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_inspect.txt")
        mInspectCustomParamFileName = BuildInspectInputFile()
        If mInspectCustomParamFileName.Length = 0 Then
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

        ' Create a file watcher to monitor the .tmp file being created by Inspect
        ' This file is updated after each chunk of 100 spectra are processed
        ' This can be used to compute the percentage of the spectra that have been processed
        InitializeInspectResultsFileWatcher(WorkingDir, mInspectResultsFilePath)

        ' Set up and execute a program runner to run Inspect.exe
        CmdStr = " -i " & mInspectCustomParamFileName & " -o " & mInspectResultsFilePath & " -e " & errorFilename
        m_logger.PostEntry(progLoc & CmdStr, ILogger.logMsgType.logDebug, True)
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "Inspect.exe", True) Then

            If CmdRunner.ExitCode <> 0 Then
                m_logger.PostEntry("Inspect.exe returned a non-zero exit code: " & CmdRunner.ExitCode.ToString, ILogger.logMsgType.logWarning, True)
            Else
                m_logger.PostEntry("Call to Inspect.exe failed (but exit code is 0)", ILogger.logMsgType.logWarning, True)
            End If

            Select Case CmdRunner.ExitCode
                Case -1073741819
                    ' Corresponds to message "{W0010} .\PValue.c:453:Only 182 top-scoring matches for charge state; not recalibrating the p-value curve."
                    ' This is a warning, and not an error
                    blnSuccess = True
                Case -1073741510
                    ' Corresponds to the user pressing Ctrl+Break to stop Inspect
                    blnSuccess = False
                Case Else
                    blnSuccess = False
            End Select
        Else
            blnSuccess = True
        End If

        If Not blnSuccess Then
            m_logger.PostEntry("Error running Inspect.exe : " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            blnSuccess = False
        Else
            m_progress = PROGRESS_PCT_INSPECT_COMPLETE
            m_StatusTools.UpdateAndWrite(m_progress)
            blnSuccess = True
        End If

        If Not mResultsFileWatcher Is Nothing Then
            mResultsFileWatcher.EnableRaisingEvents = False
            mResultsFileWatcher = Nothing
        End If

        ' Parse the _errors.txt file (if it exists) and copy any errors to the analysis manager log
        ParseInspectErrorsFile(WorkingDir, errorFilename)

        If blnSuccess Then
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


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
        Dim fastaFilename As String = Path.Combine(orgDbDir, m_jobParams.GetParam("generatedFastaName"))
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

        ' Possibly required: Update the PTMods.txt file in InspectDir to contain the modification details, as defined in inspect_input.txt
        UpdatePTModsFile(InspectDir, WorkingDir & "\inspect_input.txt")

        'Set up and execute a program runner to run PValue.py
        'to do get value from settings files
        CmdStr = " " & InspectDir & "\PValue.py -r " & inspectFilename & " -s " & pvalDistributionFilename & " -w " & filteredGroupFilename & " -p " & pthresh & " -i -a -d " & dbFilename
        m_logger.PostEntry(progLoc & CmdStr, ILogger.logMsgType.logDebug, True)
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "PValue.py", True) Then
            m_logger.PostEntry("Error running PValue.py : " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        m_progress = PROGRESS_PCT_PVALUE_COMPLETE
        m_StatusTools.UpdateAndWrite(m_progress)

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

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
        Dim fastaFilename As String = Path.Combine(orgDbDir, m_jobParams.GetParam("generatedFastaName"))
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

        'Set up and execute a program runner to run Summary.py
        CmdStr = " " & InspectDir & "\summary.py -r " & filteredGroupFilename & " -d " & dbFilename & " -p 0.1 -w " & filteredHtmlFilename
        m_logger.PostEntry(progLoc & CmdStr, ILogger.logMsgType.logDebug, True)
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "summary.py", True) Then
            m_logger.PostEntry("Error running summary.py : " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        m_progress = PROGRESS_PCT_SUMMARY_COMPLETE
        m_StatusTools.UpdateAndWrite(m_progress)

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
    ''' <summary>
    ''' Assures that the PTMods.txt file in strInspectDir contains the modification info defined in strInspectInputFilePath
    ''' </summary>
    ''' <param name="strInspectDir"></param>
    ''' <param name="strInspectParameterFilePath"></param>
    ''' <remarks></remarks>
    Private Function UpdatePTModsFile(ByVal strInspectDir As String, ByVal strInspectParameterFilePath As String) As Boolean

        Dim srInFile As System.IO.StreamReader
        Dim swOutFile As System.IO.StreamWriter

        Dim intIndex As Integer

        Dim strPTModsFilePath As String
        Dim strPTModsFilePathOld As String
        Dim strPTModsFilePathNew As String

        Dim strLineIn As String
        Dim strSplitLine() As String
        Dim strModName As String

        Dim udtModList() As udtModInfoType
        ReDim udtModList(-1)

        Dim blnModProcessed() As Boolean

        Dim blnMatchFound As Boolean
        Dim blnPrevLineWasBlank As Boolean
        Dim blnDifferenceFound As Boolean

        Try
            If m_DebugLevel > 4 Then
                m_logger.PostEntry("clsAnalysisToolRunnerIN.UpdatePTModsFile(): Enter ", ILogger.logMsgType.logDebug, True)
            End If

            ' Read the mods defined in strInspectInputFilePath
            If ExtractModInfoFromInspectParamFile(strInspectParameterFilePath, udtModList) Then

                If udtModList.Length > 0 Then

                    ' Initialize blnModProcessed()
                    ReDim blnModProcessed(udtModList.Length - 1)

                    ' Read PTMods.txt to make look for the mods in udtModList
                    ' While reading, will create a new file with any required updates

                    strPTModsFilePath = System.IO.Path.Combine(strInspectDir, "PTMods.txt")
                    strPTModsFilePathNew = strPTModsFilePath & ".tmp"

                    If m_DebugLevel > 4 Then
                        m_logger.PostEntry("clsAnalysisToolRunnerIN.UpdatePTModsFile(): Open " & strPTModsFilePath, ILogger.logMsgType.logDebug, True)
                    End If
                    srInFile = New System.IO.StreamReader(New System.IO.FileStream(strPTModsFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                    If m_DebugLevel > 4 Then
                        m_logger.PostEntry("clsAnalysisToolRunnerIN.UpdatePTModsFile(): Create " & strPTModsFilePathNew, ILogger.logMsgType.logDebug, True)
                    End If
                    swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strPTModsFilePathNew, FileMode.Create, FileAccess.Write, FileShare.Read))

                    blnDifferenceFound = False
                    Do While srInFile.Peek <> -1
                        strLineIn = srInFile.ReadLine
                        strLineIn = strLineIn.Trim

                        If strLineIn.Length > 0 Then

                            If strLineIn.Chars(0) = "#"c Then
                                ' Comment line; skip it
                            Else
                                ' Split the line on tabs
                                strSplitLine = strLineIn.Split(ControlChars.Tab)

                                If strSplitLine.Length >= 3 Then
                                    strModName = strSplitLine(0).ToLower

                                    blnMatchFound = False
                                    For intIndex = 0 To udtModList.Length - 1
                                        If udtModList(intIndex).ModName.ToLower = strModName Then
                                            ' Match found
                                            blnMatchFound = True
                                            Exit For
                                        End If
                                    Next

                                    If blnMatchFound Then
                                        If blnModProcessed(intIndex) Then
                                            ' This mod was already processed; don't write the line out again
                                            strLineIn = String.Empty
                                        Else
                                            With udtModList(intIndex)
                                                ' First time we've seen this mod; make sure the mod mass and residues are correct
                                                If strSplitLine(1) <> .ModMass OrElse strSplitLine(2) <> .Residues Then
                                                    ' Mis-match; update the line
                                                    strLineIn = .ModName & ControlChars.Tab & .ModMass & ControlChars.Tab & .Residues

                                                    If m_DebugLevel > 4 Then
                                                        m_logger.PostEntry("clsAnalysisToolRunnerIN.UpdatePTModsFile(): Mod def in PTMods.txt doesn't match required mod def; updating to: " & strLineIn, ILogger.logMsgType.logDebug, True)
                                                    End If

                                                    blnDifferenceFound = True
                                                End If
                                            End With
                                            blnModProcessed(intIndex) = True
                                        End If
                                    End If
                                End If
                            End If
                        End If

                        If blnPrevLineWasBlank AndAlso strLineIn.Length = 0 Then
                            ' Don't write out two blank lines in a row; skip this line
                        Else
                            swOutFile.WriteLine(strLineIn)

                            If strLineIn.Length = 0 Then
                                blnPrevLineWasBlank = True
                            Else
                                blnPrevLineWasBlank = False
                            End If
                        End If

                    Loop

                    ' Close the input file
                    srInFile.Close()

                    ' Look for any unprocessed mods
                    For intIndex = 0 To udtModList.Length - 1
                        If Not blnModProcessed(intIndex) Then
                            With udtModList(intIndex)
                                strLineIn = .ModName & ControlChars.Tab & .ModMass & ControlChars.Tab & .Residues
                            End With
                            swOutFile.WriteLine(strLineIn)

                            blnDifferenceFound = True
                        End If
                    Next

                    ' Close the output file
                    swOutFile.Close()

                    If blnDifferenceFound Then
                        ' Wait 2 seconds, then replace PTMods.txt with strPTModsFilePathNew
                        System.Threading.Thread.Sleep(2000)

                        Try
                            strPTModsFilePathOld = strPTModsFilePath & ".old"
                            If System.IO.File.Exists(strPTModsFilePathOld) Then
                                System.IO.File.Delete(strPTModsFilePathOld)
                            End If

                            System.IO.File.Move(strPTModsFilePath, strPTModsFilePathOld)
                            System.IO.File.Move(strPTModsFilePathNew, strPTModsFilePath)
                        Catch ex As Exception
                            m_logger.PostEntry("Error swapping in the new PTMods.txt file : " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
                            Return False
                        End Try
                    Else
                        ' No difference was found; delete the .tmp file
                        System.Threading.Thread.Sleep(500)
                        Try
                            System.IO.File.Delete(strPTModsFilePathNew)
                        Catch ex As Exception
                            ' Ignore errors here
                        End Try
                    End If

                End If
            End If

        Catch ex As Exception
            m_logger.PostEntry("Error creating the new PTMods.txt file : " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Zips Inspect search result file
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

        'Delete the Inspect search result file
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

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting

        'Update the status file
        m_StatusTools.UpdateAndWrite(m_progress)

    End Sub

    ''' <summary>
    ''' Event handler for mResultsFileWatcher.Changed event
    ''' </summary>
    ''' <param name="sender"></param>
    ''' <param name="e"></param>
    ''' <remarks></remarks>
    Private Sub mResultsFileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mResultsFileWatcher.Changed
        Dim ioFile As System.IO.FileInfo
        Dim sngProgressNew As Single

        ioFile = New System.IO.FileInfo(mInspectResultsTempFilePath)
        If ioFile.Exists AndAlso ioFile.Length > 0 Then
            ' File has been updated

            ' The following can be used to open the file and parse the data to determine how many spectra have been processed
            ' We need to be careful about doing this when Inspect is nearing completion, since we don't want to be reading the file
            '  when it's trying to generate the probabilities and create the final file
            ' Thus, we only read the file if at least 200 spectra remain to be processed

            If mCachedSpectraCountInfo.CachedCount <= m_DtaCount - 200 AndAlso _
               CountsSpectraInInspectResultsFile(mInspectResultsTempFilePath, mCachedSpectraCountInfo) Then
                mSpectraProcessed = mCachedSpectraCountInfo.CachedCount
            Else
                ' Increment mSpectraProcessed (assuming the file is updated after each block of 100 spectra are processed)
                mSpectraProcessed += 100

                If mSpectraProcessed > m_DtaCount Then
                    ' FileWatcher was triggered too many times; decrement mSpectraProcessed by 100
                    mSpectraProcessed -= 100
                End If
            End If

            If m_DtaCount > 0 Then
                sngProgressNew = 100.0! * CSng(mSpectraProcessed / m_DtaCount)
                If sngProgressNew > 100 Then sngProgressNew = 100
                If sngProgressNew > m_progress Then
                    m_progress = sngProgressNew
                Else
                    m_progress -= 1
                    m_progress += 1
                End If
            Else
                m_progress = 0
            End If
        End If
    End Sub

#End Region

End Class
