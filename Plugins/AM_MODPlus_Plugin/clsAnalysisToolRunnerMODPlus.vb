'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/12/2015
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Runtime.InteropServices
Imports System.Threading
Imports System.Xml

''' <summary>
''' Class for running MODPlus
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisToolRunnerMODPlus
    Inherits clsAnalysisToolRunnerBase

#Region "Constants and Enums"

    Protected Const MODPlus_CONSOLE_OUTPUT As String = "MODPlus_ConsoleOutput.txt"
    Protected Const MODPlus_JAR_NAME As String = "modp_pnnl.jar"
    Protected Const TDA_PLUS_JAR_NAME As String = "tda_plus.jar"

    Protected Const PROGRESS_PCT_CONVERTING_MSXML_TO_MGF As Single = 1
    Protected Const PROGRESS_PCT_SPLITTING_MGF As Single = 3
    Protected Const PROGRESS_PCT_MODPLUS_STARTING As Single = 5
    Protected Const PROGRESS_PCT_MODPLUS_COMPLETE As Single = 95
    Protected Const PROGRESS_PCT_COMPUTING_FDR As Single = 96

    Protected Const MGF_SPLIT_PROGRESS_THRESHOLD As Integer = 25

    Protected Const USE_THREADING As Boolean = True

#End Region

#Region "Module Variables"
    Protected mToolVersionWritten As Boolean
    Protected mMODPlusVersion As String

    Protected mMODPlusProgLoc As String
    Protected mConsoleOutputErrorMsg As String

    Protected mLastMgfSplitProgress As DateTime
    Protected mNextMgfSplitProgressThreshold As Integer
    Protected mMgfSplitterErrorMessage As String

    ''' <summary>
    ''' Dictionary of ModPlus instances
    ''' </summary>
    ''' <remarks>Key is core number (1 through NumCores), value is the instance</remarks>
    Protected mMODPlusRunners As Dictionary(Of Integer, clsMODPlusRunner)

#End Region

#Region "Methods"

    ''' <summary>
    ''' Runs MODPlus
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMODPlus.RunTool(): Enter")
            End If

            ' Verify that program files exist

            ' JavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
            Dim javaProgLoc = GetJavaProgLoc()
            If String.IsNullOrEmpty(javaProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Determine the path to the MODPlus program
            mMODPlusProgLoc = DetermineProgramLocation("MODPlus", "MODPlusProgLoc", MODPlus_JAR_NAME)

            If String.IsNullOrWhiteSpace(mMODPlusProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Dim fastaFileIsDecoy As Boolean
            If Not InitializeFastaFile(fastaFileIsDecoy) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Run MODPlus (using multiple threads)
            Dim blnSuccess = StartMODPlus(javaProgLoc)

            If blnSuccess Then
                ' Look for the results file(s)
                blnSuccess = PostProcessMODPlusResults(javaProgLoc, fastaFileIsDecoy)
                If Not blnSuccess Then
                    If String.IsNullOrEmpty(m_message) Then
                        LogError("Unknown error post-processing the MODPlus results")
                    End If
                End If
            End If

            m_progress = PROGRESS_PCT_MODPLUS_COMPLETE

            ' Stop the job timer
            m_StopTime = DateTime.UtcNow

            ' Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If


            ' Make sure objects are released
            Threading.Thread.Sleep(500)        ' 500 msec delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If Not blnSuccess Then
                ' Move the source files and any results to the Failed Job folder
                ' Useful for debugging problems
                CopyFailedResultsToArchiveFolder()
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' MakeResultsFolder handles posting to local log, so set database error message and exit
                LogError("Error making results folder")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MoveResultFiles()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                LogError("Error moving files into results folder")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            LogError("Error in MODPlusPlugin->RunTool", ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Sub AddXMLElement(doc As XmlDocument, elementName As String, attributeName As String, attributeValue As String)
        Dim attributes = New Dictionary(Of String, String)
        attributes.Add(attributeName, attributeValue)

        AddXMLElement(doc, elementName, attributes)
    End Sub

    Private Sub AddXMLElement(doc As XmlDocument, elementName As String, attributes As Dictionary(Of String, String))

        Dim newElem As XmlElement = doc.CreateElement(elementName)

        ' Add the attributes
        For Each attrib In attributes
            Dim newAttr As XmlAttribute = doc.CreateAttribute(attrib.Key)
            newAttr.Value = attrib.Value
            newElem.Attributes.Append(newAttr)
        Next

        ' Add the new element to the search element
        doc.DocumentElement.AppendChild(newElem)

    End Sub
    
    ''' <summary>
    ''' Use MSConvert to convert the .mzXML or .mzML file to a .mgf file
    ''' </summary>
    ''' <param name="fiSpectrumFile"></param>
    ''' <param name="fiMgfFile"></param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Private Function ConvertMsXmlToMGF(fiSpectrumFile As FileInfo, fiMgfFile As FileInfo) As Boolean

        ' Set up and execute a program runner to run MSConvert

        Dim msConvertProgLoc = DetermineProgramLocation("MSConvert", "ProteoWizardDir", "msconvert.exe")
        If String.IsNullOrWhiteSpace(msConvertProgLoc) Then
            If String.IsNullOrWhiteSpace(m_message) Then
                LogError("Manager parameter ProteoWizardDir was not found; cannot run MSConvert.exe")
            End If
            Return False
        End If

        Dim msConvertConsoleOutput = Path.Combine(m_WorkDir, "MSConvert_ConsoleOutput.txt")
        m_jobParams.AddResultFileToSkip(msConvertConsoleOutput)

        Dim cmdStr = " --mgf"
        cmdStr &= " --outfile " & fiMgfFile.FullName
        cmdStr &= " " & PossiblyQuotePath(fiSpectrumFile.FullName)

        If m_DebugLevel >= 1 Then
            ' C:\DMS_Programs\ProteoWizard\msconvert.exe --mgf --outfile Dataset.mgf Dataset.mzML
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msConvertProgLoc & " " & cmdStr)
        End If

        Dim msConvertRunner = New clsRunDosProgram(m_WorkDir)
        AddHandler msConvertRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

        With msConvertRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = msConvertConsoleOutput
        End With

        m_progress = PROGRESS_PCT_CONVERTING_MSXML_TO_MGF

        Dim success = msConvertRunner.RunProgram(msConvertProgLoc, cmdStr, "MSConvert", True)

        If success Then
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSConvert.exe successfully created " & fiMgfFile.Name)
            End If
            Return True
        End If

        Dim msg As String
        msg = "Error running MSConvert"
        m_message = clsGlobal.AppendToComment(m_message, msg)

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg & ", job " & m_JobNum)

        If msConvertRunner.ExitCode <> 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSConvert returned a non-zero exit code: " & msConvertRunner.ExitCode.ToString)
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MSConvert failed (but exit code is 0)")
        End If

        Return False

    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the .mzXML file first)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        Try
            File.Delete(Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION))
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

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="paramFileName"></param>
    ''' <param name="fastaFilePath"></param>
    ''' <param name="mgfFiles"></param>
    ''' <returns>Dictionary where key is the thread number and value is the parameter file path</returns>
    ''' <remarks></remarks>
    Private Function CreateParameterFiles(
      paramFileName As String,
      fastaFilePath As String,
      mgfFiles As IEnumerable(Of FileInfo)) As Dictionary(Of Integer, String)

        Try
            Dim fiParamFile = New FileInfo(Path.Combine(m_WorkDir, paramFileName))
            If Not fiParamFile.Exists Then
                LogError("Parameter file not found by CreateParameterFiles")
                Return New Dictionary(Of Integer, String)
            End If

            Dim doc = New XmlDocument()
            doc.Load(fiParamFile.FullName)

            Dim nodeList As XmlNodeList = doc.SelectNodes("/search/dataset")
            If nodeList.Count > 0 Then
                ' This value will get updated to the correct name later in this function
                nodeList(0).Attributes("local_path").Value = "Dataset_PartX.mgf"
                nodeList(0).Attributes("format").Value = "mgf"
            Else
                ' Match not found; add it
                Dim attributes = New Dictionary(Of String, String)
                attributes.Add("local_path", "Dataset_PartX.mgf")
                attributes.Add("format", "mgf")
                AddXMLElement(doc, "dataset", attributes)
            End If

            nodeList = doc.SelectNodes("/search/database")
            If nodeList.Count > 0 Then
                nodeList(0).Attributes("local_path").Value = fastaFilePath
            Else
                ' Match not found; add it
                AddXMLElement(doc, "database", "local_path", fastaFilePath)
            End If

            Dim reThreadNumber = New Regex("_Part(\d+)\.mgf", RegexOptions.Compiled Or RegexOptions.IgnoreCase)
            Dim paramFileList = New Dictionary(Of Integer, String)

            For Each fiMgfFile In mgfFiles

                Dim reMatch = reThreadNumber.Match(fiMgfFile.Name)
                If Not reMatch.Success Then
                    LogError("RegEx failed to extract the thread number from the MGF file name: " + fiMgfFile.Name)
                    Return New Dictionary(Of Integer, String)
                End If

                Dim threadNumber As Integer
                If Not Integer.TryParse(reMatch.Groups(1).Value, threadNumber) Then
                    LogError("RegEx logic error extracting the thread number from the MGF file name: " + fiMgfFile.Name)
                    Return New Dictionary(Of Integer, String)
                End If

                If paramFileList.ContainsKey(threadNumber) Then
                    LogError("MGFSplitter logic error; duplicate thread number encountered for " + fiMgfFile.Name)
                    Return New Dictionary(Of Integer, String)
                End If

                nodeList = doc.SelectNodes("/search/dataset")
                If nodeList.Count > 0 Then
                    nodeList(0).Attributes("local_path").Value = fiMgfFile.FullName
                    nodeList(0).Attributes("format").Value = "mzxml"
                End If

                Dim paramFilePathCurrent = Path.Combine(fiParamFile.DirectoryName, Path.GetFileNameWithoutExtension(fiParamFile.Name) & "_Part" & threadNumber & ".xml")

                Using objXmlWriter = New XmlTextWriter(New FileStream(paramFilePathCurrent, FileMode.Create, FileAccess.Write, FileShare.Read), New Text.UTF8Encoding(False))
                    objXmlWriter.Formatting = Formatting.Indented
                    objXmlWriter.Indentation = 4

                    doc.WriteTo(objXmlWriter)
                End Using

                paramFileList.Add(threadNumber, paramFilePathCurrent)

                m_jobParams.AddResultFileToSkip(paramFilePathCurrent)

            Next

            Return paramFileList

        Catch ex As Exception
            LogError("Exception in CreateParameterFiles", ex)
            Return New Dictionary(Of Integer, String)
        End Try

    End Function

    Private Function InitializeFastaFile(<Out()> ByRef fastaFileIsDecoy As Boolean) As Boolean

        fastaFileIsDecoy = False

        ' Define the path to the fasta file
        Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")
        Dim fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

        Dim fiFastaFile = New FileInfo(fastaFilePath)

        If Not fiFastaFile.Exists Then
            ' Fasta file not found
            LogError("Fasta file not found: " & fiFastaFile.Name, "Fasta file not found: " & fiFastaFile.FullName)
            Return False
        End If

        Dim decoyFastaFileFlag = m_jobParams.GetParam(clsAnalysisResourcesMODPlus.MOD_PLUS_RUNTIME_PARAM_FASTA_FILE_IS_DECOY, String.Empty)
        Dim paramValue As Boolean
        If Boolean.TryParse(decoyFastaFileFlag, paramValue) Then
            fastaFileIsDecoy = paramValue
        End If
      
        Return True

    End Function

    Private Function ParseDecoyResults(javaProgLoc As String, fiResultsFile As FileInfo) As Boolean

        Dim fiModPlusProgram = New FileInfo(mMODPlusProgLoc)
        Dim tdaPlusProgLoc = Path.Combine(fiModPlusProgram.DirectoryName, TDA_PLUS_JAR_NAME)

        Dim tdaPlusConsoleOutput = Path.Combine(m_WorkDir, "TDA_Plus_ConsoleOutput.txt")
        m_jobParams.AddResultFileToSkip(tdaPlusConsoleOutput)

        Dim fdrThreshold = m_jobParams.GetJobParameter("MODPlusDecoyFilterFDR", "0.01")
        Dim decoyPrefix = m_jobParams.GetJobParameter("MODPlusDecoyPrefix", clsAnalysisResourcesMODPlus.DECOY_PROTEIN_PREFIX)   ' XXX.

        Dim cmdStr = " -jar " & PossiblyQuotePath(tdaPlusProgLoc)
        cmdStr &= " -i " & PossiblyQuotePath(fiResultsFile.FullName)
        cmdStr &= " -fdr " & fdrThreshold
        cmdStr &= " -d " & decoyPrefix

        If m_DebugLevel >= 1 Then
            ' "C:\Program Files\Java\jre8\bin\java.exe" -jar C:\DMS_Programs\MODPlus\tda_plus.jar -i Dataset_modp.txt -fdr 0.01 -d XXX.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, javaProgLoc & " " & cmdStr)
        End If

        Dim tdaPlusRunner = New clsRunDosProgram(m_WorkDir)
        AddHandler tdaPlusRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting
        tdaPlusRunner = New clsRunDosProgram(m_WorkDir)

        With tdaPlusRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = tdaPlusConsoleOutput
        End With

        m_progress = PROGRESS_PCT_COMPUTING_FDR

        Dim success = tdaPlusRunner.RunProgram(javaProgLoc, cmdStr, "TDA_Plus", True)

        If success Then
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "tda_plus.jar complete")
            End If

            ' Confirm that the expected files were created
            Dim baseName = Path.Combine(fiResultsFile.DirectoryName, Path.GetFileNameWithoutExtension(fiResultsFile.Name))

            Dim fiFilteredResults = New FileInfo(baseName & ".id.txt")
            Dim fiPtmTable = New FileInfo(baseName & ".ptm.txt")

            If Not fiFilteredResults.Exists Then
                LogError("TDA_Plus did not create the .id.txt file")
                Return False
            End If

            If Not fiPtmTable.Exists Then
                ' Treat this as a warning
                m_EvalMessage = "TDA_Plus did not create the .ptm.txt file"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
            End If

            Return True

        End If

        Dim msg As String
        msg = "Error running TDA_Plus"
        m_message = clsGlobal.AppendToComment(m_message, msg)

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg & ", job " & m_JobNum)

        If tdaPlusRunner.ExitCode <> 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "TDA_Plus returned a non-zero exit code: " & tdaPlusRunner.ExitCode.ToString)
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to TDA_Plus failed (but exit code is 0)")
        End If

        Return False

    End Function

    Private Function PostProcessMODPlusResults(javaProgLoc As String, fastaFileIsDecoy As Boolean) As Boolean

        Dim successOverall = True

        Try
            Dim lstNextAvailableScan = New SortedList(Of Integer, List(Of clsMODPlusResultsReader))

            ' Combine the result files using a Merge Sort (we assume the results are sorted by scan in each result file)

            For Each modPlusRunner In mMODPlusRunners
                Dim fiOutputFile = New FileInfo(modPlusRunner.Value.OutputFilePath)

                If Not fiOutputFile.Exists Then
                    m_message = clsGlobal.AppendToComment(m_message, "Result file not found for thread " & modPlusRunner.Key)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    successOverall = False
                    Continue For
                End If

                Dim reader = New clsMODPlusResultsReader(fiOutputFile)
                If reader.SpectrumAvailable Then
                    PushReader(lstNextAvailableScan, reader)
                End If
            Next

            Dim fiResultsFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & clsMODPlusRunner.RESULTS_FILE_SUFFIX))

            Using swCombinedResults = New StreamWriter(New FileStream(fiResultsFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))

                While lstNextAvailableScan.Count > 0
                    Dim nextScan = lstNextAvailableScan.First()

                    lstNextAvailableScan.Remove(nextScan.Key)

                    For Each reader In nextScan.Value
                        For Each dataLine In reader.CurrentScanData
                            swCombinedResults.WriteLine(dataLine)
                        Next

                        ' Add a blank line
                        swCombinedResults.WriteLine()

                        If reader.ReadNextSpectrum() Then
                            PushReader(lstNextAvailableScan, reader)
                        End If
                    Next
                End While

            End Using

            For Each modPlusRunner In mMODPlusRunners
                m_jobParams.AddResultFileToSkip(modPlusRunner.Value.OutputFilePath)
            Next

            ' Zip the output file
            Dim blnSuccess = ZipOutputFile(fiResultsFile, "MODPlus")

            If blnSuccess Then
                m_jobParams.AddResultFileToSkip(fiResultsFile.Name)
            ElseIf String.IsNullOrEmpty(m_message) Then
                LogError("Unknown error zipping the MODPlus results")
                Return False
            End If

            If fastaFileIsDecoy Then
                blnSuccess = ParseDecoyResults(javaProgLoc, fiResultsFile)

            End If


            Return successOverall

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception preparing the MODPlus results for zipping: " & ex.Message)
            Return False
        End Try

    End Function

    Protected Sub PushReader(
      lstNextAvailableScan As SortedList(Of Integer, List(Of clsMODPlusResultsReader)),
      reader As clsMODPlusResultsReader)

        Dim readersForValue As List(Of clsMODPlusResultsReader) = Nothing

        If lstNextAvailableScan.TryGetValue(reader.CurrentScan, readersForValue) Then
            readersForValue.Add(reader)
        Else
            readersForValue = New List(Of clsMODPlusResultsReader)
            readersForValue.Add(reader)

            lstNextAvailableScan.Add(reader.CurrentScan, readersForValue)
        End If
    End Sub

    ''' <summary>
    ''' Split the .mgf file into multiple parts
    ''' </summary>
    ''' <param name="fiMgfFile"></param>
    ''' <param name="threadCount"></param>
    ''' <returns>List of newly created .mgf files</returns>
    ''' <remarks>Yses a round-robin splitting</remarks>
    Private Function SplitMGFFiles(fiMgfFile As FileInfo, threadCount As Integer) As List(Of FileInfo)

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Splitting mgf file into " & threadCount & " parts: " & fiMgfFile.Name)
        End If

        mLastMgfSplitProgress = DateTime.UtcNow
        mNextMgfSplitProgressThreshold = MGF_SPLIT_PROGRESS_THRESHOLD
        mMgfSplitterErrorMessage = String.Empty

        Dim splitter = New clsSplitMGFFile()
        AddHandler splitter.ErrorEvent, AddressOf SplitMgfErrorHandler
        AddHandler splitter.WarningEvent, AddressOf SplitMgfWarningHandler
        AddHandler splitter.ProgressUpdate, AddressOf SplitMgfProgressHandler

        Dim mgfFiles = splitter.SplitMgfFile(fiMgfFile.FullName, threadCount, "_Part")

        If mgfFiles.Count = 0 Then
            If String.IsNullOrWhiteSpace(mMgfSplitterErrorMessage) Then
                LogError("SplitMgfFile returned an empty list of files")
            Else
                LogError(mMgfSplitterErrorMessage)
            End If

            Return New List(Of FileInfo)
        End If

        Return mgfFiles

    End Function

    Protected Function StartMODPlus(javaProgLoc As String) As Boolean

        Dim currentTask = "Initializing"

        Try

            ' We will store the MODPlus version info in the database after the header block is written to file MODPlus_ConsoleOutput.txt

            mToolVersionWritten = False
            mMODPlusVersion = String.Empty
            mConsoleOutputErrorMsg = String.Empty

            currentTask = "Determine thread count"

            ' Determine the number of threads
            Dim threadCountText = m_jobParams.GetJobParameter("MODPlusThreads", "90%")
            Dim threadCount As Integer = ParseThreadCount(threadCountText)

            ' Convert the .mzXML or .mzML file to the MGF format
            Dim spectrumFileName = m_Dataset

            Dim msXmlOutputType = m_jobParams.GetJobParameter("MSXMLOutputType", String.Empty)
            If msXmlOutputType.ToLower() = "mzxml" Then
                spectrumFileName &= clsAnalysisResources.DOT_MZXML_EXTENSION
            Else
                spectrumFileName &= clsAnalysisResources.DOT_MZML_EXTENSION
            End If

            currentTask = "Convert .mzML file to MGF"
            
            Dim fiSpectrumFile = New FileInfo(Path.Combine(m_WorkDir, spectrumFileName))
            If Not fiSpectrumFile.Exists Then
                LogError("Spectrum file not found: " + fiSpectrumFile.Name)
                Return False
            End If

            Dim fiMgfFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MGF_EXTENSION))

            Dim success As Boolean = ConvertMsXmlToMGF(fiSpectrumFile, fiMgfFile)
            If Not success Then
                Return False
            End If

            currentTask = "Split the MGF file"

            ' Create one MGF file for each thread
            Dim mgfFiles As List(Of FileInfo) = SplitMGFFiles(fiMgfFile, threadCount)
            If mgfFiles.Count = 0 Then
                If String.IsNullOrWhiteSpace(m_message) Then
                    LogError("Unknown error calling SplitMGFFiles")
                End If
                Return False
            End If

            currentTask = "Lookup job parameters"

            ' Define the path to the fasta file
            ' Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
            Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")
            Dim dbFilename As String = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
            Dim fastaFilePath = Path.Combine(localOrgDbFolder, dbFilename)

            Dim paramFileName = m_jobParams.GetParam("ParmFileName")

            ' Lookup the amount of memory to reserve for Java; default to 3 GB 
            Dim javaMemorySizeMB = m_jobParams.GetJobParameter("MODPlusJavaMemorySize", 3000)
            If javaMemorySizeMB < 512 Then javaMemorySizeMB = 512
            
            currentTask = "Create a parameter file for each thread"

            Dim paramFileList = CreateParameterFiles(paramFileName, fastaFilePath, mgfFiles)

            If paramFileList.Count = 0 Then
                If String.IsNullOrWhiteSpace(m_message) Then
                    LogError("CreateParameterFile returned an empty list in StartMODPlus")
                End If
                Return False
            End If

            currentTask = " Set up and execute a program runner to run each MODPlus instance"

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MODPlus using " & paramFileList.Count & " threads")

            m_progress = PROGRESS_PCT_MODPLUS_STARTING

            mMODPlusRunners = New Dictionary(Of Integer, clsMODPlusRunner)()
            Dim lstThreads As New List(Of Thread)

            For Each paramFile In paramFileList

                Dim threadNum = paramFile.Key

                currentTask = "LaunchingModPlus, thread " & threadNum

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, currentTask)

                Dim modPlusRunner = New clsMODPlusRunner(m_Dataset, threadNum, m_WorkDir, paramFile.Value, javaProgLoc, mMODPlusProgLoc)
                AddHandler modPlusRunner.CmdRunnerWaiting, AddressOf CmdRunner_LoopWaiting
                modPlusRunner.JavaMemorySizeMB = javaMemorySizeMB

                mMODPlusRunners.Add(threadNum, modPlusRunner)

                If m_DebugLevel >= 1 Then
                    ' "C:\Program Files\Java\jre8\bin\java.exe" -Xmx3G -jar C:\DMS_Programs\MODPlus\modp_pnnl.jar -i MODPlus_Params_Part1.xml -o E:\DMS_WorkDir2\Dataset_Part1_modp.txt  > MODPlus_ConsoleOutput_Part1.txt
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, javaProgLoc & " " & modPlusRunner.CommandLineArgs)
                End If

                If USE_THREADING Then
                    Dim newThread As New Thread(New ThreadStart(AddressOf modPlusRunner.StartAnalysis))
                    newThread.Priority = Threading.ThreadPriority.Normal
                    newThread.Start()
                    lstThreads.Add(newThread)
                Else
                    modPlusRunner.StartAnalysis()

                    If modPlusRunner.Status = clsMODPlusRunner.MODPlusRunnerStatusCodes.Failure Then
                        LogError("Error running MODPlus, thread " & threadNum)
                        Return False
                    End If
                End If
            Next

            If USE_THREADING Then
                ' Wait for all of the threads to exit
                ' Run for a maximum of 14 days

                currentTask = "Waiting for all of the threads to exit"

                Dim dtStartTime = DateTime.UtcNow
                Dim completedThreads As New SortedSet(Of Integer)

                While True

                    ' Poll the status of each of the threads

                    Dim stepsComplete = 0
                    Dim progressSum As Double = 0

                    For Each modPlusRunner In mMODPlusRunners
                        Dim eStatus = modPlusRunner.Value.Status
                        If eStatus >= clsMODPlusRunner.MODPlusRunnerStatusCodes.Success Then
                            ' Analysis completed (or failed)
                            stepsComplete += 1

                            If Not completedThreads.Contains(modPlusRunner.Key) Then
                                completedThreads.Add(modPlusRunner.Key)
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MODPlus thread " & modPlusRunner.Key & " is now complete")
                            End If

                        End If

                        progressSum += modPlusRunner.Value.Progress
                    Next

                    Dim subTaskProgress = CSng(progressSum / mMODPlusRunners.Count)
                    Dim updatedProgress = ComputeIncrementalProgress(PROGRESS_PCT_MODPLUS_STARTING, PROGRESS_PCT_MODPLUS_COMPLETE, subTaskProgress)
                    If updatedProgress > m_progress Then
                        ' This progress will get written to the status file and sent to the messaging queue by UpdateStatusRunning()
                        m_progress = updatedProgress
                    End If

                    If stepsComplete >= mMODPlusRunners.Count Then
                        ' All threads are done
                        Exit While
                    End If

                    Thread.Sleep(2000)

                    If DateTime.UtcNow.Subtract(dtStartTime).TotalDays > 14 Then
                        LogError("MODPlus ran for over 14 days; aborting")

                        For Each modPlusRunner In mMODPlusRunners
                            modPlusRunner.Value.AbortProcessingNow()
                        Next

                        Return False
                    End If
                End While
            End If

            Dim blnSuccess = True
            Dim exitCode = 0

            currentTask = "Looking for console output error messages"

            ' Look for any console output error messages
            For Each modPlusRunner In mMODPlusRunners

                Dim progRunner = modPlusRunner.Value.ProgRunner

                If progRunner Is Nothing Then Continue For

                For Each cachedError In progRunner.CachedConsoleErrors
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Thread " & modPlusRunner.Key & ": " & cachedError)
                    blnSuccess = False
                Next

                If progRunner.ExitCode <> 0 AndAlso exitCode = 0 Then
                    exitCode = progRunner.ExitCode
                End If

            Next

            If Not blnSuccess Then
                Dim msg As String
                msg = "Error running MODPlus"
                m_message = clsGlobal.AppendToComment(m_message, Msg)

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg & ", job " & m_JobNum)

                If exitCode <> 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MODPlus returned a non-zero exit code: " & exitCode.ToString())
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MODPlus failed (but exit code is 0)")
                End If

                Return False
            End If

            m_progress = PROGRESS_PCT_MODPLUS_COMPLETE

            m_StatusTools.UpdateAndWrite(m_progress)
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MODPlus Analysis Complete")
            End If

            Return True

        Catch ex As Exception
            LogError("Error in StartMODPlus at " & currentTask, ex)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo(strProgLoc As String) As Boolean

        Dim strToolVersionInfo As String

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        strToolVersionInfo = String.Copy(mMODPlusVersion)

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        Dim fiMODPlusProg = New FileInfo(mMODPlusProgLoc)
        ioToolFiles.Add(fiMODPlusProg)

        ioToolFiles.Add(New FileInfo(Path.Combine(fiMODPlusProg.DirectoryName, "tda_plus.jar")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Private Sub UpdateStatusRunning(sngPercentComplete As Single)
        m_progress = sngPercentComplete
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
    End Sub

#End Region

#Region "Event Handlers"

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting()

        Static dtLastStatusUpdate As DateTime = DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        ' Update the status file (limit the updates to every 5 seconds)
        If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = DateTime.UtcNow
            UpdateStatusRunning(m_progress)
        End If
    End Sub

    Private Sub SplitMgfProgressHandler(progressMessage As String, percentComplete As Integer)
        Dim logProgress = False

        If m_DebugLevel >= 2 Then
            If DateTime.UtcNow.Subtract(mLastMgfSplitProgress).TotalSeconds > 10 Then
                mLastMgfSplitProgress = DateTime.UtcNow
                logProgress = True
            End If
        End If

        If Not logProgress And m_DebugLevel >= 1 Then
            If percentComplete >= mNextMgfSplitProgressThreshold Then
                mNextMgfSplitProgressThreshold += MGF_SPLIT_PROGRESS_THRESHOLD

                While percentComplete >= mNextMgfSplitProgressThreshold
                    mNextMgfSplitProgressThreshold += MGF_SPLIT_PROGRESS_THRESHOLD
                End While

                logProgress = True
            End If
        End If

        If logProgress Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progressMessage & ": " & percentComplete.ToString("0") & "%")
        End If

    End Sub

    Private Sub SplitMgfWarningHandler(strMessage As String)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "SplitMGFFile warning: " & strMessage)
    End Sub

    Private Sub SplitMgfErrorHandler(strMessage As String)
        If String.IsNullOrWhiteSpace(mMgfSplitterErrorMessage) Then
            mMgfSplitterErrorMessage = strMessage
        End If
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "SplitMGFFile error: " & strMessage)
    End Sub

#End Region

End Class
