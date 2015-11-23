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
Imports System.Runtime.Serialization.Formatters
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

            ' JavaProgLoc will typically be "C:\Program Files\Java\jre8\bin\java.exe"
            Dim javaProgLoc = GetJavaProgLoc()
            If String.IsNullOrEmpty(javaProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Determine the path to the MODPlus program
            mMODPlusProgLoc = DetermineProgramLocation("MODPlus", "MODPlusProgLoc", MODPlus_JAR_NAME)

            If String.IsNullOrWhiteSpace(mMODPlusProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If Not InitializeFastaFile() Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Dim paramFileList As Dictionary(Of Integer, String) = Nothing

            ' Run MODPlus (using multiple threads)
            Dim blnSuccess = StartMODPlus(javaProgLoc, paramFileList)

            If blnSuccess Then
                ' Look for the results file(s)
                blnSuccess = PostProcessMODPlusResults(paramFileList)
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

    ''' <summary>
    ''' Add a node given a simple xpath expression, for example "search/database" or "search/parameters/fragment_ion_tol"
    ''' </summary>
    ''' <param name="doc"></param>
    ''' <param name="xpath"></param>
    ''' <param name="attributeName"></param>
    ''' <param name="attributeValue"></param>
    ''' <remarks></remarks>
    Private Sub AddXMLElement(doc As XmlDocument, xpath As String, attributeName As String, attributeValue As String)
        Dim attributes = New Dictionary(Of String, String)
        attributes.Add(attributeName, attributeValue)

        AddXMLElement(doc, xpath, attributes)
    End Sub

    ''' <summary>
    ''' Add a node given a simple xpath expression, for example "search/database" or "search/parameters/fragment_ion_tol"
    ''' </summary>
    ''' <param name="doc"></param>
    ''' <param name="xpath"></param>
    ''' <param name="attributes"></param>
    ''' <remarks></remarks>
    Private Sub AddXMLElement(doc As XmlDocument, xpath As String, attributes As Dictionary(Of String, String))

        MakeXPath(doc, xpath, attributes)

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

            DefineParamfileDatasetAndFasta(doc, fastaFilePath)

            DefineParamMassResolutionSettings(doc)

            Dim paramFileList = CreateThreadParamFiles(fiParamFile, doc, mgfFiles)

            Return paramFileList

        Catch ex As Exception
            LogError("Exception in CreateParameterFiles", ex)
            Return New Dictionary(Of Integer, String)
        End Try

    End Function

    Private Function CreateThreadParamFiles(
       fiMasterParamFile As FileInfo,
       doc As XmlDocument,
       mgfFiles As IEnumerable(Of FileInfo)) As Dictionary(Of Integer, String)

        Dim reThreadNumber = New Regex("_Part(\d+)\.mgf", RegexOptions.Compiled Or RegexOptions.IgnoreCase)
        Dim paramFileList = New Dictionary(Of Integer, String)
        Dim nodeList As XmlNodeList

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
                nodeList(0).Attributes("format").Value = "mgf"
            End If

            Dim paramFileName = Path.GetFileNameWithoutExtension(fiMasterParamFile.Name) & "_Part" & threadNumber & ".xml"
            Dim paramFilePath = Path.Combine(fiMasterParamFile.DirectoryName, paramFileName)

            Using objXmlWriter = New XmlTextWriter(New FileStream(paramFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), New Text.UTF8Encoding(False))
                objXmlWriter.Formatting = Formatting.Indented
                objXmlWriter.Indentation = 4

                doc.WriteTo(objXmlWriter)
            End Using

            paramFileList.Add(threadNumber, paramFilePath)

            m_jobParams.AddResultFileToSkip(paramFilePath)

        Next

        Return paramFileList

    End Function

    Private Sub DefineParamfileDatasetAndFasta(doc As XmlDocument, fastaFilePath As String)

        ' Define the path to the dataset file
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
            AddXMLElement(doc, "/search/dataset", attributes)
        End If

        ' Define the path to the fasta file
        nodeList = doc.SelectNodes("/search/database")
        If nodeList.Count > 0 Then
            nodeList(0).Attributes("local_path").Value = fastaFilePath
        Else
            ' Match not found; add it
            AddXMLElement(doc, "/search/database", "local_path", fastaFilePath)
        End If

    End Sub

    Private Sub DefineParamMassResolutionSettings(doc As XmlDocument)

        Const LOW_RES_FLAG = "low"
        Const HIGH_RES_FLAG = "high"

        Const MIN_FRAG_TOL_LOW_RES = 0.3
        Const DEFAULT_FRAG_TOL_LOW_RES = "0.5"        
        Const DEFAULT_FRAG_TOL_HIGH_RES = "0.05"


        ' Validate the setting for instrument_resolution and fragment_ion_tol

        Dim strDatasetType = m_jobParams.GetParam("JobParameters", "DatasetType")
        Dim instrumentResolutionMsMs = LOW_RES_FLAG

        If strDatasetType.ToLower().EndsWith("hmsn") Then
            instrumentResolutionMsMs = HIGH_RES_FLAG
        End If

        Dim nodeList = doc.SelectNodes("/search/instrument_resolution")
        If nodeList.Count > 0 Then
            If nodeList(0).Attributes("msms").Value = HIGH_RES_FLAG AndAlso instrumentResolutionMsMs = "low" Then
                ' Parameter file lists the resolution as high, but it's actually low
                ' Auto-change it
                nodeList(0).Attributes("msms").Value = instrumentResolutionMsMs
                m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, "Auto-switched to low resolution mode for MS/MS data")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
            End If
        Else
            ' Match not found; add it
            Dim attributes = New Dictionary(Of String, String)
            attributes.Add("ms", HIGH_RES_FLAG)
            attributes.Add("msms", instrumentResolutionMsMs)
            AddXMLElement(doc, "/search/instrument_resolution", attributes)
        End If


        nodeList = doc.SelectNodes("/search/parameters/fragment_ion_tol")
        If nodeList.Count > 0 Then
            If instrumentResolutionMsMs = LOW_RES_FLAG Then
                Dim massTolDa As Double = 0
                If Double.TryParse(nodeList(0).Attributes("value").Value, massTolDa) Then
                    Dim massUnits = nodeList(0).Attributes("unit").Value

                    If massUnits = "ppm" Then
                        ' Convert from ppm to Da
                        massTolDa = massTolDa * 1000 / 1000000
                    End If

                    If massTolDa < MIN_FRAG_TOL_LOW_RES Then
                        m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, "Auto-changed fragment_ion_tol to " & DEFAULT_FRAG_TOL_LOW_RES & " Da since low resolution MS/MS")
                        nodeList(0).Attributes("value").Value = DEFAULT_FRAG_TOL_LOW_RES
                        nodeList(0).Attributes("unit").Value = "da"
                    End If
                End If
            End If

        Else
            ' Match not found; add it
            Dim attributes = New Dictionary(Of String, String)

            If instrumentResolutionMsMs = HIGH_RES_FLAG Then
                attributes.Add("value", DEFAULT_FRAG_TOL_HIGH_RES)
            Else
                attributes.Add("value", DEFAULT_FRAG_TOL_LOW_RES)
            End If

            attributes.Add("unit", "da")
            AddXMLElement(doc, "/search/parameters/fragment_ion_tol", attributes)
        End If

    End Sub

    Private Function InitializeFastaFile() As Boolean

        ' Define the path to the fasta file
        Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")
        Dim fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

        Dim fiFastaFile = New FileInfo(fastaFilePath)

        If Not fiFastaFile.Exists Then
            ' Fasta file not found
            LogError("Fasta file not found: " & fiFastaFile.Name, "Fasta file not found: " & fiFastaFile.FullName)
            Return False
        End If

        Return True

    End Function

    ''' <summary>
    ''' Add a node given a simple xpath expression, for example "search/database" or "search/parameters/fragment_ion_tol"
    ''' </summary>
    ''' <param name="doc"></param>
    ''' <param name="xpath"></param>
    ''' <param name="attributes"></param>
    ''' <returns></returns>
    ''' <remarks>Code adapted from "http://stackoverflow.com/questions/508390/create-xml-nodes-based-on-xpath"</remarks>
    Private Function MakeXPath(doc As XmlDocument, xpath As String, attributes As Dictionary(Of String, String)) As XmlNode
        Return MakeXPath(doc, TryCast(doc, XmlNode), xpath, attributes)
    End Function

    Private Function MakeXPath(doc As XmlDocument, parent As XmlNode, xpath As String, attributes As Dictionary(Of String, String)) As XmlNode

        ' Grab the next node name in the xpath; or return parent if empty
        Dim partsOfXPath As String() = xpath.Trim("/"c).Split("/"c)
        Dim nextNodeInXPath As String = partsOfXPath.First()
        If String.IsNullOrEmpty(nextNodeInXPath) Then
            Return parent
        End If

        ' Get or create the node from the name
        Dim node As XmlNode = parent.SelectSingleNode(nextNodeInXPath)
        If node Is Nothing Then
            Dim newNode = doc.CreateElement(nextNodeInXPath)

            If partsOfXPath.Count = 1 Then
                ' Right-most node in the xpath
                ' Add the attributes
                For Each attrib In attributes
                    Dim newAttr As XmlAttribute = doc.CreateAttribute(attrib.Key)
                    newAttr.Value = attrib.Value
                    newNode.Attributes.Append(newAttr)
                Next
            End If

            node = parent.AppendChild(newNode)
        End If

        If partsOfXPath.Count = 1 Then
            Return node
        Else
            ' Rejoin the remainder of the array as an xpath expression and recurse
            Dim rest As String = String.Join("/", partsOfXPath.Skip(1).ToArray())
            Return MakeXPath(doc, node, rest, attributes)
        End If

    End Function

    Private Function PostProcessMODPlusResults(paramFileList As Dictionary(Of Integer, String)) As Boolean

        Dim successOverall = True

        Try
            ' Keys in this list are scan numbers with charge state encoded as Charge / 100
            ' For example, if scan 1000 and charge 2, then the key will be 1000.02
            ' Values are a list of readers that have that given ScanPlusCharge combo
            Dim lstNextAvailableScan = New SortedList(Of Double, List(Of clsMODPlusResultsReader))

            ' Combine the result files using a Merge Sort (we assume the results are sorted by scan in each result file)

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Merging the results files")
            End If

            For Each modPlusRunner In mMODPlusRunners

                If String.IsNullOrWhiteSpace(modPlusRunner.Value.OutputFilePath) Then
                    Continue For
                End If

                Dim fiResultFile = New FileInfo(modPlusRunner.Value.OutputFilePath)

                If Not fiResultFile.Exists Then
                    ' Result file not found for the current thread
                    ' Log an error, but continue to combine the files
                    m_message = clsGlobal.AppendToComment(m_message, "Result file not found for thread " & modPlusRunner.Key)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    successOverall = False
                    Continue For
                ElseIf fiResultFile.Length = 0 Then
                    ' 0-byte result file
                    ' Log an error, but continue to combine the files
                    m_message = clsGlobal.AppendToComment(m_message, "Result file is empty for thread " & modPlusRunner.Key)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    successOverall = False
                    Continue For
                End If

                Dim reader = New clsMODPlusResultsReader(m_Dataset, fiResultFile)
                If reader.SpectrumAvailable Then
                    PushReader(lstNextAvailableScan, reader)
                End If
            Next

            ' The final results file is named Dataset_modp.txt
            Dim combinedResultsFilePath = Path.Combine(m_WorkDir, m_Dataset & clsMODPlusRunner.RESULTS_FILE_SUFFIX)
            Dim fiCombinedResults = New FileInfo(combinedResultsFilePath)

            Using swCombinedResults = New StreamWriter(New FileStream(fiCombinedResults.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))

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

            ' Zip the output file along with the ConsoleOutput files
            Dim diZipFolder = New DirectoryInfo(Path.Combine(m_WorkDir, "Temp_ZipScratch"))
            If Not diZipFolder.Exists Then diZipFolder.Create()

            Dim filesToMove = New List(Of FileInfo)
            filesToMove.Add(fiCombinedResults)

            Dim diWorkDir = New DirectoryInfo(m_WorkDir)
            filesToMove.AddRange(diWorkDir.GetFiles("*ConsoleOutput*.txt"))

            For Each paramFile In paramFileList
                filesToMove.Add(New FileInfo(paramFile.Value))
            Next

            For Each fiFile In filesToMove
                If fiFile.Exists Then
                    fiFile.MoveTo(Path.Combine(diZipFolder.FullName, fiFile.Name))
                End If
            Next

            Dim zippedResultsFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(fiCombinedResults.Name) & ".zip")
            Dim blnSuccess = m_IonicZipTools.ZipDirectory(diZipFolder.FullName, zippedResultsFilePath)

            If blnSuccess Then
                m_jobParams.AddResultFileToSkip(fiCombinedResults.Name)
            ElseIf String.IsNullOrEmpty(m_message) Then
                LogError("Unknown error zipping the MODPlus results and console output files")
                Return False
            End If

            If successOverall Then
                m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MGF_EXTENSION)
            End If

            Return successOverall

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception preparing the MODPlus results for zipping: " & ex.Message)
            Return False
        End Try

    End Function

    Protected Sub PushReader(
      lstNextAvailableScan As SortedList(Of Double, List(Of clsMODPlusResultsReader)),
      reader As clsMODPlusResultsReader)

        Dim readersForValue As List(Of clsMODPlusResultsReader) = Nothing

        If lstNextAvailableScan.TryGetValue(reader.CurrentScanChargeCombo, readersForValue) Then
            readersForValue.Add(reader)
        Else
            readersForValue = New List(Of clsMODPlusResultsReader)
            readersForValue.Add(reader)

            lstNextAvailableScan.Add(reader.CurrentScanChargeCombo, readersForValue)
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

        m_jobParams.AddResultFileToSkip(fiMgfFile.FullName)

        Return mgfFiles

    End Function

    ''' <summary>
    ''' Run MODPlus
    ''' </summary>
    ''' <param name="javaProgLoc">Path to java.exe</param>
    ''' <param name="paramFileList">Output: Dictionary where key is the thread number and value is the parameter file path</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function StartMODPlus(
      javaProgLoc As String,
      <Out()> ByRef paramFileList As Dictionary(Of Integer, String)) As Boolean

        Dim currentTask = "Initializing"

        paramFileList = New Dictionary(Of Integer, String)

        Try

            ' We will store the MODPlus version info in the database after the header block is written to file MODPlus_ConsoleOutput.txt

            mToolVersionWritten = False
            mMODPlusVersion = String.Empty
            mConsoleOutputErrorMsg = String.Empty

            currentTask = "Determine thread count"

            Dim javaMemorySizeMB = m_jobParams.GetJobParameter("MODPlusJavaMemorySize", 3000)
            Dim maxThreadsToAllow = ComputeMaxThreadsGivenMemoryPerThread(javaMemorySizeMB)

            ' Determine the number of threads
            Dim threadCountText = m_jobParams.GetJobParameter("MODPlusThreads", "90%")
            Dim threadCount As Integer = ParseThreadCount(threadCountText, maxThreadsToAllow)

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

            If fiMgfFile.Exists Then
                ' The .MGF file already exists
                ' This will typically only be true while debugging
            Else

                Dim success As Boolean = ConvertMsXmlToMGF(fiSpectrumFile, fiMgfFile)
                If Not success Then
                    Return False
                End If

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

            currentTask = "Create a parameter file for each thread"

            paramFileList = CreateParameterFiles(paramFileName, fastaFilePath, mgfFiles)

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

                If Not USE_THREADING Then
                    AddHandler modPlusRunner.CmdRunnerWaiting, AddressOf CmdRunner_LoopWaiting
                End If

                modPlusRunner.JavaMemorySizeMB = javaMemorySizeMB

                mMODPlusRunners.Add(threadNum, modPlusRunner)

                If USE_THREADING Then
                    Dim newThread As New Thread(New ThreadStart(AddressOf modPlusRunner.StartAnalysis))
                    newThread.Priority = Threading.ThreadPriority.BelowNormal
                    newThread.Start()
                    lstThreads.Add(newThread)
                Else
                    modPlusRunner.StartAnalysis()

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, javaProgLoc & " " & modPlusRunner.CommandLineArgs)

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
                    Dim processIDFirst = 0

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

                        If processIDFirst = 0 AndAlso modPlusRunner.Value.ProgRunner.ProcessID <> 0 Then
                            processIDFirst = modPlusRunner.Value.ProgRunner.ProcessID
                        End If

                        progressSum += modPlusRunner.Value.Progress

                        If m_DebugLevel >= 1 Then

                            If Not modPlusRunner.Value.CommandLineArgsLogged AndAlso Not String.IsNullOrWhiteSpace(modPlusRunner.Value.CommandLineArgs) Then
                                modPlusRunner.Value.CommandLineArgsLogged = True

                                ' "C:\Program Files\Java\jre8\bin\java.exe" -Xmx3G -jar C:\DMS_Programs\MODPlus\modp_pnnl.jar -i MODPlus_Params_Part1.xml -o E:\DMS_WorkDir2\Dataset_Part1_modp.txt  > MODPlus_ConsoleOutput_Part1.txt
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, javaProgLoc & " " & modPlusRunner.Value.CommandLineArgs)

                            End If

                        End If

                        If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(modPlusRunner.Value.ReleaseDate) Then
                            mMODPlusVersion = modPlusRunner.Value.ReleaseDate
                            mToolVersionWritten = StoreToolVersionInfo(mMODPlusProgLoc)
                        End If
                    Next

                    Dim subTaskProgress = CSng(progressSum / mMODPlusRunners.Count)
                    Dim updatedProgress = ComputeIncrementalProgress(PROGRESS_PCT_MODPLUS_STARTING, PROGRESS_PCT_MODPLUS_COMPLETE, subTaskProgress)
                    If updatedProgress > m_progress Then
                        ' This progress will get written to the status file and sent to the messaging queue by UpdateStatusFile()
                        m_progress = updatedProgress
                    End If

                    CmdRunner_LoopWaiting()

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

                ' One last check for the ToolVersion info being written to the database
                If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(modPlusRunner.Value.ReleaseDate) Then
                    mMODPlusVersion = modPlusRunner.Value.ReleaseDate
                    mToolVersionWritten = StoreToolVersionInfo(mMODPlusProgLoc)
                End If

                Dim progRunner = modPlusRunner.Value.ProgRunner

                If progRunner Is Nothing Then
                    blnSuccess = False
                    If String.IsNullOrWhiteSpace(m_message) Then
                        m_message = "progRunner object is null for thread " & modPlusRunner.Key
                    End If
                    Continue For
                End If

                If Not String.IsNullOrWhiteSpace(progRunner.CachedConsoleErrors) Then
                    Dim consoleError = "Console error for thread " & modPlusRunner.Key & ": " & progRunner.CachedConsoleErrors.Replace(Environment.NewLine, "; ")
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, consoleError)
                    blnSuccess = False
                End If

                If progRunner.ExitCode <> 0 AndAlso exitCode = 0 Then
                    blnSuccess = False
                    exitCode = progRunner.ExitCode
                End If

            Next

            If Not blnSuccess Then
                Dim msg As String
                msg = "Error running MODPlus"
                m_message = clsGlobal.AppendToComment(m_message, msg)

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
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=True)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

#End Region

#Region "Event Handlers"

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting()

        UpdateStatusFile(m_progress)

        LogProgress("MODPlus")

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
