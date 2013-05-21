Option Strict On

Imports AnalysisManagerBase
Imports System.Text.RegularExpressions

Public Class clsCodeTest
	'Imports Protein_Exporter
	'Imports Protein_Exporter.ExportProteinCollectionsIFC


	Protected WithEvents m_FastaTools As Protein_Exporter.ExportProteinCollectionsIFC.IGetFASTAFromDMS
	Protected m_GenerationStarted As Boolean = False
	Protected m_GenerationComplete As Boolean = False
	Protected m_FastaToolsCnStr As String = "Data Source=proteinseqs;Initial Catalog=Protein_Sequences;Integrated Security=SSPI;"
	Protected m_FastaFileName As String = ""
	Protected WithEvents m_FastaTimer As System.Timers.Timer
	Protected m_FastaGenTimeOut As Boolean = False

	Protected m_mgrParams As AnalysisManagerBase.IMgrParams
	Protected m_EvalMessage As String
	Protected m_EvalCode As Integer
	Protected m_DebugLevel As Integer = 2

	Protected m_Progress As Single
	Protected m_MaxScanInFile As Integer
	Private WithEvents mDTAWatcher As System.IO.FileSystemWatcher

	Protected Const FASTA_GEN_TIMEOUT_INTERVAL_SEC As Integer = 450				' 7.5 minutes

	Public Sub New()
		Const CUSTOM_LOG_SOURCE_NAME As String = "Analysis Manager"
		Const CUSTOM_LOG_NAME As String = "DMS_AnalysisMgr"

		' Get settings from config file
		Dim lstMgrSettings As Generic.Dictionary(Of String, String)

		Try
			lstMgrSettings = AnalysisManagerProg.clsMainProcess.LoadMgrSettingsFromFile()

			m_mgrParams = New clsAnalysisMgrSettings(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME, lstMgrSettings, clsGlobal.GetAppFolderPath())

			m_DebugLevel = 2

			m_mgrParams.SetParam("workdir", "E:\DMS_WorkDir")
			m_mgrParams.SetParam("MgrName", "Monroe_Test")
			m_mgrParams.SetParam("debuglevel", m_DebugLevel.ToString)

		Catch ex As Exception
			Console.WriteLine()
			Console.WriteLine("===============================================================")
			Console.WriteLine("Exception loading settings from AnalysisManagerProg.exe.config: " & ex.Message)
			Console.WriteLine("===============================================================")
			Console.WriteLine()
			System.Threading.Thread.Sleep(500)
		End Try


	End Sub

	''' <summary>
	''' Initializes m_mgrParams and returns example job params
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function InitializeManagerParams() As clsAnalysisJob

		Dim intDebugLevel As Integer = 1

		Dim objJobParams As New clsAnalysisJob(m_mgrParams, 0)

		m_mgrParams.SetParam("workdir", "E:\DMS_WorkDir")
		m_mgrParams.SetParam("MgrName", "Monroe_Test")
		m_mgrParams.SetParam("debuglevel", "1")

		objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool")
		objJobParams.SetParam("JobParameters", "ToolName", "TestTool")

		objJobParams.SetParam("StepParameters", "Job", "12345")
		objJobParams.SetParam("StepParameters", "OutputFolderName", "Tst_Results")

		Return objJobParams

	End Function
	'Public Function Test(ByVal DestFolder As String) As Boolean
	'       Dim HashString As String = String.Empty

	'	TestException()
	'	Return False


	'	'Instantiate fasta tool if not already done
	'	If m_FastaTools Is Nothing Then
	'		If m_FastaToolsCnStr = "" Then
	'			Console.WriteLine("Protein database connection string not specified")
	'			Return False
	'		End If
	'		m_FastaTools = New Protein_Exporter.clsGetFASTAFromDMS(m_FastaToolsCnStr)
	'	End If

	'	'Initialize fasta generation state variables
	'	m_GenerationStarted = False
	'	m_GenerationComplete = False

	'	'Set up variables for fasta creation call
	'       Dim LegacyFasta As String = "na"
	'	Dim CreationOpts As String = "seq_direction=forward,filetype=fasta"
	'	Dim CollectionList As String = "Geobacter_bemidjiensis_Bem_T_2006-10-10,Geobacter_lovelyi_SZ_2007-06-19,Geobacter_metallireducens_GS-15_2007-10-02,Geobacter_sp_FRC-32_2007-07-07,Geobacter_sulfurreducens_2006-07-07,Geobacter_uraniumreducens_Rf4_2007-06-19"

	'	' Test what the Protein_Exporter does if a protein collection name is truncated (and thus invalid)
	'	CollectionList = "Geobacter_bemidjiensis_Bem_T_2006-10-10,Geobacter_lovelyi_SZ_2007-06-19,Geobacter_metallireducens_GS-15_2007-10-02,Geobacter_sp_"

	'	'Setup a timer to prevent an infinite loop if there's a fasta generation problem
	'	m_FastaTimer = New System.Timers.Timer
	'	m_FastaTimer.Interval = FASTA_GEN_TIMEOUT_INTERVAL_SEC * 1000
	'	m_FastaTimer.AutoReset = False

	'	'Create the fasta file
	'	m_FastaGenTimeOut = False
	'	Try
	'		m_FastaTimer.Start()
	'           HashString = m_FastaTools.ExportFASTAFile(CollectionList, CreationOpts, LegacyFasta, DestFolder)
	'       Catch ex As Exception
	'           Console.WriteLine("clsAnalysisResources.CreateFastaFile(), Exception generating OrgDb file: ", ex.Message)
	'           Return False
	'	End Try

	'	'Wait for fasta creation to finish
	'	While Not m_GenerationComplete
	'		System.Threading.Thread.Sleep(2000)
	'	End While

	'	If m_FastaGenTimeOut Then
	'		'Fasta generator hung - report error and exit
	'		Console.WriteLine("Timeout error while generating OrdDb file (" & FASTA_GEN_TIMEOUT_INTERVAL_SEC.ToString & " seconds have elapsed)")
	'		Return False
	'	End If

	'	'If we got to here, everything worked OK
	'	Return True

	'End Function

	Public Sub PerformanceCounterTest()
		Try
			' Note that the Memory and Processor performance monitor categories are not 
			' available on Windows instances running under VMWare on PIC
			'Console.WriteLine("Performance monitor categories")
			'Dim perfCats As PerformanceCounterCategory() = PerformanceCounterCategory.GetCategories()
			'For Each category As PerformanceCounterCategory In perfCats.OrderBy(Function(c) c.CategoryName)
			'	Console.WriteLine("Category Name: {0}", category.CategoryName)
			'Next
			'Console.WriteLine()


			Dim mCPUUsagePerformanceCounter As System.Diagnostics.PerformanceCounter
			Dim mFreeMemoryPerformanceCounter As System.Diagnostics.PerformanceCounter

			mCPUUsagePerformanceCounter = New System.Diagnostics.PerformanceCounter("Processor", "% Processor Time", "_Total")
			mCPUUsagePerformanceCounter.ReadOnly = True

			mFreeMemoryPerformanceCounter = New System.Diagnostics.PerformanceCounter("Memory", "Available MBytes")
			mFreeMemoryPerformanceCounter.ReadOnly = True

		Catch ex As Exception
			Console.WriteLine()
			Console.WriteLine("Error in PerformanceCounterTest: " & ex.Message)
			Console.WriteLine(ex.StackTrace)
			Dim rePub1000 As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("Pub-1\d{3,}", RegexOptions.IgnoreCase)
			If rePub1000.IsMatch(Environment.MachineName) Then
				Console.WriteLine("This is a known issue with Windows instances running under VMWare on PIC")
			End If
		End Try
	End Sub

	Public Sub TestArchiveFileStart()
		Dim strParamFilePath As String
		Dim strTargetFolderPath As String

		strParamFilePath = "D:\Temp\sequest_N14_NE.params"
		strTargetFolderPath = "\\gigasax\dms_parameter_Files\Sequest"

		TestArchiveFile(strParamFilePath, strTargetFolderPath)

		'TestArchiveFile("\\n2.emsl.pnl.gov\dmsarch\LCQ_1\LCQ_C1\DR026_dialyzed_7june00_a\Seq200104201154_Auto1001\sequest_Tryp_4_IC.params", strTargetFolderPath)
		'TestArchiveFile("\\proto-4\C1_DMS1\DR026_dialyzed_7june00_a\Seq200104201154_Auto1001\sequest_Tryp_4_IC.params", strTargetFolderPath)

		Console.WriteLine("Done syncing files")

	End Sub

	Public Sub TestArchiveFile(ByVal strSrcFilePath As String, ByVal strTargetFolderPath As String)

		Dim blnNeedToArchiveFile As Boolean
		Dim strTargetFilePath As String
		Dim strLineIgnoreRegExList() As String

		Dim strNewNameBase As String
		Dim strNewName As String
		Dim strNewPath As String

		Dim intRevisionNumber As Integer

		Dim fiArchivedFile As System.IO.FileInfo

		Try
			ReDim strLineIgnoreRegExList(0)
			strLineIgnoreRegExList(0) = "mass_type_parent *=.*"

			blnNeedToArchiveFile = False

			strTargetFilePath = System.IO.Path.Combine(strTargetFolderPath, System.IO.Path.GetFileName(strSrcFilePath))

			If Not System.IO.File.Exists(strTargetFilePath) Then
				blnNeedToArchiveFile = True
			Else

				' Read the files line-by-line and compare
				' Since the first 2 lines of a Sequest parameter file don't matter, and since the 3rd line can vary from computer to computer, we start the comparison at the 4th line

				If Not TextFilesMatch(strSrcFilePath, strTargetFilePath, 4, 0, True, strLineIgnoreRegExList) Then
					' Files don't match; rename the old file

					fiArchivedFile = New System.IO.FileInfo(strTargetFilePath)

					strNewNameBase = System.IO.Path.GetFileNameWithoutExtension(strTargetFilePath) & "_" & fiArchivedFile.LastWriteTime.ToString("yyyy-MM-dd")
					strNewName = strNewNameBase & System.IO.Path.GetExtension(strTargetFilePath)

					' See if the renamed file exists; if it does, we'll have to tweak the name
					intRevisionNumber = 1
					Do
						strNewPath = System.IO.Path.Combine(strTargetFolderPath, strNewName)
						If Not System.IO.File.Exists(strNewPath) Then
							Exit Do
						End If

						intRevisionNumber += 1
						strNewName = strNewNameBase & "_v" & intRevisionNumber.ToString & System.IO.Path.GetExtension(strTargetFilePath)
					Loop

					fiArchivedFile.MoveTo(strNewPath)

					blnNeedToArchiveFile = True
				End If
			End If

			If blnNeedToArchiveFile Then
				' Copy the new parameter file to the archive
				Console.WriteLine("Copying " & System.IO.Path.GetFileName(strSrcFilePath) & " to " & strTargetFilePath)
				System.IO.File.Copy(strSrcFilePath, strTargetFilePath, True)
			End If

		Catch ex As Exception
			Console.WriteLine("Error caught: " & ex.Message)
		End Try

	End Sub

	''' <summary>
	''' Compares two files line-by-line.  If intComparisonStartLine is > 0, then ignores differences up until the given line number.  If 
	''' </summary>
	''' <param name="strFile1">First file</param>
	''' <param name="strFile2">Second file</param>
	''' <param name="intComparisonStartLine">Line at which to start the comparison; if 0 or 1, then compares all lines</param>
	''' <param name="intComparisonEndLine">Line at which to end the comparison; if 0, then compares all the way to the end</param>
	''' <param name="blnIgnoreWhitespace">If true, then removes white space from the beginning and end of each line before compaing</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function TextFilesMatch(ByVal strFile1 As String, ByVal strFile2 As String, _
	   ByVal intComparisonStartLine As Integer, ByVal intComparisonEndLine As Integer, _
	   ByVal blnIgnoreWhitespace As Boolean) As Boolean

		Return TextFilesMatch(strFile1, strFile2, intComparisonStartLine, intComparisonEndLine, blnIgnoreWhitespace, Nothing)

	End Function

	''' <summary>
	''' Compares two files line-by-line.  If intComparisonStartLine is > 0, then ignores differences up until the given line number.  If 
	''' </summary>
	''' <param name="strFile1">First file</param>
	''' <param name="strFile2">Second file</param>
	''' <param name="intComparisonStartLine">Line at which to start the comparison; if 0 or 1, then compares all lines</param>
	''' <param name="intComparisonEndLine">Line at which to end the comparison; if 0, then compares all the way to the end</param>
	''' <param name="blnIgnoreWhitespace">If true, then removes white space from the beginning and end of each line before compaing</param>
	''' <param name="strLineIgnoreRegExList">List of RegEx match specs that indicate lines to ignore</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function TextFilesMatch(ByVal strFile1 As String, ByVal strFile2 As String, _
	 ByVal intComparisonStartLine As Integer, ByVal intComparisonEndLine As Integer, _
	 ByVal blnIgnoreWhitespace As Boolean, _
	 ByRef strLineIgnoreRegExList() As String) As Boolean

		Dim srFile1 As System.IO.StreamReader
		Dim srFile2 As System.IO.StreamReader

		Dim strLineIn1 As String
		Dim strLineIn2 As String

		Dim intIndex As Integer

		Dim chWhiteSpaceChars() As Char
		Dim blnFilesMatch As Boolean
		Dim intLineNumber As Integer = 0

		Dim intLineIgnoreListCount As Integer
		Dim reLineIgnoreList() As System.Text.RegularExpressions.Regex

		ReDim chWhiteSpaceChars(1)
		chWhiteSpaceChars(0) = ControlChars.Tab
		chWhiteSpaceChars(1) = " "c

		blnFilesMatch = True

		Try
			intLineIgnoreListCount = 0
			If Not strLineIgnoreRegExList Is Nothing AndAlso strLineIgnoreRegExList.Length > 0 Then
				ReDim reLineIgnoreList(strLineIgnoreRegExList.Length - 1)

				For intIndex = 0 To strLineIgnoreRegExList.Length - 1
					If Not strLineIgnoreRegExList(intIndex) Is Nothing AndAlso strLineIgnoreRegExList(intIndex).Length > 0 Then
						reLineIgnoreList(intLineIgnoreListCount) = New System.Text.RegularExpressions.Regex(strLineIgnoreRegExList(intIndex), System.Text.RegularExpressions.RegexOptions.Compiled Or System.Text.RegularExpressions.RegexOptions.IgnoreCase)
						intLineIgnoreListCount += 1
					End If
				Next
			Else
				ReDim reLineIgnoreList(0)
			End If

			srFile1 = New System.IO.StreamReader(New System.IO.FileStream(strFile1, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))
			srFile2 = New System.IO.StreamReader(New System.IO.FileStream(strFile2, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

			Do While srFile1.Peek >= 0
				strLineIn1 = srFile1.ReadLine
				intLineNumber += 1

				If intComparisonEndLine > 0 AndAlso intLineNumber > intComparisonEndLine Then
					' No need to compare further; files match up to this point
					Exit Do
				End If

				If srFile2.Peek >= 0 Then
					strLineIn2 = srFile2.ReadLine

					If intLineNumber >= intComparisonStartLine Then
						If blnIgnoreWhitespace Then
							strLineIn1 = strLineIn1.Trim(chWhiteSpaceChars)
							strLineIn2 = strLineIn2.Trim(chWhiteSpaceChars)
						End If

						If strLineIn1 <> strLineIn2 Then
							' Lines don't match; are we ignoring both of them?
							If TextFilesMatchIgnoreLine(strLineIn1, intLineIgnoreListCount, reLineIgnoreList) AndAlso _
							   TextFilesMatchIgnoreLine(strLineIn2, intLineIgnoreListCount, reLineIgnoreList) Then
								' Ignoring both lines
							Else
								blnFilesMatch = False
								Exit Do
							End If
						End If
					End If

				Else
					' File1 has more lines than file2
					If blnIgnoreWhitespace Then
						' Ignoring whitespace
						' If file1 only has blank lines from here on out, then the files match; otherwise, they don't
						' See if the remaining lines are blank
						Do
							If strLineIn1.Length <> 0 Then
								If Not TextFilesMatchIgnoreLine(strLineIn1, intLineIgnoreListCount, reLineIgnoreList) Then
									blnFilesMatch = False
									Exit Do
								End If
							End If

							If srFile1.Peek >= 0 Then
								strLineIn1 = srFile1.ReadLine
								strLineIn1 = strLineIn1.Trim(chWhiteSpaceChars)
							Else
								Exit Do
							End If
						Loop

					Else
						' Not ignoring whitespace; files don't match
						blnFilesMatch = False
					End If

					Exit Do
				End If
			Loop

			If srFile2.Peek >= 0 Then
				' File2 has more lines than file1
				If blnIgnoreWhitespace Then
					' Ignoring whitespace
					' If file2 only has blank lines from here on out, then the files match; otherwise, they don't
					' See if the remaining lines are blank
					Do
						strLineIn2 = srFile2.ReadLine
						strLineIn2 = strLineIn2.Trim(chWhiteSpaceChars)

						If strLineIn2.Length <> 0 Then
							If Not TextFilesMatchIgnoreLine(strLineIn2, intLineIgnoreListCount, reLineIgnoreList) Then
								blnFilesMatch = False
								Exit Do
							End If
						End If
					Loop While srFile2.Peek >= 0

				Else
					' Not ignoring whitespace; files don't match
					blnFilesMatch = False
				End If
			End If


			srFile1.Close()
			srFile2.Close()

		Catch ex As Exception
			' Error occurred
			blnFilesMatch = False
		End Try

		Return blnFilesMatch

	End Function

	Protected Function TextFilesMatchIgnoreLine(ByVal strText As String, ByVal intLineIgnoreListCount As Integer, ByRef reLineIgnoreList() As System.Text.RegularExpressions.Regex) As Boolean

		Dim intIndex As Integer
		Dim blnIgnoreLine As Boolean = False

		If Not reLineIgnoreList Is Nothing Then
			For intIndex = 0 To intLineIgnoreListCount - 1
				If Not reLineIgnoreList(intIndex) Is Nothing Then
					If reLineIgnoreList(intIndex).Match(strText).Success Then
						' Line matches; ignore it
						blnIgnoreLine = True
						Exit For
					End If
				End If
			Next
		End If

		Return blnIgnoreLine

	End Function

	Protected Sub TestException()
		InnerTestException()
	End Sub

	Protected Sub InnerTestException()
		Throw New System.IO.PathTooLongException
	End Sub

	Public Function TestUncat(ByVal rootFileName As String, ByVal strResultsFolder As String) As Boolean
		Console.WriteLine("Splitting concatenated DTA file")

		Dim FileSplitter As New clsSplitCattedFiles()
		FileSplitter.SplitCattedDTAsOnly(rootFileName, strResultsFolder)

		Console.WriteLine("Completed splitting concatenated DTA file")


	End Function

	Public Sub TestDTASplit()
		''Dim intDebugLevel As Integer = 2

		''Dim objToolRunner As clsAnalysisToolRunnerDtaSplit
		''Dim objJobParams As New clsAnalysisJob(m_mgrParams, 0)
		''Dim objStatusTools As New clsStatusFile("Status.xml", intDebugLevel)

		''m_mgrParams.SetParam("workdir", "D:\Temp\DMS_Work")
		''m_mgrParams.SetParam("MgrName", "Monroe_Test")
		''m_mgrParams.SetParam("debuglevel", "2")

		''objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool")
		''objJobParams.SetParam("JobParameters", "ToolName", "TestTool")
		''objJobParams.SetParam("JobParameters", "DatasetNum", "QC_05_2_05Dec05_Doc_0508-08")
		''objJobParams.SetParam("JobParameters", "NumberOfClonedSteps", "25")

		''objJobParams.SetParam("StepParameters", "Job", "12345")
		''objJobParams.SetParam("StepParameters", "OutputFolderName", "Tst_Results")
		''objJobParams.SetParam("JobParameters", "ClonedStepsHaveEqualNumSpectra", "True")

		''objToolRunner = New clsAnalysisToolRunnerDtaSplit
		''objToolRunner.Setup(m_mgrParams, objJobParams, objStatusTools)

		''objToolRunner.RunTool()

	End Sub

	Public Function TestProteinDBExport(ByVal DestFolder As String) As Boolean

		Dim strLegacyFasta As String
		Dim strProteinCollectionList As String
		Dim strProteinOptions As String


		' Test what the Protein_Exporter does if a protein collection name is truncated (and thus invalid)
		strLegacyFasta = "na"
		strProteinCollectionList = "Geobacter_bemidjiensis_Bem_T_2006-10-10,Geobacter_lovelyi_SZ_2007-06-19,Geobacter_metallireducens_GS-15_2007-10-02,Geobacter_sp_"
		strProteinOptions = "seq_direction=forward,filetype=fasta"

		' Test a legacy fasta file:
		strLegacyFasta = "GOs_Surface_Sargasso_Meso_2009-02-11_24.fasta"
		strProteinCollectionList = ""
		strProteinOptions = ""

		' Test a 34 MB fasta file
		strLegacyFasta = "na"
		strProteinCollectionList = "nr_ribosomal_2010-08-17,Tryp_Pig"
		strProteinOptions = "seq_direction=forward,filetype=fasta"

		' Test 100 MB fasta file
		'strLegacyFasta = "na"
		'strProteinCollectionList = "GWB1_Rifle_2011_9_13_0_1_2013-03-27,Tryp_Pig_Bov"
		'strProteinOptions = "seq_direction=forward,filetype=fasta"

		Dim blnSuccess As Boolean
		blnSuccess = TestProteinDBExport(DestFolder, "na", strProteinCollectionList, strProteinOptions)

		If blnSuccess Then

			Dim oJobParams As IJobParams
			oJobParams = InitializeManagerParams()

			Dim blnMsgfPlus As Boolean = True
			Dim strJobNum As String = "12345"
			Dim intDebugLevel As Short = CShort(m_mgrParams.GetParam("debuglevel", 1))

			Dim JavaProgLoc As String = "C:\Program Files\Java\jre7\bin\java.exe"
			Dim MSGFDbProgLoc As String = "C:\DMS_Programs\MSGFDB\MSGFPlus.jar"
			Dim FastaFileIsDecoy As Boolean = False
			Dim FastaFilePath As String = String.Empty

			oJobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", m_FastaFileName)

			'' Note: This won't compile if the AM_Shared project is loaded in the solution
			'Dim oTool As AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils
			'oTool = New AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils(m_mgrParams, oJobParams, strJobNum, m_mgrParams.GetParam("workdir"), intDebugLevel, blnMsgfPlus)

			'Dim FastaFileSizeKB As Single
			'Dim eResult As IJobParams.CloseOutType

			'' Note that FastaFilePath will be populated by this function call
			'eResult = oTool.InitializeFastaFile(JavaProgLoc, MSGFDbProgLoc, FastaFileSizeKB, FastaFileIsDecoy, FastaFilePath)

		End If

	End Function

	Public Function TestProteinDBExport(ByVal DestFolder As String, _
	   ByVal strLegacyFasta As String, _
	   ByVal strProteinCollectionList As String, _
	   ByVal strProteinOptions As String) As Boolean

		Dim HashString As String = String.Empty

		'Instantiate fasta tool if not already done
		If m_FastaTools Is Nothing Then
			If m_FastaToolsCnStr = "" Then
				Console.WriteLine("Protein database connection string not specified")
				Return False
			End If
			m_FastaTools = New Protein_Exporter.clsGetFASTAFromDMS(m_FastaToolsCnStr)
		End If

		'Initialize fasta generation state variables
		m_GenerationStarted = False
		m_GenerationComplete = False

		' Setup a timer to prevent an infinite loop if there's a fasta generation problem
		m_FastaTimer = New System.Timers.Timer
		m_FastaTimer.Interval = FASTA_GEN_TIMEOUT_INTERVAL_SEC * 1000
		m_FastaTimer.AutoReset = False

		' Create the fasta file
		m_FastaGenTimeOut = False
		Try
			m_FastaTimer.Start()
			HashString = m_FastaTools.ExportFASTAFile(strProteinCollectionList, strProteinOptions, strLegacyFasta, DestFolder)
		Catch ex As Exception
			Console.WriteLine("clsAnalysisResources.CreateFastaFile(), Exception generating OrgDb file: ", ex.Message)
			Return False
		End Try

		' Wait for fasta creation to finish
		While Not m_GenerationComplete
			System.Threading.Thread.Sleep(2000)
		End While

		If m_FastaGenTimeOut Then
			'Fasta generator hung - report error and exit
			Console.WriteLine("Timeout error while generating OrdDb file (" & FASTA_GEN_TIMEOUT_INTERVAL_SEC.ToString & " seconds have elapsed)")
			Return False
		End If

		'If we got to here, everything worked OK
		Return True

	End Function

	Public Sub TestDeleteFiles()

		Dim OutFileName As String = "MyTestDataset_out.txt"
		Dim intDebugLevel As Integer = 2

		Dim objToolRunner As clsCodeTestAM
		Dim objJobParams As New clsAnalysisJob(m_mgrParams, 0)
		Dim objStatusTools As New clsStatusFile("Status.xml", intDebugLevel)
		Dim objSummaryFile As New clsSummaryFile()

		m_mgrParams.SetParam("workdir", "E:\DMS_WorkDir")
		m_mgrParams.SetParam("MgrName", "Monroe_Test")
		m_mgrParams.SetParam("debuglevel", "0")

		objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool")
		objJobParams.SetParam("JobParameters", "ToolName", "TestTool")

		objJobParams.SetParam("StepParameters", "Job", "12345")
		objJobParams.SetParam("StepParameters", "OutputFolderName", "Tst_Results")

		objToolRunner = New clsCodeTestAM
		objToolRunner.Setup(m_mgrParams, objJobParams, objStatusTools, objSummaryFile)

		objJobParams.AddResultFileToSkip(OutFileName)

		objToolRunner.RunTool()

	End Sub

	Public Sub TestDeliverResults()

		Dim OutFileName As String = "MyTestDataset_out.txt"
		Dim intDebugLevel As Integer = 2

		Dim objToolRunner As clsCodeTestAM
		Dim objJobParams As New clsAnalysisJob(m_mgrParams, 0)
		Dim objStatusTools As New clsStatusFile("Status.xml", intDebugLevel)
		Dim objSummaryFile As New clsSummaryFile()

		m_mgrParams.SetParam("workdir", "E:\DMS_WorkDir")
		m_mgrParams.SetParam("MgrName", "Monroe_Test")
		m_mgrParams.SetParam("debuglevel", "0")

		objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool")
		objJobParams.SetParam("JobParameters", "ToolName", "TestTool")

		objJobParams.SetParam("StepParameters", "Job", "12345")
		objJobParams.SetParam("StepParameters", "OutputFolderName", "Tst_Results_" & System.DateTime.Now.ToString("hh_mm_ss"))

		objJobParams.SetParam("JobParameters", "transferFolderPath", "\\proto-3\DMS3_XFER")
		objJobParams.SetParam("JobParameters", "DatasetNum", "Test_Dataset")

		objToolRunner = New clsCodeTestAM
		objToolRunner.Setup(m_mgrParams, objJobParams, objStatusTools, objSummaryFile)

		objToolRunner.RunTool()

	End Sub

	Public Sub TestFileDateConversion()
		Dim objTargetFile As System.IO.FileInfo
		Dim strDate As String

		objTargetFile = New System.IO.FileInfo("D:\JobSteps.png")

		strDate = objTargetFile.LastWriteTime.ToString()

		Dim ResultFiles() As String

		ResultFiles = System.IO.Directory.GetFiles("C:\Temp\", "*.*")

		For Each FileToCopy As String In ResultFiles
			Console.WriteLine(FileToCopy)
		Next

		Console.WriteLine(strDate)

	End Sub

	Public Function TestUnzip(ByVal strZipFilePath As String, ByVal strOutFolderPath As String) As Boolean

		Dim intDebugLevel As Integer = 2

		Dim objResources As New clsResourceTestClass

		Dim objJobParams As IJobParams
		objJobParams = New clsAnalysisJob(m_mgrParams, 0)

		Dim objStatusTools As New clsStatusFile("Status.xml", intDebugLevel)
		Dim blnSuccess As Boolean

		m_mgrParams.SetParam("workdir", "E:\DMS_WorkDir")
		m_mgrParams.SetParam("MgrName", "Monroe_Test")
		m_mgrParams.SetParam("debuglevel", "3")
		m_mgrParams.SetParam("zipprogram", "C:\PKWARE\PKZIPC\pkzipc.exe")

		objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool")
		objJobParams.SetParam("JobParameters", "ToolName", "TestTool")

		objJobParams.SetParam("StepParameters", "Job", "12345")
		objJobParams.SetParam("StepParameters", "OutputFolderName", "Tst_Results")

		objResources.Setup(m_mgrParams, objJobParams)

		blnSuccess = objResources.UnzipFileStart(strZipFilePath, strOutFolderPath, "TestUnzip", False)
		'blnSuccess = objResources.UnzipFileStart(strZipFilePath, strOutFolderPath, "TestUnzip", True)

		Return blnSuccess
	End Function

	Public Function TestIonicZipTools() As Boolean
		Dim oIonicZipTools As clsIonicZipTools

		oIonicZipTools = New clsIonicZipTools(1, "E:\DMS_WorkDir")

		oIonicZipTools.UnzipFile("E:\DMS_WorkDir\Temp.zip", "E:\DMS_WorkDir", "*.png")
		For Each item In oIonicZipTools.MostRecentUnzippedFiles
			Console.WriteLine(item.Key & " - " & item.Value)
		Next

	End Function

	Public Function TestMALDIDataUnzip(ByVal strSourceDatasetFolder As String) As Boolean

		Dim intDebugLevel As Integer = 2

		Dim objResources As New clsResourceTestClass

		Dim objJobParams As IJobParams
		objJobParams = New clsAnalysisJob(m_mgrParams, 0)

		Dim objStatusTools As New clsStatusFile("Status.xml", intDebugLevel)
		Dim blnSuccess As Boolean

		If String.IsNullOrEmpty(strSourceDatasetFolder) Then
			strSourceDatasetFolder = "\\proto-4\9T_Imaging_DMS1\ratjoint071110_INCAS_MS"
		End If

		m_mgrParams.SetParam("workdir", "E:\DMS_WorkDir")
		m_mgrParams.SetParam("MgrName", "Monroe_Test")
		m_mgrParams.SetParam("debuglevel", "3")
		m_mgrParams.SetParam("ChameleonCachedDataFolder", "H:\9T_Imaging")

		objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool")
		objJobParams.SetParam("JobParameters", "ToolName", "TestTool")

		objJobParams.SetParam("StepParameters", "Job", "12345")
		objJobParams.SetParam("StepParameters", "OutputFolderName", "Test_Results")
		objJobParams.SetParam("JobParameters", "DatasetNum", "ratjoint071110_INCAS_MS")

		objJobParams.SetParam("JobParameters", "DatasetStoragePath", "\\proto-4\9T_Imaging_DMS1")
		objJobParams.SetParam("JobParameters", "DatasetArchivePath", "\\a2.emsl.pnl.gov\dmsarch\9T_FTICR_Imaging_1")
		objJobParams.SetParam("JobParameters", "transferFolderPath", "\\proto-4\DMS3_Xfer")


		objResources.Setup(m_mgrParams, objJobParams)

		blnSuccess = objResources.RetrieveBrukerMALDIImagingFolders("E:\DMS_WorkDir", True)

		'blnSuccess = objResources.UnzipFileStart(strZipFilePath, strOutFolderPath, "TestUnzip", True)

		Return blnSuccess
	End Function

	Public Sub TestZipAndUnzip()

		Dim objZipper As New clsIonicZipTools(3, "F:\Temp")

		objZipper.ZipFile("F:\Temp\Sarc_P12_D12_1104_148_8Sep11_Cheetah_11-05-34.uimf", False)

		objZipper.ZipFile("F:\Temp\Schutzer_cf_ff_XTandem_AllProt.txt", False, "F:\Temp\TestCustom.zip")

		objZipper.ZipDirectory("F:\Temp\STAC", "F:\Temp\ZippedFolderTest.zip")

		'objZipper.ZipDirectory("F:\Temp\UnzipTest\0_R00X051Y065", "F:\Temp\UnzipTest\0_R00X051Y065.zip", False)

		'      objZipper.ZipDirectory("F:\Temp\UnzipTest\0_R00X051Y065", "F:\Temp\UnzipTest\ZippedFolders2.zip", True, "*.baf*")

		'      objZipper.ZipDirectory("F:\Temp\UnzipTest\0_R00X051Y065", "F:\Temp\UnzipTest\ZippedFolders3.zip", True, "*.ini")

		'      objZipper.UnzipFile("f:\temp\unziptest\StageMD5_Scratch.zip")

		'      objZipper.UnzipFile("F:\Temp\UnzipTest\ZippedFolders.zip", "F:\Temp\UnzipTest\Unzipped")

		'      objZipper.UnzipFile("F:\Temp\UnzipTest\ZippedFolders.zip", "F:\Temp\UnzipTest\Unzipped2", "*.baf*")

		'      objZipper.UnzipFile("F:\Temp\UnzipTest\ZippedFolders.zip", "F:\Temp\UnzipTest\Unzipped3", "*.baf*", Ionic.Zip.ExtractExistingFileAction.DoNotOverwrite)

		'      objZipper.UnzipFile("F:\Temp\UnzipTest\ZippedFolders3.zip", "F:\Temp\UnzipTest\Unzipped4", "*.ini", Ionic.Zip.ExtractExistingFileAction.OverwriteSilently)

		'      objZipper.UnzipFile("F:\Temp\UnzipTest\ZippedFolders3.zip", "F:\Temp\UnzipTest\Unzipped5", "my*.ini", Ionic.Zip.ExtractExistingFileAction.OverwriteSilently)


	End Sub


	Public Function TestFileSplitThenCombine() As Boolean
		Const SYN_FILE_MAX_SIZE_MB As Integer = 200
		Const PEPPROPHET_RESULT_FILE_SUFFIX As String = "_PepProphet.txt"

		Dim SynFile As String
		Dim strSynFileNameAndSize As String

		Dim fiSynFile As System.IO.FileInfo
		Dim Msg As String
		Dim strFileList() As String

		Dim sngParentSynFileSizeMB As Single
		Dim blnSuccess As Boolean

		Dim strBaseName As String
		Dim intFileIndex As Integer
		Dim strPepProphetOutputFilePath As String
		Dim blnIgnorePeptideProphetErrors As Boolean
		blnIgnorePeptideProphetErrors = False

		SynFile = "JGI_Fungus_02_13_8Apr09_Griffin_09-02-12_syn.txt"

		'Check to see if Syn file exists
		fiSynFile = New System.IO.FileInfo(SynFile)
		If Not fiSynFile.Exists Then
			Msg = "clsExtractToolRunner.RunPeptideProphet(); Syn file " & SynFile & " not found; unable to run peptide prophet"
			Console.WriteLine(Msg)
			Return False
		End If

		' Check the size of the Syn file
		' If it is too large, then we will need to break it up into multiple parts, process each part separately, and then combine the results
		sngParentSynFileSizeMB = CSng(fiSynFile.Length / 1024.0 / 1024.0)
		If sngParentSynFileSizeMB <= SYN_FILE_MAX_SIZE_MB Then
			ReDim strFileList(0)
			strFileList(0) = fiSynFile.FullName
		Else
			' File is too large; split it into multiple chunks
			ReDim strFileList(0)
			blnSuccess = SplitFileRoundRobin(fiSynFile.FullName, SYN_FILE_MAX_SIZE_MB * 1024 * 1024, True, strFileList)
		End If


		'Setup Peptide Prophet and run for each file in strFileList
		For intFileIndex = 0 To strFileList.Length - 1
			' Run PeptideProphet

			fiSynFile = New System.IO.FileInfo(strFileList(intFileIndex))
			strSynFileNameAndSize = fiSynFile.Name & " (file size = " & (fiSynFile.Length / 1024.0 / 1024.0).ToString("0.00") & " MB"
			If strFileList.Length > 1 Then
				strSynFileNameAndSize &= "; parent syn file is " & sngParentSynFileSizeMB.ToString("0.00") & " MB)"
			Else
				strSynFileNameAndSize &= ")"
			End If

			If True Then
				' Make sure the Peptide Prophet output file was actually created
				strPepProphetOutputFilePath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(strFileList(intFileIndex)), _
				   System.IO.Path.GetFileNameWithoutExtension(strFileList(intFileIndex)) & _
				   PEPPROPHET_RESULT_FILE_SUFFIX)

				If Not System.IO.File.Exists(strPepProphetOutputFilePath) Then

					Msg = "clsExtractToolRunner.RunPeptideProphet(); Peptide Prophet output file not found for synopsis file " & strSynFileNameAndSize
					''m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)

					If blnIgnorePeptideProphetErrors Then
						''m_logger.PostEntry("Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True", ILogger.logMsgType.logWarning, True)
					Else
						''eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
						Exit For
					End If
				End If
			Else
				Msg = "clsExtractToolRunner.RunPeptideProphet(); Error running Peptide Prophet on file " & strSynFileNameAndSize & _
				   ": "
				''m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)

				If blnIgnorePeptideProphetErrors Then
					''m_logger.PostEntry("Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True", ILogger.logMsgType.logWarning, True)
				Else
					''eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
					Exit For
				End If
			End If

		Next

		If strFileList.Length > 1 Then
			' We now need to recombine the peptide prophet result files

			' Update strFileList() to have the peptide prophet result file names
			strBaseName = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(fiSynFile.FullName), System.IO.Path.GetFileNameWithoutExtension(SynFile))

			For intFileIndex = 0 To strFileList.Length - 1
				strFileList(intFileIndex) = strBaseName & "_part" & (intFileIndex + 1).ToString & PEPPROPHET_RESULT_FILE_SUFFIX
			Next intFileIndex

			' Define the final peptide prophet output file name
			strPepProphetOutputFilePath = strBaseName & PEPPROPHET_RESULT_FILE_SUFFIX

			blnSuccess = InterleaveFiles(strFileList, strPepProphetOutputFilePath, True)

			If blnSuccess Then
				Return True
			Else
				Msg = "Error interleaving the peptide prophet result files (FileCount=" & strFileList.Length & ")"
				If blnIgnorePeptideProphetErrors Then
					Msg &= "; Ignoring the error since 'IgnorePeptideProphetErrors' = True"
					''m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, True)
					Return True
				Else
					''m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
					Return False
				End If
			End If
		End If


	End Function

	Protected Function InterleaveFiles(ByRef strFileList() As String, _
	  ByVal strCombinedFilePath As String, _
	  ByVal blnLookForHeaderLine As Boolean) As Boolean

		Dim Msg As String
		Dim intIndex As Integer

		Dim intFileCount As Integer
		Dim srInFiles() As System.IO.StreamReader
		Dim swOutFile As System.IO.StreamWriter

		Dim strLineIn As String = String.Empty
		Dim strSplitLine() As String

		Dim intFileIndex As Integer
		Dim intLinesRead() As Integer
		Dim intTotalLinesRead As Integer

		Dim intTotalLinesReadSaved As Integer

		Dim blnContinueReading As Boolean
		Dim blnProcessLine As Boolean
		Dim blnSuccess As Boolean

		Try
			If strFileList Is Nothing OrElse strFileList.Length = 0 Then
				' Nothing to do
				Return False
			End If

			intFileCount = strFileList.Length
			ReDim srInFiles(intFileCount - 1)
			ReDim intLinesRead(intFileCount - 1)

			' Open each of the input files
			For intIndex = 0 To intFileCount - 1
				If System.IO.File.Exists(strFileList(intIndex)) Then
					srInFiles(intIndex) = New System.IO.StreamReader(New System.IO.FileStream(strFileList(intIndex), System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
				Else
					' File not found; unable to continue
					Msg = "Source peptide prophet file not found, unable to continue: " & strFileList(intIndex)
					''m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
					Return False
				End If
			Next

			' Create the output file

			swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strCombinedFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

			intTotalLinesRead = 0
			blnContinueReading = True

			Do While blnContinueReading
				intTotalLinesReadSaved = intTotalLinesRead
				For intFileIndex = 0 To intFileCount - 1

					If srInFiles(intFileIndex).Peek >= 0 Then
						strLineIn = srInFiles(intFileIndex).ReadLine

						intLinesRead(intFileIndex) += 1
						intTotalLinesRead += 1

						If Not strLineIn Is Nothing Then
							blnProcessLine = True

							If intLinesRead(intFileIndex) = 1 AndAlso blnLookForHeaderLine AndAlso strLineIn.Length > 0 Then
								' Check for a header line
								strSplitLine = strLineIn.Split(New Char() {ControlChars.Tab}, 2)

								If strSplitLine.Length > 0 AndAlso Not Double.TryParse(strSplitLine(0), 0) Then
									' First column does not contain a number; this must be a header line
									' Write the header to the output file (provided intFileIndex=0)
									If intFileIndex = 0 Then
										swOutFile.WriteLine(strLineIn)
									End If
									blnProcessLine = False
								End If
							End If

							If blnProcessLine Then
								swOutFile.WriteLine(strLineIn)
							End If

						End If
					End If

				Next

				If intTotalLinesRead = intTotalLinesReadSaved Then
					blnContinueReading = False
				End If
			Loop

			' Close the input files
			For intIndex = 0 To intFileCount - 1
				srInFiles(intIndex).Close()
			Next

			' Close the output file
			swOutFile.Close()

			blnSuccess = True


		Catch ex As System.Exception
			Msg = "Exception in clsExtractToolRunner.InterleaveFiles: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			''m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Examines the X!Tndem param file to determine if ETD mode is enabled
	''' If it is, then sets m_ETDMode to True
	''' </summary>
	''' <param name="strParamFilePath">X!Tandem XML parameter file to read</param>
	''' <returns>True if success; false if an error</returns>
	Public Function CheckETDModeEnabledXTandem(ByVal strParamFilePath As String, ByRef blnEtdMode As Boolean) As Boolean

		Dim objParamFile As System.Xml.XmlDocument

		Dim objSelectedNodes As System.Xml.XmlNodeList = Nothing
		Dim objAttributeNode As System.Xml.XmlNode

		Dim intSettingIndex As Integer
		Dim intMatchIndex As Integer

		Try
			blnEtdMode = False

			' Open the parameter file
			' Look for either of these lines:
			'   <note type="input" label="scoring, c ions">yes</note>
			'   <note type="input" label="scoring, z ions">yes</note>

			objParamFile = New System.Xml.XmlDocument
			objParamFile.PreserveWhitespace = True
			objParamFile.Load(strParamFilePath)

			For intSettingIndex = 0 To 1
				Select Case intSettingIndex
					Case 0
						objSelectedNodes = objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, c ions']")
					Case 1
						objSelectedNodes = objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, z ions']")
				End Select

				If Not objSelectedNodes Is Nothing Then

					For intMatchIndex = 0 To objSelectedNodes.Count - 1
						' Make sure this node has an attribute of type="input"
						objAttributeNode = objSelectedNodes.Item(intMatchIndex).Attributes.GetNamedItem("type")

						If objAttributeNode Is Nothing Then
							' Node does not have an attribute named "type"; ignore it
						Else
							If objAttributeNode.Value.ToLower = "input" Then
								' Node does have attribute type="input"
								' Now examine the node's InnerText value
								If objSelectedNodes.Item(intMatchIndex).InnerText.ToLower() = "yes" Then
									blnEtdMode = True
								End If
							End If
						End If
					Next intMatchIndex

				End If

				If blnEtdMode Then Exit For
			Next intSettingIndex

		Catch ex As Exception
			Console.WriteLine("Error: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Reads strSrcFilePath line-by-line and splits into multiple files such that none of the output 
	''' files has length greater than lngMaxSizeBytes. It will also check for a header line on the 
	''' first line; if a header line is found, then all of the split files will be assigned the same header line
	''' </summary>
	''' <param name="strSrcFilePath">FilePath to parse</param>
	''' <param name="lngMaxSizeBytes">Maximum size of each file</param>
	''' <param name="blnLookForHeaderLine">When true, then looks for a header line by checking if the first column contains a number</param>
	''' <param name="strSplitFileList">Output array listing the full paths to the split files that were created</param>
	''' <returns>True if success, False if failure</returns>
	''' <remarks></remarks>
	Private Function SplitFileRoundRobin(ByVal strSrcFilePath As String, _
	 ByVal lngMaxSizeBytes As Int64, _
	 ByVal blnLookForHeaderLine As Boolean, _
	 ByRef strSplitFileList() As String) As Boolean

		Dim fiFileInfo As System.IO.FileInfo
		Dim strBaseName As String

		Dim intLinesRead As Integer = 0
		Dim intTargetFileIndex As Integer

		Dim Msg As String
		Dim strLineIn As String = String.Empty
		Dim strSplitLine() As String

		Dim srInFile As System.IO.StreamReader
		Dim swOutFiles() As System.IO.StreamWriter

		Dim intSplitCount As Integer
		Dim intIndex As Integer

		Dim blnProcessLine As Boolean
		Dim blnSuccess As Boolean = False

		Try
			fiFileInfo = New System.IO.FileInfo(strSrcFilePath)
			If Not fiFileInfo.Exists Then Return False

			If fiFileInfo.Length <= lngMaxSizeBytes Then
				' File is already less than the limit
				ReDim strSplitFileList(0)
				strSplitFileList(0) = fiFileInfo.FullName

				blnSuccess = True
			Else

				' Determine the number of parts to split the file into
				intSplitCount = CInt(Math.Ceiling(fiFileInfo.Length / CDbl(lngMaxSizeBytes)))

				If intSplitCount < 2 Then
					' This code should never be reached; we'll set intSplitCount to 2
					intSplitCount = 2
				End If

				' Open the input file
				srInFile = New System.IO.StreamReader(New System.IO.FileStream(fiFileInfo.FullName, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

				' Create each of the output files
				ReDim strSplitFileList(intSplitCount - 1)
				ReDim swOutFiles(intSplitCount - 1)

				strBaseName = System.IO.Path.Combine(fiFileInfo.DirectoryName, System.IO.Path.GetFileNameWithoutExtension(fiFileInfo.Name))

				For intIndex = 0 To intSplitCount - 1
					strSplitFileList(intIndex) = strBaseName & "_part" & (intIndex + 1).ToString & System.IO.Path.GetExtension(fiFileInfo.Name)
					swOutFiles(intIndex) = New System.IO.StreamWriter(New System.IO.FileStream(strSplitFileList(intIndex), System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))
				Next

				intLinesRead = 0
				intTargetFileIndex = 0

				Do While srInFile.Peek >= 0
					strLineIn = srInFile.ReadLine
					intLinesRead += 1

					If Not strLineIn Is Nothing Then
						blnProcessLine = True

						If intLinesRead = 1 AndAlso blnLookForHeaderLine AndAlso strLineIn.Length > 0 Then
							' Check for a header line
							strSplitLine = strLineIn.Split(New Char() {ControlChars.Tab}, 2)

							If strSplitLine.Length > 0 AndAlso Not Double.TryParse(strSplitLine(0), 0) Then
								' First column does not contain a number; this must be a header line
								' Write the header to each output file
								For intIndex = 0 To intSplitCount - 1
									swOutFiles(intIndex).WriteLine(strLineIn)
								Next
								blnProcessLine = False
							End If
						End If

						If blnProcessLine Then
							swOutFiles(intTargetFileIndex).WriteLine(strLineIn)
							intTargetFileIndex += 1
							If intTargetFileIndex = intSplitCount Then intTargetFileIndex = 0
						End If
					End If
				Loop

				' Close the input file
				srInFile.Close()

				' Close the output files
				For intIndex = 0 To intSplitCount - 1
					swOutFiles(intIndex).Close()
				Next

				blnSuccess = True
			End If


		Catch ex As System.Exception
			Msg = "Exception in clsExtractToolRunner.SplitFileRoundRobin: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			''m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Public Sub TestResultsTransfer()
		Dim strTransferFolderPath As String = "\\proto-5\DMS3_XFER"
		Dim strDatasetFolderPath As String = "\\proto-5\LTQ_Orb1_DMS2"
		Dim strDatasetName As String = "Trmt_hg_03_orbiB_25Jan08_Draco_07-12-24"
		Dim strInputFolderName As String = "DTA_Gen_1_12_142914"

		PerformResultsXfer(strTransferFolderPath, strDatasetFolderPath, strDatasetName, strInputFolderName)
	End Sub

	Protected Overridable Function PerformResultsXfer(ByVal strTransferFolderPath As String, _
	  ByVal strDatasetFolderPath As String, _
	  ByVal strDatasetName As String, _
	  ByVal strInputFolderName As String) As IJobParams.CloseOutType

		Const m_DebugLevel As Integer = 3

		Dim Msg As String
		Dim FolderToMove As String
		Dim DatasetDir As String
		Dim TargetDir As String
		Dim diDatasetFolder As System.IO.DirectoryInfo

		'Verify input folder exists in storage server xfer folder
		FolderToMove = System.IO.Path.Combine(strTransferFolderPath, strDatasetName)
		FolderToMove = System.IO.Path.Combine(FolderToMove, strInputFolderName)
		If Not System.IO.Directory.Exists(FolderToMove) Then
			Msg = "clsResultXferToolRunner.PerformResultsXfer(); results folder " & FolderToMove & " not found"
			'' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		ElseIf m_DebugLevel >= 4 Then
			'' m_logger.PostEntry("Results folder to move: " & FolderToMove, ILogger.logMsgType.logDebug, True)
		End If

		' Verify dataset folder exists on storage server
		' If it doesn't exist, we will auto-create it (this behavior was added 4/24/2009)
		DatasetDir = System.IO.Path.Combine(strDatasetFolderPath, strDatasetName)
		diDatasetFolder = New System.IO.DirectoryInfo(DatasetDir)
		If Not diDatasetFolder.Exists Then
			Msg = "clsResultXferToolRunner.PerformResultsXfer(); dataset folder " & DatasetDir & " not found; will attempt to make it"
			'' m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, clsGlobal.LOG_LOCAL_ONLY)

			Try

				If diDatasetFolder.Parent.Exists Then
					' Parent folder exists; try to create the dataset folder
					diDatasetFolder.Create()

					System.Threading.Thread.Sleep(500)
					diDatasetFolder.Refresh()
					If Not diDatasetFolder.Exists Then
						' Creation of the dataset folder failed; unable to continue
						Msg = "clsResultXferToolRunner.PerformResultsXfer(); error trying to create missing dataset folder " & DatasetDir & ": folder creation failed for unknown reason"
						'' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End If
				Else
					Msg = "clsResultXferToolRunner.PerformResultsXfer(); parent folder not found: " & diDatasetFolder.Parent.FullName & "; unable to continue"
					'' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

			Catch ex As Exception
				Msg = "clsResultXferToolRunner.PerformResultsXfer(); error trying to create missing dataset folder " & DatasetDir & ": " & ex.Message
				'' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)

				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try


		ElseIf m_DebugLevel >= 4 Then
			'' m_logger.PostEntry("Dataset folder path: " & DatasetDir, ILogger.logMsgType.logDebug, True)
		End If

		'Determine if output folder already exists on storage server
		TargetDir = System.IO.Path.Combine(DatasetDir, strInputFolderName)
		If System.IO.Directory.Exists(TargetDir) Then
			Msg = "clsResultXferToolRunner.PerformResultsXfer(); destination directory " & DatasetDir & " already exists"
			'' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Move the directory
		Try
			If m_DebugLevel >= 3 Then
				'' m_logger.PostEntry("Moving '" & FolderToMove & "' to '" & TargetDir & "'", ILogger.logMsgType.logDebug, True)
			End If


			My.Computer.FileSystem.MoveDirectory(FolderToMove, TargetDir, False)

		Catch ex As Exception
			Msg = "clsResultXferToolRunner.PerformResultsXfer(); Exception moving results folder " & FolderToMove & ": " & ex.Message
			'' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	Private Sub m_FastaTools_FileGenerationStarted1(ByVal taskMsg As String) Handles m_FastaTools.FileGenerationStarted

		m_GenerationStarted = True
		m_FastaTimer.Start()	 'Reset the fasta generation timer

	End Sub

	Private Sub m_FastaTools_FileGenerationCompleted(ByVal FullOutputPath As String) Handles m_FastaTools.FileGenerationCompleted

		m_FastaFileName = System.IO.Path.GetFileName(FullOutputPath)		'Get the name of the fasta file that was generated
		m_FastaTimer.Stop()	  'Stop the fasta generation timer so no false error occurs
		m_GenerationComplete = True		'Set the completion flag

	End Sub

	Private Sub m_FastaTools_FileGenerationProgress(ByVal statusMsg As String, ByVal fractionDone As Double) Handles m_FastaTools.FileGenerationProgress

		'Reset the fasta generation timer
		m_FastaTimer.Start()

	End Sub

	Private Sub m_FastaTimer_Elapsed(ByVal sender As Object, ByVal e As System.Timers.ElapsedEventArgs) Handles m_FastaTimer.Elapsed

		'If this event occurs, it means there was a hang during fasta generation and the manager will have to quit
		m_FastaTimer.Stop()		'Stop the timer to prevent false errors
		m_FastaGenTimeOut = True	  'Set the timeout flag so an error will be reported
		m_GenerationComplete = True		'Set the completion flag so the fasta generation wait loop will exit

	End Sub

	Public Sub TestFindAndReplace()
		Dim strTest As String


		Const HPCMaxHours As Double = 2.75
		Const PPN_VALUE As Integer = 8

		Dim HPCNodeCount As String = "3"

		Dim WallTimeMax As Date = CDate("1/1/2010").AddHours(CDbl(HPCMaxHours))
		Dim WallTimeResult As String

		Dim intNodeCount As Integer
		Dim intTotalCores As Integer


		intNodeCount = CInt(HPCNodeCount)
		intTotalCores = intNodeCount * PPN_VALUE

		If intNodeCount = 1 Then
			' Always use a wall-time value of 30 minutes when only using one node
			WallTimeResult = "00:30:00"
		Else
			WallTimeResult = WallTimeMax.ToString("T", System.Globalization.CultureInfo.CreateSpecificCulture("fr-FR"))
			WallTimeResult = WallTimeMax.ToString("HH:mm:ss")
		End If



		Dim NewIDMatchText As String = ""
		Dim NewIDReplaceText As String = ""

		Dim NewLabelMatchText As String = ""
		Dim NewLabelReplaceText As String = ""

		Dim OriginalGroupID As Integer = 7432
		Dim CurrentMaxNum As Integer = 10000

		NewIDMatchText = "id=""" & OriginalGroupID.ToString
		NewIDReplaceText = "id=""" & (OriginalGroupID + CurrentMaxNum).ToString

		NewLabelMatchText = "label=""" & OriginalGroupID.ToString
		NewLabelReplaceText = "label=""" & (OriginalGroupID + CurrentMaxNum).ToString

		strTest = "<group id=""7432"" mh=""1055.228000"" z=""2"" rt="""" expect=""1.1e-01"" label=""SbaltOS185_c39_1:236893-241128 Shewanella_baltica_OS185_contig39 236893..241128"" type=""model"" sumI=""5.75"" maxI=""105413"" fI=""1054.13"" >"
		FindAndReplace(strTest, NewIDMatchText, NewIDReplaceText)
		FindAndReplace(strTest, NewLabelMatchText, NewLabelReplaceText)

		strTest = "<protein expect=""-306.9"" id=""7432.1"" uid=""1471"" label=""SbaltOS185_c39_1:236893-241128 Shewanella_baltica_OS185_contig39 236893..241128"" sumI=""7.12"" >"
		FindAndReplace(strTest, NewIDMatchText, NewIDReplaceText)
		FindAndReplace(strTest, NewLabelMatchText, NewLabelReplaceText)

		strTest = "<GAML:Xdata label=""7432.hyper"" units=""score"">"
		FindAndReplace(strTest, NewIDMatchText, NewIDReplaceText)
		FindAndReplace(strTest, NewLabelMatchText, NewLabelReplaceText)

	End Sub

	Public Sub TestFindFile()
		Dim strFolderPath As String
		Dim strFileName As String
		Dim strPath As String

		strFolderPath = "\\proto-3\12T_DMS3\111410_blank_H061010A_am_000001\111410_blank_H061010A_am_000001.d"
		strFileName = "apexAcquisition.method"

		strPath = clsAnalysisResources.FindFileInDirectoryTree(strFolderPath, strFileName)

	End Sub

	Protected Sub FindAndReplace(ByRef lineText As String, ByRef strOldValue As String, ByRef strNewValue As String)
		Dim intMatchIndex As Integer

		intMatchIndex = lineText.IndexOf(strOldValue)

		If intMatchIndex > 0 Then
			lineText = lineText.Substring(0, intMatchIndex) + strNewValue + lineText.Substring(intMatchIndex + strOldValue.Length)
		ElseIf intMatchIndex = 0 Then
			lineText = strNewValue + lineText.Substring(intMatchIndex + strOldValue.Length)
		End If
	End Sub

	Public Sub TestProgRunner()

		Dim strAppPath As String

		Dim strWorkDir As String
		Dim blnSuccess As Boolean


		strAppPath = "F:\My Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\AM_Program\bin\XTandem\tandem.exe"

		strWorkDir = System.IO.Path.GetDirectoryName(strAppPath)

		Dim objProgRunner As clsRunDosProgram

		objProgRunner = New clsRunDosProgram(strWorkDir)

		With objProgRunner
			.CacheStandardOutput = True
			.CreateNoWindow = True
			.EchoOutputToConsole = True
			.WriteConsoleOutputToFile = True

			.DebugLevel = 1
			.MonitorInterval = 1000
		End With

		blnSuccess = objProgRunner.RunProgram( _
		 strAppPath, _
		 "input.xml", "X!Tandem", False)


		If objProgRunner.CacheStandardOutput And Not objProgRunner.EchoOutputToConsole Then
			Console.WriteLine(objProgRunner.CachedConsoleOutput)
		End If

		If objProgRunner.CachedConsoleError.Length > 0 Then
			Console.WriteLine("Console error output")
			Console.WriteLine(objProgRunner.CachedConsoleError)
		End If

		Console.WriteLine()


	End Sub

	Sub TestProgRunnerIDPicker()

		Dim m_WorkDir As String = "E:\dms_workdir"
		Dim strConsoleOutputFileName As String = ""
		Dim blnWriteConsoleOutputFileRealtime As Boolean = False
		Dim blnSuccess As Boolean

		Dim strExePath As String = "C:\DMS_Programs\IDPicker\idpQonvert.exe"
		Dim CmdStr As String = "-MaxFDR 0.1 -ProteinDatabase C:\DMS_Temp_Org\ID_003521_89E56851.fasta -SearchScoreWeights ""msgfspecprob -1"" -OptimizeScoreWeights 1 -NormalizedSearchScores msgfspecprob -DecoyPrefix Reversed_ -dump E:\DMS_WorkDir\Malaria844_msms_29Dec11_Draco_11-10-04.pepXML"
		Dim strProgramDescription As String = "IDPQonvert"

		Dim CmdRunner As clsRunDosProgram

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = False
			.EchoOutputToConsole = False
			If String.IsNullOrEmpty(strConsoleOutputFileName) OrElse Not blnWriteConsoleOutputFileRealtime Then
				.CacheStandardOutput = False
				.WriteConsoleOutputToFile = False
			Else
				.CacheStandardOutput = False
				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, strConsoleOutputFileName)
			End If
		End With

		blnSuccess = CmdRunner.RunProgram(strExePath, CmdStr, strProgramDescription, True)

		Console.WriteLine(blnSuccess)

	End Sub

	''' <summary>
	''' Look for the .PEK and .PAR files in the specified folder
	''' Make sure they are named Dataset_m_dd_yyyy.PAR andDataset_m_dd_yyyy.Pek
	''' </summary>
	''' <param name="strFolderPath">Folder to examine</param>
	''' <param name="strDatasetName">Dataset name</param>
	''' <remarks></remarks>
	Public Sub FixICR2LSResultFileNames(ByVal strFolderPath As String, ByVal strDatasetName As String)

		Dim objExtensionsToCheck As New Generic.List(Of String)

		Dim fiFolder As System.IO.DirectoryInfo
		Dim fiFile As System.IO.FileInfo

		Dim strDSNameLCase As String
		Dim strExtension As String

		Dim strDesiredName As String

		Try

			objExtensionsToCheck.Add("PAR")
			objExtensionsToCheck.Add("Pek")

			strDSNameLCase = strDatasetName.ToLower()

			fiFolder = New System.IO.DirectoryInfo(strFolderPath)

			If fiFolder.Exists Then
				For Each strExtension In objExtensionsToCheck

					For Each fiFile In fiFolder.GetFiles("*." & strExtension)
						If fiFile.Name.ToLower.StartsWith(strDSNameLCase) Then
							strDesiredName = strDatasetName & "_" & System.DateTime.Now.ToString("M_d_yyyy") & "." & strExtension

							If fiFile.Name.ToLower <> strDesiredName.ToLower Then
								Try
									fiFile.MoveTo(System.IO.Path.Combine(fiFolder.FullName, strDesiredName))
								Catch ex As Exception
									' Rename failed; that means the correct file already exists; this is OK
								End Try

							End If

							Exit For
						End If
					Next fiFile

				Next strExtension

			End If


		Catch ex As Exception
			' Ignore errors here
		End Try

	End Sub

	Public Sub SystemMemoryUsage()

		' The following reports various memory stats
		' However, it doesn't report the available physical memory

		'Try

		'	Dim winQuery As System.Management.ObjectQuery
		'	Dim searcher As System.Management.ManagementObjectSearcher

		'	winQuery = New System.Management.ObjectQuery("SELECT * FROM Win32_LogicalMemoryConfiguration")

		'	searcher = New System.Management.ManagementObjectSearcher(winQuery)

		'	For Each item As System.Management.ManagementObject In searcher.Get()
		'		Console.WriteLine("Total Space = " & item("TotalPageFileSpace").ToString)
		'		Console.WriteLine("Total Physical Memory = " & item("TotalPhysicalMemory").ToString)
		'		Console.WriteLine("Total Virtual Memory = " & item("TotalVirtualMemory").ToString)
		'		Console.WriteLine("Available Virtual Memory = " & item("AvailableVirtualMemory").ToString)
		'	Next
		'Catch ex As Exception
		'	Console.WriteLine()
		'	Console.WriteLine("Error in SystemMemoryUsage (A): " & ex.Message)
		'	Console.WriteLine(ex.StackTrace)

		'End Try

		Dim mFreeMemoryPerformanceCounter As System.Diagnostics.PerformanceCounter
		Dim sngFreeMemoryMB As Single

		Try
			mFreeMemoryPerformanceCounter = New System.Diagnostics.PerformanceCounter("Memory", "Available MBytes")
			mFreeMemoryPerformanceCounter.ReadOnly = True

			sngFreeMemoryMB = mFreeMemoryPerformanceCounter.NextValue()

			Console.WriteLine()
			Console.WriteLine("Available memory (MB) = " & sngFreeMemoryMB.ToString)

		Catch ex As Exception


			Console.WriteLine()
			Console.WriteLine("Error in SystemMemoryUsage (C): " & ex.Message)
			Console.WriteLine(ex.StackTrace)


			Dim rePub1000 As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("Pub-1\d{3,}", RegexOptions.IgnoreCase)
			If rePub1000.IsMatch(Environment.MachineName) Then
				' The Memory performance counters are not available on Windows instances running under VMWare on PIC
			Else

				' To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
				' A possible fix for this is to add the user who is running this process to the "Performance Monitor Users" group 
				' in "Local Users and Groups" on the machine showing this error.  Alternatively, add the user to the "Administrators" group.  
				' In either case, you will need to reboot the computer for the change to take effect
				If System.DateTime.Now().Hour = 0 And System.DateTime.Now().Minute <= 30 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error instantiating the Memory.[Available MBytes] performance counter (this message is only logged between 12 am and 12:30 am): " & ex.Message)
				End If
			End If

			Try
				Dim oInfo As Microsoft.VisualBasic.Devices.ComputerInfo
				oInfo = New Microsoft.VisualBasic.Devices.ComputerInfo

				sngFreeMemoryMB = CSng(oInfo.AvailablePhysicalMemory / 1024.0 / 1024.0)
				Console.WriteLine("Available memory from VB: " & sngFreeMemoryMB & " MB")

			Catch ex2 As Exception
				Console.WriteLine()
				Console.WriteLine("Error in SystemMemoryUsage using Microsoft.VisualBasic.Devices.ComputerInfo: " & ex2.Message)
				Console.WriteLine(ex2.StackTrace)
			End Try

		End Try


	End Sub

	Public Sub TestDTAWatcher(ByVal strWorkDir As String, ByVal sngWaitTimeMinutes As Single)

		m_Progress = 0
		m_MaxScanInFile = 10000

		' Setup a FileSystemWatcher to watch for new .Dta files being created
		' We can compare the scan number of new .Dta files to the m_MaxScanInFile value to determine % complete
		mDTAWatcher = New System.IO.FileSystemWatcher(strWorkDir, "*.dta")

		mDTAWatcher.IncludeSubdirectories = False
		mDTAWatcher.NotifyFilter = IO.NotifyFilters.FileName Or IO.NotifyFilters.CreationTime

		mDTAWatcher.EnableRaisingEvents = True

		Dim dtStartTime As System.DateTime = System.DateTime.UtcNow

		Do
			System.Threading.Thread.Sleep(2000)
			Console.WriteLine("Current progress: " & m_Progress)
		Loop While System.DateTime.UtcNow.Subtract(dtStartTime).TotalMinutes < sngWaitTimeMinutes

	End Sub

	Private Sub UpdateDTAProgress(ByVal DTAFileName As String)
		Static reDTAFile As System.Text.RegularExpressions.Regex

		Dim reMatch As System.Text.RegularExpressions.Match
		Dim intScanNumber As Integer

		If reDTAFile Is Nothing Then
			reDTAFile = New System.Text.RegularExpressions.Regex("(\d+)\.\d+\.\d\.dta$", System.Text.RegularExpressions.RegexOptions.Compiled Or System.Text.RegularExpressions.RegexOptions.IgnoreCase)
		End If

		Try
			' Extract out the scan number from the DTA filename
			reMatch = reDTAFile.Match(DTAFileName)
			If reMatch.Success Then
				If Integer.TryParse(reMatch.Groups.Item(1).Value, intScanNumber) Then
					m_Progress = CSng(intScanNumber / m_MaxScanInFile * 100)
				End If
			End If
		Catch ex As Exception
			' Ignore errors here
		End Try

	End Sub

	Private Sub mDTAWatcher_Created(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mDTAWatcher.Created
		UpdateDTAProgress(e.Name)
	End Sub


	Public Sub TestGetFileContents()

		Dim strFilePath As String = "TestInputFile.txt"
		Dim strContents As String

		strContents = GetFileContents(strFilePath)

		Console.WriteLine(strContents)

	End Sub

	Private Function GetFileContents(ByVal filePath As String) As String
		Dim fi As New System.IO.FileInfo(filePath)
		Dim tr As System.IO.StreamReader
		Dim s As String

		tr = New System.IO.StreamReader(New System.IO.FileStream(fi.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

		s = tr.ReadToEnd

		If s Is Nothing Then
			s = String.Empty
		End If

		Return s

	End Function

	Public Sub RemoveSparseSpectra()

		Dim oCDTAUtilities As clsCDTAUtilities = New clsCDTAUtilities

		oCDTAUtilities.RemoveSparseSpectra("e:\dms_workdir", "ALZ_VP2P101_C_SCX_02_7Dec08_Draco_08-10-29_dta.txt")

	End Sub

	Public Function ValidateSequestNodeCount(ByVal strLogFilePath As String, ByVal blnLogToConsole As Boolean) As Boolean
		Const ERROR_CODE_A As Integer = 2
		Const ERROR_CODE_B As Integer = 4
		Const ERROR_CODE_C As Integer = 8
		Const ERROR_CODE_D As Integer = 16
		Const ERROR_CODE_E As Integer = 32

		Dim reStartingTask As System.Text.RegularExpressions.Regex
		Dim reWaitingForReadyMsg As System.Text.RegularExpressions.Regex
		Dim reReceivedReadyMsg As System.Text.RegularExpressions.Regex
		Dim reSpawnedSlaveProcesses As System.Text.RegularExpressions.Regex
		Dim reSearchedDTAFile As System.Text.RegularExpressions.Regex
		Dim objMatch As System.Text.RegularExpressions.Match

		Dim strParam As String
		Dim strLineIn As String
		Dim strHostName As String

		' This dictionary tracks the number of DTAs processed by each node
		Dim dctHostCounts As Generic.Dictionary(Of String, Integer)

		' This dictionary tracks the number of distinct nodes on each host
		Dim dctHostNodeCount As Generic.Dictionary(Of String, Integer)

		Dim intValue As Integer

		' This dictionary tracks the number of DTAs processed per node on each host
		Dim dctHostProcessingRate As Generic.Dictionary(Of String, Single)

		' This array is used to compute a median
		Dim sngHostProcessingRateSorted() As Single

		Dim blnShowDetailedRates As Boolean

		Dim intHostCount As Integer
		Dim intNodeCountStarted As Integer
		Dim intNodeCountActive As Integer
		Dim intDTACount As Integer

		Dim intNodeCountExpected As Integer

		Dim strProcessingMsg As String

		Try

			blnShowDetailedRates = False

			If Not System.IO.File.Exists(strLogFilePath) Then
				strProcessingMsg = "Sequest.log file not found; cannot verify the sequest node count"
				If blnLogToConsole Then Console.WriteLine(strProcessingMsg & ": " & strLogFilePath)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
				Return False
			End If

			' Initialize the RegEx objects
			reStartingTask = New System.Text.RegularExpressions.Regex("Starting the SEQUEST task on (\d+) node", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
			reWaitingForReadyMsg = New System.Text.RegularExpressions.Regex("Waiting for ready messages from (\d+) node", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
			reReceivedReadyMsg = New System.Text.RegularExpressions.Regex("received ready messsage from (.+)\(", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
			reSpawnedSlaveProcesses = New System.Text.RegularExpressions.Regex("Spawned (\d+) slave processes", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
			reSearchedDTAFile = New System.Text.RegularExpressions.Regex("Searched dta file .+ on (.+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)

			intHostCount = 0			' Value for reStartingTask
			intNodeCountStarted = 0		' Value for reWaitingForReadyMsg
			intNodeCountActive = 0		' Value for reSpawnedSlaveProcesses
			intDTACount = 0

			' Note: This value is obtained when the manager params are grabbed from the Manager Control DB
			' Use this query to view/update expected node counts'
			'  SELECT M.M_Name, PV.MgrID, PV.Value
			'  FROM T_ParamValue AS PV INNER JOIN T_Mgrs AS M ON PV.MgrID = M.M_ID
			'  WHERE (PV.TypeID = 122)

			strParam = m_mgrParams.GetParam("SequestNodeCountExpected")
			If Integer.TryParse(strParam, intNodeCountExpected) Then
			Else
				intNodeCountExpected = 0
			End If

			' Initialize the dictionary that will track the number of spectra processed by each host
			dctHostCounts = New Generic.Dictionary(Of String, Integer)

			' Initialize the dictionary that will track the number of distinct nodes on each host
			dctHostNodeCount = New Generic.Dictionary(Of String, Integer)

			' Initialize the dictionary that will track processing rates
			dctHostProcessingRate = New Generic.Dictionary(Of String, Single)

			Using srLogFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strLogFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				' Read each line from the input file
				Do While srLogFile.Peek >= 0
					strLineIn = srLogFile.ReadLine

					If Not strLineIn Is Nothing AndAlso strLineIn.Length > 0 Then

						' See if the line matches one of the expected RegEx values
						objMatch = reStartingTask.Match(strLineIn)
						If Not objMatch Is Nothing AndAlso objMatch.Success Then
							If Not Integer.TryParse(objMatch.Groups(1).Value, intHostCount) Then
								strProcessingMsg = "Unable to parse out the Host Count from the 'Starting the SEQUEST task ...' entry in the Sequest.log file"
								If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
							End If

						Else
							objMatch = reWaitingForReadyMsg.Match(strLineIn)
							If Not objMatch Is Nothing AndAlso objMatch.Success Then
								If Not Integer.TryParse(objMatch.Groups(1).Value, intNodeCountStarted) Then
									strProcessingMsg = "Unable to parse out the Node Count from the 'Waiting for ready messages ...' entry in the Sequest.log file"
									If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
								End If

							Else
								objMatch = reReceivedReadyMsg.Match(strLineIn)
								If Not objMatch Is Nothing AndAlso objMatch.Success Then
									strHostName = objMatch.Groups(1).Value

									If dctHostNodeCount.TryGetValue(strHostName, intValue) Then
										dctHostNodeCount(strHostName) = intValue + 1
									Else
										dctHostNodeCount.Add(strHostName, 1)
									End If

								Else
									objMatch = reSpawnedSlaveProcesses.Match(strLineIn)
									If Not objMatch Is Nothing AndAlso objMatch.Success Then
										If Not Integer.TryParse(objMatch.Groups(1).Value, intNodeCountActive) Then
											strProcessingMsg = "Unable to parse out the Active Node Count from the 'Spawned xx slave processes ...' entry in the Sequest.log file"
											If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
											clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
										End If

									Else
										objMatch = reSearchedDTAFile.Match(strLineIn)
										If Not objMatch Is Nothing AndAlso objMatch.Success Then
											strHostName = objMatch.Groups(1).Value

											If Not strHostName Is Nothing Then
												If dctHostCounts.TryGetValue(strHostName, intValue) Then
													dctHostCounts(strHostName) = intValue + 1
												Else
													dctHostCounts.Add(strHostName, 1)
												End If

												intDTACount += 1
											End If
										Else
											' Ignore this line
										End If
									End If
								End If
							End If
						End If

					End If
				Loop

			End Using


			Try
				' Validate the stats

				strProcessingMsg = "HostCount=" & intHostCount & "; NodeCountActive=" & intNodeCountActive
				If m_DebugLevel >= 1 Then
					If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strProcessingMsg)
				End If
				m_EvalMessage = String.Copy(strProcessingMsg)

				If intNodeCountActive < intNodeCountExpected OrElse intNodeCountExpected = 0 Then
					strProcessingMsg = "Error: NodeCountActive less than expected value (" & intNodeCountActive & " vs. " & intNodeCountExpected & ")"
					If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)

					' Update the evaluation message and evaluation code
					' These will be used by sub CloseTask in clsAnalysisJob
					'
					' An evaluation code with bit ERROR_CODE_A set will result in DMS_Pipeline DB views
					'  V_Job_Steps_Stale_and_Failed and V_Sequest_Cluster_Warnings showing this message:
					'  "SEQUEST node count is less than the expected value"

					m_EvalMessage &= "; " & strProcessingMsg
					m_EvalCode = m_EvalCode Or ERROR_CODE_A
				Else
					If intNodeCountStarted <> intNodeCountActive Then
						strProcessingMsg = "Warning: NodeCountStarted (" & intNodeCountStarted & ") <> NodeCountActive"
						If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
						m_EvalMessage &= "; " & strProcessingMsg
						m_EvalCode = m_EvalCode Or ERROR_CODE_B

						' Update the evaluation message and evaluation code
						' These will be used by sub CloseTask in clsAnalysisJob
						' An evaluation code with bit ERROR_CODE_A set will result in view V_Sequest_Cluster_Warnings in the DMS_Pipeline DB showing this message:
						'  "SEQUEST node count is less than the expected value"

					End If
				End If

				If dctHostCounts.Count < intHostCount Then
					' Only record an error here if the number of DTAs processed was at least 2x the number of nodes
					If intDTACount >= 2 * intNodeCountActive Then
						strProcessingMsg = "Error: only " & dctHostCounts.Count & " host" & CheckForPlurality(dctHostCounts.Count) & " processed DTAs"
						If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)
						m_EvalMessage &= "; " & strProcessingMsg
						m_EvalCode = m_EvalCode Or ERROR_CODE_C
					End If
				End If

				' See if any of the hosts processed far fewer or far more spectra than the other hosts
				' When comparing hosts, we need to scale by the number of active nodes on each host
				' We'll populate intHostProcessingRate() with the number of DTAs processed per node on each host

				Const LOW_THRESHOLD_MULTIPLIER As Single = 0.33
				Const HIGH_THRESHOLD_MULTIPLIER As Single = 2

				Dim intNodeCountThisHost As Integer
				Dim intIndex As Integer

				Dim sngProcessingRate As Single
				Dim sngProcessingRateMedian As Single

				Dim intMidpoint As Integer
				Dim sngThresholdRate As Single
				Dim intWarningCount As Integer

				For Each objItem As Generic.KeyValuePair(Of String, Integer) In dctHostCounts
					intNodeCountThisHost = 0
					dctHostNodeCount.TryGetValue(objItem.Key, intNodeCountThisHost)
					If intNodeCountThisHost < 1 Then intNodeCountThisHost = 1

					sngProcessingRate = CSng(objItem.Value / intNodeCountThisHost)
					dctHostProcessingRate.Add(objItem.Key, sngProcessingRate)
				Next


				' Determine the median number of spectra processed

				ReDim sngHostProcessingRateSorted(dctHostProcessingRate.Count - 1)

				intIndex = 0
				For Each objItem As Generic.KeyValuePair(Of String, Single) In dctHostProcessingRate
					sngHostProcessingRateSorted(intindex) = objItem.Value
					intIndex += 1
				Next

				' Now sort sngHostProcessingRateSorted
				Array.Sort(sngHostProcessingRateSorted, 0, sngHostProcessingRateSorted.Length)

				If sngHostProcessingRateSorted.Length <= 2 Then
					intMidpoint = 0
				Else
					intMidpoint = CInt(Math.Floor(sngHostProcessingRateSorted.Length / 2))
				End If

				sngProcessingRateMedian = sngHostProcessingRateSorted(intMidpoint)

				' Count the number of hosts that had a processing rate fewer than LOW_THRESHOLD_MULTIPLIER times the the median value
				intWarningCount = 0
				sngThresholdRate = CSng(LOW_THRESHOLD_MULTIPLIER * sngProcessingRateMedian)

				For Each objItem As Generic.KeyValuePair(Of String, Single) In dctHostProcessingRate
					If objItem.Value < sngThresholdRate Then
						intWarningCount = +1
					End If
				Next

				If intWarningCount > 0 Then
					strProcessingMsg = "Warning: " & intWarningCount & " host" & CheckForPlurality(intWarningCount) & " processed fewer than " & sngThresholdRate.ToString("0.0") & " DTAs/node, which is " & LOW_THRESHOLD_MULTIPLIER & " times the median value of " & sngProcessingRateMedian.ToString("0.0")
					If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)

					m_EvalMessage &= "; " & strProcessingMsg
					m_EvalCode = m_EvalCode Or ERROR_CODE_D
					blnShowDetailedRates = True
				End If

				' Count the number of nodes that had a processing rate more than HIGH_THRESHOLD_MULTIPLIER times the median value 
				' When comparing hosts, have to scale by the number of active nodes on each host
				intWarningCount = 0
				sngThresholdRate = CSng(HIGH_THRESHOLD_MULTIPLIER * sngProcessingRateMedian)

				For Each objItem As Generic.KeyValuePair(Of String, Single) In dctHostProcessingRate
					If objItem.Value > sngThresholdRate Then
						intWarningCount = +1
					End If
				Next

				If intWarningCount > 0 Then
					strProcessingMsg = "Warning: " & intWarningCount & " host" & CheckForPlurality(intWarningCount) & " processed more than " & sngThresholdRate.ToString("0.0") & " DTAs/node, which is " & HIGH_THRESHOLD_MULTIPLIER & " times the median value of " & sngProcessingRateMedian.ToString("0.0")
					If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)

					m_EvalMessage &= "; " & strProcessingMsg
					m_EvalCode = m_EvalCode Or ERROR_CODE_E
					blnShowDetailedRates = True
				End If

				If m_DebugLevel >= 2 OrElse blnShowDetailedRates Then
					' Log the number of DTAs processed by each host

					For Each objItem As Generic.KeyValuePair(Of String, Integer) In dctHostCounts

						intNodeCountThisHost = 0
						dctHostNodeCount.TryGetValue(objItem.Key, intNodeCountThisHost)
						If intNodeCountThisHost < 1 Then intNodeCountThisHost = 1

						sngProcessingRate = 0
						dctHostProcessingRate.TryGetValue(objItem.Key, sngProcessingRate)

						strProcessingMsg = "Host " & objItem.Key & " processed " & objItem.Value & " DTA" & CheckForPlurality(objItem.Value) & _
						  " using " & intNodeCountThisHost & " node" & CheckForPlurality(intNodeCountThisHost) & _
						  " (" & sngProcessingRate.ToString("0.0") & " DTAs/node)"

						If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strProcessingMsg)
					Next

				End If

			Catch ex As Exception
				' Error occurred

				strProcessingMsg = "Error in validating the stats in ValidateSequestNodeCount" & ex.Message
				If blnLogToConsole Then
					Console.WriteLine("====================================================================")
					Console.WriteLine(strProcessingMsg)
					Console.WriteLine("====================================================================")
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)
				Return False
			End Try

		Catch ex As Exception
			' Error occurred

			strProcessingMsg = "Error parsing Sequest.log file in ValidateSequestNodeCount" & ex.Message
			If blnLogToConsole Then
				Console.WriteLine("====================================================================")
				Console.WriteLine(strProcessingMsg)
				Console.WriteLine("====================================================================")
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)
			Return False
		End Try

		Return True

	End Function

	Private Function CheckForPlurality(ByVal intValue As Integer) As String
		If intValue = 1 Then
			Return ""
		Else
			Return "s"
		End If
	End Function

	Protected Class clsResourceTestClass
		Inherits clsAnalysisResources

		Public Overrides Function GetResources() As IJobParams.CloseOutType

		End Function
	End Class

End Class
