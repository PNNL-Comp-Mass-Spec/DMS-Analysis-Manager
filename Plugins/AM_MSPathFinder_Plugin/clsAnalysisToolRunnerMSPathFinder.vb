'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/10/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Runtime.InteropServices

Public Class clsAnalysisToolRunnerMSPathFinder
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running MSPathFinder analysis of top down data
	'*********************************************************************************************************

#Region "Constants and Enums"
	Protected Const MSPATHFINDER_CONSOLE_OUTPUT As String = "MSPathFinder_ConsoleOutput.txt"

	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_SEARCHING_TARGET_DB As Single = 5
	Protected Const PROGRESS_PCT_SEARCHING_DECOY_DB As Single = 50
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	'Protected Const MSPathFinder_RESULTS_FILE_SUFFIX As String = "_MSPathFinder.txt"
	'Protected Const MSPathFinder_FILTERED_RESULTS_FILE_SUFFIX As String = "_MSPathFinder.id.txt"
#End Region

#Region "Module Variables"

	Protected mConsoleOutputErrorMsg As String

	Protected mMSPathFinderResultsFilePath As String

	Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
	''' <summary>
	''' Runs MSPathFinder
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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSPathFinder.RunTool(): Enter")
			End If

			' Determine the path to the MSPathFinder program (Top-down version)
			Dim progLoc As String
			progLoc = DetermineProgramLocation("MSPathFinder", "MSPathFinderProgLoc", "MSPathFinderT.exe")

			If String.IsNullOrWhiteSpace(progLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Store the MSPathFinder version info in the database
			If Not StoreToolVersionInfo(progLoc) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				m_message = "Error determining MSPathFinder version"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim fastaFileIsDecoy As Boolean
			If Not InitializeFastaFile(fastaFileIsDecoy) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Run MSPathFinder
			Dim tdaEnabled As Boolean
			Dim blnSuccess = StartMSPathFinder(progLoc, fastaFileIsDecoy, tdaEnabled)

			If blnSuccess Then
				' Look for the results file

				Dim fiResultsFile As FileInfo

				If tdaEnabled Then
					fiResultsFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_IcTda.tsv"))
				Else
					fiResultsFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_IcTarget.tsv"))
				End If

				If fiResultsFile.Exists Then
					blnSuccess = PostProcessMSPathFinderResults()
					If Not blnSuccess Then
						If String.IsNullOrEmpty(m_message) Then
							m_message = "Unknown error post-processing the MSPathFinder results"
						End If
					End If

				Else
					If String.IsNullOrEmpty(m_message) Then
						m_message = "MSPathFinder results file not found: " & fiResultsFile.Name
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

			CmdRunner = Nothing

			'Make sure objects are released
			Threading.Thread.Sleep(2000)		'2 second delay
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
			m_message = "Error in MSPathFinderPlugin->RunTool"
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

	Protected Function GetMSPathFinderParameterNames() As Dictionary(Of String, String)
		Dim dctParamNames As Dictionary(Of String, String)
		dctParamNames = New Dictionary(Of String, String)(25, StringComparer.CurrentCultureIgnoreCase)

		dctParamNames.Add("PMTolerance", "t")
		dctParamNames.Add("FragTolerance", "f")
		dctParamNames.Add("SearchMode", "m")
		dctParamNames.Add("TDA", "tda")
		dctParamNames.Add("minLength", "minLength")
		dctParamNames.Add("maxLength", "maxLength")

		dctParamNames.Add("minCharge", "minCharge")
		dctParamNames.Add("maxCharge", "maxCharge")

		dctParamNames.Add("minFragCharge", "minFragCharge")
		dctParamNames.Add("maxFragCharge", "maxFragCharge")

		dctParamNames.Add("minMass", "minMass")
		dctParamNames.Add("maxMass", "maxMass")

		' The following are special cases; 
		' do not add to dctParamNames
		'   NumMods
		'   StaticMod
		'   DynamicMod

		Return dctParamNames

	End Function

	Private Function InitializeFastaFile(<Out()> ByRef fastaFileIsDecoy As Boolean) As Boolean

		fastaFileIsDecoy = False

		' Define the path to the fasta file
		Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")
		Dim FastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

		Dim fiFastaFile As FileInfo
		fiFastaFile = New FileInfo(FastaFilePath)

		If Not fiFastaFile.Exists Then
			' Fasta file not found
			LogError("Fasta file not found: " & fiFastaFile.Name, "Fasta file not found: " & fiFastaFile.FullName)
			Return False
		End If

		Dim strProteinOptions As String
		strProteinOptions = m_jobParams.GetParam("ProteinOptions")
		If Not String.IsNullOrEmpty(strProteinOptions) Then
			If strProteinOptions.ToLower.Contains("seq_direction=decoy") Then
				fastaFileIsDecoy = True
			End If
		End If

		Return True

	End Function

	''' <summary>
	''' Parse the MSPathFinder console output file to track the search progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output
		'
		' MSPathFinderT 0.12 (June 17, 2014)
		' SpecFilePath: E:\DMS_WorkDir\Synocho_L2_1.pbf
		' DatabaseFilePath: C:\DMS_Temp_Org\ID_003962_71E1A1D4.fasta
		' OutputDir: E:\DMS_WorkDir
		' SearchMode: 1
		' Tda: True
		' PrecursorIonTolerancePpm: 10
		' ProductIonTolerancePpm: 10
		' MinSequenceLength: 21
		' MaxSequenceLength: 300
		' MinPrecursorIonCharge: 2
		' MaxPrecursorIonCharge: 30
		' MinProductIonCharge: 1
		' MaxProductIonCharge: 15
		' MinSequenceMass: 3000
		' MaxSequenceMass: 50000
		' MaxDynamicModificationsPerSequence: 4
		' Modifications:
		' C(2) H(3) N(1) O(1) S(0),C,fix,Everywhere,Carbamidomethyl
		' C(0) H(0) N(0) O(1) S(0),M,opt,Everywhere,Oxidation
		' C(0) H(1) N(0) O(3) S(0) P(1),S,opt,Everywhere,Phospho
		' C(0) H(1) N(0) O(3) S(0) P(1),T,opt,Everywhere,Phospho
		' C(0) H(1) N(0) O(3) S(0) P(1),Y,opt,Everywhere,Phospho
		' C(0) H(-1) N(0) O(0) S(0),C,opt,Everywhere,Dehydro
		' C(2) H(2) N(0) O(1) S(0),*,opt,ProteinNTerm,Acetyl
		' Reading raw file...Elapsed Time: 4.4701 sec
		' Determining precursor masses...Elapsed Time: 59.2987 sec
		' Deconvoluting MS2 spectra...Elapsed Time: 9.5820 sec
		' Generating C:\DMS_Temp_Org\ID_003962_71E1A1D4.icseq and C:\DMS_Temp_Org\ID_003962_71E1A1D4.icanno...    Done.
		' Reading the target database...Elapsed Time: 0.0074 sec
		' Searching the target database
		' Generating C:\DMS_Temp_Org\ID_003962_71E1A1D4.icplcp... Done.

		Const REGEX_MSPathFinder_PROGRESS As String = "(\d+)% complete"
		Static reCheckProgress As New Regex(REGEX_MSPathFinder_PROGRESS, RegexOptions.Compiled Or RegexOptions.IgnoreCase)
		Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow

		Static reProcessingProteins As New Regex("Processing (\d+)th proteins", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

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
			Dim targetProteinsSearched As Integer = 0
			Dim decoyProteinsSearched As Integer = 0

			Dim searchingDecoyDB = False

			Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				Do While srInFile.Peek() >= 0
					Dim strLineIn = srInFile.ReadLine()

					If Not String.IsNullOrWhiteSpace(strLineIn) Then

						Dim strLineInLCase = strLineIn.ToLower()

						If strLineInLCase.StartsWith("error:") OrElse strLineInLCase.Contains("unhandled exception") Then
							If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
								mConsoleOutputErrorMsg = "Error running MSPathFinder:"
							End If
							mConsoleOutputErrorMsg &= "; " & strLineIn
							Continue Do

						ElseIf strLineIn.StartsWith("Searching the target database") Then
							progressComplete = PROGRESS_PCT_SEARCHING_TARGET_DB

						ElseIf strLineIn.StartsWith("Searching the decoy database") Then
							progressComplete = PROGRESS_PCT_SEARCHING_DECOY_DB
							searchingDecoyDB = True

						Else
							Dim oMatch As Match = reCheckProgress.Match(strLineIn)
							If oMatch.Success Then
								Single.TryParse(oMatch.Groups(1).ToString(), progressComplete)
								Continue Do
							End If

							oMatch = reProcessingProteins.Match(strLineIn)
							If oMatch.Success Then
								Dim proteinsSearched As Integer
								If Integer.TryParse(oMatch.Groups(1).ToString(), proteinsSearched) Then
									If searchingDecoyDB Then
										decoyProteinsSearched = Math.Max(decoyProteinsSearched, proteinsSearched)
									Else
										targetProteinsSearched = Math.Max(targetProteinsSearched, proteinsSearched)
									End If
								End If

								Continue Do
							End If

						End If

					End If
				Loop

			End Using

			If searchingDecoyDB Then
				progressComplete = ComputeIncrementalProgress(PROGRESS_PCT_SEARCHING_DECOY_DB, PROGRESS_PCT_COMPLETE, decoyProteinsSearched, targetProteinsSearched)
			End If

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
	''' Parses the static and dynamic modification information to create the MSPathFinder Mods file
	''' </summary>
	''' <param name="strParameterFilePath">Full path to the MSPathFinder parameter file; will create file MSPathFinder_Mods.txt in the same folder</param>
	''' <param name="sbOptions">String builder of command line arguments to pass to MSPathFinder</param>
	''' <param name="intNumMods">Max Number of Modifications per peptide</param>
	''' <param name="lstStaticMods">List of Static Mods</param>
	''' <param name="lstDynamicMods">List of Dynamic Mods</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Protected Function ParseMSPathFinderModifications(ByVal strParameterFilePath As String, _
	  ByRef sbOptions As Text.StringBuilder, _
	  ByVal intNumMods As Integer, _
	  ByRef lstStaticMods As List(Of String), _
	  ByRef lstDynamicMods As List(Of String)) As Boolean

		Const MOD_FILE_NAME As String = "MSPathFinder_Mods.txt"
		Dim blnSuccess As Boolean
		Dim strModFilePath As String
		Dim errMsg As String

		Try
			Dim fiParameterFile = New FileInfo(strParameterFilePath)

			strModFilePath = Path.Combine(fiParameterFile.DirectoryName, MOD_FILE_NAME)

			sbOptions.Append(" -mod " & MOD_FILE_NAME)

			Using swModFile As StreamWriter = New StreamWriter(New FileStream(strModFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

				swModFile.WriteLine("# This file is used to specify modifications for MSPathFinder")
				swModFile.WriteLine("")
				swModFile.WriteLine("# Max Number of Modifications per peptide")
				swModFile.WriteLine("NumMods=" & intNumMods)

				swModFile.WriteLine("")
				swModFile.WriteLine("# Static mods")
				If lstStaticMods.Count = 0 Then
					swModFile.WriteLine("# None")
				Else
					For Each strStaticMod As String In lstStaticMods
						Dim strModClean As String = String.Empty

						If ParseMSPathFinderValidateMod(strStaticMod, strModClean) Then
							If strModClean.Contains(",opt,") Then
								' Static (fixed) mod is listed as dynamic
								' Abort the analysis since the parameter file is misleading and needs to be fixed							
								errMsg = "Static mod definition contains ',opt,'; update the param file to have ',fix,' or change to 'DynamicMod='"
								LogError(errMsg, errMsg & "; " & strStaticMod)
								Return False
							End If
							swModFile.WriteLine(strModClean)
						Else
							Return False
						End If
					Next
				End If

				swModFile.WriteLine("")
				swModFile.WriteLine("# Dynamic mods")
				If lstDynamicMods.Count = 0 Then
					swModFile.WriteLine("# None")
				Else
					For Each strDynamicMod As String In lstDynamicMods
						Dim strModClean As String = String.Empty

						If ParseMSPathFinderValidateMod(strDynamicMod, strModClean) Then
							If strModClean.Contains(",fix,") Then
								' Dynamic (optional) mod is listed as static
								' Abort the analysis since the parameter file is misleading and needs to be fixed							
								errMsg = "Dynamic mod definition contains ',fix,'; update the param file to have ',opt,' or change to 'StaticMod='"
								LogError(errMsg, errMsg & "; " & strDynamicMod)
								Return False
							End If
							swModFile.WriteLine(strModClean)
						Else
							Return False
						End If
					Next
				End If

			End Using

			blnSuccess = True

		Catch ex As Exception
			errMsg = "Exception creating MSPathFinder Mods file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errMsg, ex)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Read the MSPathFinder options file and convert the options to command line switches
	''' </summary>
	''' <param name="fastaFileIsDecoy">True if the fasta file has had forward and reverse index files created</param>
	''' <param name="strCmdLineOptions">Output: MSGFDb command line arguments</param>
	''' <returns>Options string if success; empty string if an error</returns>
	''' <remarks></remarks>
	Public Function ParseMSPathFinderParameterFile(ByVal fastaFileIsDecoy As Boolean, <Out()> ByRef strCmdLineOptions As String, <Out()> tdaEnabled As Boolean) As IJobParams.CloseOutType

		Dim intNumMods As Integer = 0
		Dim lstStaticMods As List(Of String) = New List(Of String)
		Dim lstDynamicMods As List(Of String) = New List(Of String)

		Dim errMsg As String

		strCmdLineOptions = String.Empty
		tdaEnabled = False

		Dim strParameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))

		If Not File.Exists(strParameterFilePath) Then
			LogError("Parameter file not found", "Parameter file not found: " & strParameterFilePath)
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		Dim sbOptions = New Text.StringBuilder(500)

		Try

			' Initialize the Param Name dictionary
			Dim dctParamNames = GetMSPathFinderParameterNames()

			Using srParamFile As StreamReader = New StreamReader(New FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

				Do While srParamFile.Peek > -1
					Dim strLineIn = srParamFile.ReadLine()

					Dim kvSetting = clsGlobal.GetKeyValueSetting(strLineIn)

					If Not String.IsNullOrWhiteSpace(kvSetting.Key) Then

						Dim strValue As String = kvSetting.Value
						Dim intValue As Integer

						Dim strArgumentSwitch As String = String.Empty

						' Check whether kvSetting.key is one of the standard keys defined in dctParamNames
						If dctParamNames.TryGetValue(kvSetting.Key, strArgumentSwitch) Then

							sbOptions.Append(" -" & strArgumentSwitch & " " & strValue)

						ElseIf clsGlobal.IsMatch(kvSetting.Key, "NumMods") Then
							If Integer.TryParse(strValue, intValue) Then
								intNumMods = intValue
							Else
								errMsg = "Invalid value for NumMods in MSGFDB parameter file"
								LogError(errMsg, errMsg & ": " & strLineIn)
								srParamFile.Close()
								Return IJobParams.CloseOutType.CLOSEOUT_FAILED
							End If

						ElseIf clsGlobal.IsMatch(kvSetting.Key, "StaticMod") Then
							If Not String.IsNullOrWhiteSpace(strValue) AndAlso Not clsGlobal.IsMatch(strValue, "none") Then
								lstStaticMods.Add(strValue)
							End If

						ElseIf clsGlobal.IsMatch(kvSetting.Key, "DynamicMod") Then
							If Not String.IsNullOrWhiteSpace(strValue) AndAlso Not clsGlobal.IsMatch(strValue, "none") Then
								lstDynamicMods.Add(strValue)
							End If
						End If

					End If
				Loop

			End Using

		Catch ex As Exception
			m_message = "Exception reading MSPathFinder parameter file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)			
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' Create the modification file and append the -mod switch
		If Not ParseMSPathFinderModifications(strParameterFilePath, sbOptions, intNumMods, lstStaticMods, lstDynamicMods) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		strCmdLineOptions = sbOptions.ToString()

		If strCmdLineOptions.Contains("-tda 1") Then
			tdaEnabled = True
			' Make sure the .Fasta file is not a Decoy fasta
			If fastaFileIsDecoy Then
				LogError("Parameter file / decoy protein collection conflict: do not use a decoy protein collection when using a target/decoy parameter file (which has setting TDA=1)")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Validates that the modification definition text
	''' </summary>
	''' <param name="strMod">Modification definition</param>
	''' <param name="strModClean">Cleaned-up modification definition (output param)</param>
	''' <returns>True if valid; false if invalid</returns>
	''' <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
	Protected Function ParseMSPathFinderValidateMod(ByVal strMod As String, <Out()> ByRef strModClean As String) As Boolean

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

	Private Function PostProcessMSPathFinderResults() As Boolean

		' Move the output files into a subfolder so that we can zip them
		Dim compressDirPath As String = String.Empty

		Try
			Dim diWorkDir = New DirectoryInfo(m_WorkDir)

			' Make sure MSPathFinder has released the file handles
			PRISM.Processes.clsProgRunner.GarbageCollectNow()
			Threading.Thread.Sleep(500)

			Dim diCompressDir = New DirectoryInfo(Path.Combine(m_WorkDir, "TempCompress"))
			If diCompressDir.Exists Then
				For Each fiFile In diCompressDir.GetFiles()
					fiFile.Delete()
				Next
			Else
				diCompressDir.Create()
			End If

			Dim fiResultFiles = diWorkDir.GetFiles(m_Dataset & "*_Ic*.tsv").ToList()

			If fiResultFiles.Count = 0 Then
				m_message = "Did not find any _Ic*.tsv files"
				Return False
			End If

			For Each fiFile In fiResultFiles
				Dim targetFilePath = Path.Combine(diCompressDir.FullName, fiFile.Name)
				fiFile.MoveTo(targetFilePath)
			Next

			compressDirPath = diCompressDir.FullName

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception preparing the MSPathFinder results for zipping: " & ex.Message)
			Return False
		End Try

		Try

			m_IonicZipTools.DebugLevel = m_DebugLevel

			Dim resultsZipFilePath As String = Path.Combine(m_WorkDir, m_Dataset & "_IcTsv.zip")
			Dim blnSuccess = m_IonicZipTools.ZipDirectory(compressDirPath, resultsZipFilePath)

			If Not blnSuccess Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = m_IonicZipTools.Message
					If String.IsNullOrEmpty(m_message) Then
						m_message = "Unknown error zipping the MSPathFinder results"
					End If
				End If
			End If

			Return blnSuccess

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception zipping the MSPathFinder results: " & ex.Message)
			Return False
		End Try

	End Function

	Protected Function StartMSPathFinder(ByVal progLoc As String, ByVal fastaFileIsDecoy As Boolean, <Out()> ByRef tdaEnabled As Boolean) As Boolean

		Dim CmdStr As String
		Dim blnSuccess As Boolean

		mConsoleOutputErrorMsg = String.Empty

		' Read the MSPathFinder Parameter File
		' The parameter file name specifies the mass modifications to consider, plus also the analysis parameters

		Dim strCmdLineOptions As String = String.Empty

		Dim eResult = ParseMSPathFinderParameterFile(fastaFileIsDecoy, strCmdLineOptions, tdaEnabled)

		If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return False
		ElseIf String.IsNullOrEmpty(strCmdLineOptions) Then
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Problem parsing MSPathFinder parameter file"
			End If
			Return False
		End If

		Dim pbfFilePath As String = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_PBF_EXTENSION)

		' Define the path to the fasta file
		Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")
		Dim fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSPathFinder")

		'Set up and execute a program runner to run MSPathFinder
		CmdStr = " -s " & pbfFilePath
		CmdStr &= " -d " & fastaFilePath
		CmdStr &= " -o " & m_WorkDir
		CmdStr &= " " & strCmdLineOptions

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & CmdStr)
		End If

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = False
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MSPATHFINDER_CONSOLE_OUTPUT)
		End With

		m_progress = PROGRESS_PCT_STARTING

		blnSuccess = CmdRunner.RunProgram(progLoc, CmdStr, "MSPathFinder", True)

		If Not CmdRunner.WriteConsoleOutputToFile Then
			' Write the console output to a text file
			System.Threading.Thread.Sleep(250)

			Dim swConsoleOutputfile = New StreamWriter(New FileStream(CmdRunner.ConsoleOutputFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))
			swConsoleOutputfile.WriteLine(CmdRunner.CachedConsoleOutput)
			swConsoleOutputfile.Close()
		End If

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		' Parse the console output file one more time to check for errors
		System.Threading.Thread.Sleep(250)
		ParseConsoleOutputFile(CmdRunner.ConsoleOutputFilePath)

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		If Not blnSuccess Then
			Dim Msg As String
			Msg = "Error running MSPathFinder"
			m_message = clsGlobal.AppendToComment(m_message, Msg)

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

			If CmdRunner.ExitCode <> 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSPathFinder returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MSPathFinder failed (but exit code is 0)")
			End If

			Return False

		End If

		m_progress = PROGRESS_PCT_COMPLETE
		m_StatusTools.UpdateAndWrite(m_progress)
		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSPathFinder Search Complete")
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
		ioToolFiles.Add(New FileInfo(Path.Combine(fiProgram.Directory.FullName, "InformedProteomics.TopDown.dll")))

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
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
		Static dtLastStatusUpdate As DateTime = DateTime.UtcNow
		Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 5 seconds)
		If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = DateTime.UtcNow
			UpdateStatusRunning(m_progress)
		End If

		' Parse the console output file every 15 seconds
		If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = DateTime.UtcNow

			ParseConsoleOutputFile(Path.Combine(m_WorkDir, MSPATHFINDER_CONSOLE_OUTPUT))

		End If

	End Sub

#End Region

End Class
