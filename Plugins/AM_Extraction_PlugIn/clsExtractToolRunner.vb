'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 10/10/2008
'
' Last modified 10/24/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO
Imports System.Runtime.InteropServices
Imports PHRPReader

Public Class clsExtractToolRunner
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Primary class for controlling data extraction
	'*********************************************************************************************************

#Region "Constants"
	Protected Const SEQUEST_PROGRESS_EXTRACTION_DONE As Single = 33
	Protected Const SEQUEST_PROGRESS_PHRP_DONE As Single = 66
	Protected Const SEQUEST_PROGRESS_PEPPROPHET_DONE As Single = 100

	Public Const INSPECT_UNFILTERED_RESULTS_FILE_SUFFIX As String = "_inspect_unfiltered.txt"

	Protected Const MODa_JAR_NAME As String = "moda.jar"
	Protected Const MODa_FILTER_JAR_NAME As String = "anal_moda.jar"

#End Region

#Region "Module variables"
	Protected WithEvents m_PeptideProphet As clsPeptideProphetWrapper
	Protected WithEvents m_PHRP As clsPepHitResultsProcWrapper
	Protected WithEvents mMSGFDBUtils As AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils
	Protected mGeneratedFastaFilePath As String
#End Region

#Region "Properties"
#End Region

#Region "Methods"
	''' <summary>
	''' Runs the data extraction tool(s)
	''' </summary>
	''' <returns>IJobParams.CloseOutType representing success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim Msg As String
		Dim Result As IJobParams.CloseOutType
		Dim eReturnCode As IJobParams.CloseOutType

		Dim OrgDbDir As String
		Dim FastaFileName As String

		Dim strCurrentAction As String = "preparing for extraction"
		Dim blnProcessingError As Boolean

		Try

			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDeconPeakDetector.RunTool(): Enter")
			End If
			
			' Store the AnalysisManager version info in the database
			If Not StoreToolVersionInfo() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				m_message = "Error determining version of Data Extraction tools"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			OrgDbDir = m_mgrParams.GetParam("orgdbdir")

			' Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
			FastaFileName = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
			If String.IsNullOrEmpty(FastaFileName) Then
				mGeneratedFastaFilePath = String.Empty
			Else
				mGeneratedFastaFilePath = Path.Combine(OrgDbDir, FastaFileName)
			End If

			Select Case m_jobParams.GetParam("ResultType")
				Case clsAnalysisResources.RESULT_TYPE_SEQUEST	'Sequest result type

					' Run Ken's Peptide Extractor DLL
					strCurrentAction = "running peptide extraction for Sequest"
					Result = PerformPeptideExtraction()
					' Check for no data first. If no data, then exit but still copy results to server
					If Result = IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
						Exit Select
					End If

					' Run PHRP
					If Result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
						m_progress = SEQUEST_PROGRESS_EXTRACTION_DONE	  ' 33% done
						m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)

						strCurrentAction = "running peptide hits result processor for Sequest"
						Result = RunPhrpForSequest()
					End If

					If Result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
						m_progress = SEQUEST_PROGRESS_PHRP_DONE	  ' 66% done
						m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)
						strCurrentAction = "running peptide prophet for Sequest"
						RunPeptideProphet()
					End If

				Case clsAnalysisResources.RESULT_TYPE_XTANDEM
					' Run PHRP
					strCurrentAction = "running peptide hits result processor for X!Tandem"
					Result = RunPhrpForXTandem()

				Case clsAnalysisResources.RESULT_TYPE_INSPECT
					' Run PHRP
					strCurrentAction = "running peptide hits result processor for Inspect"
					Result = RunPhrpForInSpecT()

				Case clsAnalysisResources.RESULT_TYPE_MSGFDB
					' Run PHRP
					strCurrentAction = "running peptide hits result processor for MSGF+"
					Result = RunPhrpForMSGFDB()

				Case clsAnalysisResources.RESULT_TYPE_MSALIGN
					' Run PHRP
					strCurrentAction = "running peptide hits result processor for MSAlign"
					Result = RunPhrpForMSAlign()

				Case clsAnalysisResources.RESULT_TYPE_MODA

					' Convert the MODa results to a tab-delimited file, filtering by FDR when converting
					Dim strFilteredMODaResultsFilePath As String = String.Empty
					Result = ConvertMODaResultsToTxt(strFilteredMODaResultsFilePath)
					If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
						blnProcessingError = True
						Exit Select
					End If

					' Run PHRP
					strCurrentAction = "running peptide hits result processor for MODa"
					Result = RunPhrpForMODa(strFilteredMODaResultsFilePath)

				Case Else
					' Should never get here - invalid result type specified
					Msg = "Invalid ResultType specified: " & m_jobParams.GetParam("ResultType")
					m_message = clsGlobal.AppendToComment(m_message, Msg)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " & Msg)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Select

			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS And Result <> IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
				Msg = "Error " & strCurrentAction
				m_message = clsGlobal.AppendToComment(m_message, Msg)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " & Msg)
				blnProcessingError = True
			Else
				m_progress = 100	' 100% done
				m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)
			End If

			' Stop the job timer
			m_StopTime = DateTime.UtcNow

			If blnProcessingError Then
				' Something went wrong
				' In order to help diagnose things, we will move whatever files were created into the Result folder, 
				'  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
				eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			Result = MakeResultsFolder()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' MakeResultsFolder handles posting to local log, so set database error message and exit
				m_message = "Error making results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Result = MoveResultFiles()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' MoveResultFiles moves the Result files to the Result folder
				m_message = "Error moving files into results folder"
				eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If blnProcessingError Or eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
				' Try to save whatever files were moved into the results folder
				Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
				objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName))

				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Result = CopyResultsFolderToServer()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
				Return Result
			End If

			' Everything succeeded; now delete the _msgfdb.tsv file from the server
			RemoveNonResultServerFiles()

		Catch ex As Exception
			Msg = "clsExtractToolRunner.RunTool(); Exception running extraction tool: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception running extraction tool")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' If we got to here, everything worked so exit happily
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Convert the MODa output file to a tab-delimited text file
	''' </summary>
	''' <param name="strFilteredMODaResultsFilePath">Output parameter: path to the filtered results file</param>
	''' <returns>The path to the .txt file if successful; empty string if an error</returns>
	''' <remarks></remarks>
	Protected Function ConvertMODaResultsToTxt(<Out()> ByRef strFilteredMODaResultsFilePath As String) As IJobParams.CloseOutType

		strFilteredMODaResultsFilePath = String.Empty

		Try
			Dim fdrThreshold = m_jobParams.GetJobParameter("MODaFDRThreshold", 0.05)
			Dim decoyPrefix = m_jobParams.GetJobParameter("MODaDecoyPrefix", "XXX_")

			Dim paramFileName = m_jobParams.GetParam("ParmFileName")
			Dim paramFilePath = Path.Combine(m_WorkDir, paramFileName)

			Dim MODaResultsFilePath = Path.Combine(m_WorkDir, m_Dataset & "_moda.txt")

			If Math.Abs(fdrThreshold) < Single.Epsilon Then
				fdrThreshold = 0.05
			ElseIf fdrThreshold > 1 Then
				fdrThreshold = 1
			End If

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Filtering MODa Results with FDR threshold " & fdrThreshold.ToString("0.00"))
			End If

			Const intJavaMemorySize = 1000

			' JavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
			Dim JavaProgLoc = GetJavaProgLoc()
			If String.IsNullOrEmpty(JavaProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Determine the path to the MODa program
			Dim strMODaProgLoc = DetermineProgramLocation("MODa", "MODaProgLoc", MODa_JAR_NAME)

			Dim fiModA = New FileInfo(strMODaProgLoc)

			'Set up and execute a program runner to run anal_moda.jar
			Dim CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & Path.Combine(fiModA.Directory.FullName, MODa_FILTER_JAR_NAME)
			CmdStr &= " -i " & MODaResultsFilePath
			CmdStr &= " -p " & paramFilePath
			CmdStr &= " -fdr " & fdrThreshold
			CmdStr &= " -d " & decoyPrefix

			' Example command line:
			' "C:\Program Files\Java\jre7\bin\java.exe" -Xmx1500M -jar C:\DMS_Programs\MODa\anal_moda.jar -i "E:\DMS_WorkDir3\QC_Shew_13_04_pt1_1_2_45min_14Nov13_Leopard_13-05-21_moda.txt" -p "E:\DMS_WorkDir3\MODa_PartTryp_Par20ppm_Frag0pt6Da" -fdr 0.05 -d XXX_
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)

			Dim progRunner = New clsRunDosProgram(m_WorkDir)

			With progRunner
				.CreateNoWindow = True
				.CacheStandardOutput = False
				.EchoOutputToConsole = True

				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = Path.Combine(m_WorkDir, "MODa_Filter_ConsoleOutput.txt")
			End With

			Dim blnSuccess = progRunner.RunProgram(JavaProgLoc, CmdStr, "MODa_Filter", True)

			If Not blnSuccess Then
				Dim Msg As String
				Msg = "Error parsing and filtering MODa results"
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If progRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, MODa_FILTER_JAR_NAME & " returned a non-zero exit code: " & progRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to " & MODa_FILTER_JAR_NAME & " failed (but exit code is 0)")
				End If

				Return IJobParams.CloseOutType.CLOSEOUT_FAILED

			End If

			' Confirm that the reuslts file was created
			Dim fiFilteredMODaResultsFilePath = New FileInfo(Path.ChangeExtension(MODaResultsFilePath, ".id.txt"))

			If Not fiFilteredMODaResultsFilePath.Exists() Then
				m_message = "Filtered MODa results file not found: " & fiFilteredMODaResultsFilePath.Name
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If

			strFilteredMODaResultsFilePath = fiFilteredMODaResultsFilePath.FullName
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		Catch ex As Exception
			m_message = "Error in MODaPlugin->PostProcessResults"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	''' <summary>
	''' Convert the .mzid file created by MSGF+ to a .tsv file
	''' </summary>
	''' <param name="suffixToAdd">Suffix to add when parsing files created by Parallel MSGF+</param>
	''' <returns>The path to the .tsv file if successful; empty string if an error</returns>
	''' <remarks></remarks>
	Protected Function ConvertMZIDToTSV(ByVal suffixToAdd As String) As String

		Try

			Dim strMZIDFileName = m_Dataset & "_msgfplus" & suffixToAdd & ".mzid"
			If Not File.Exists(Path.Combine(m_WorkDir, strMZIDFileName)) Then
				m_message = strMZIDFileName & " file not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return String.Empty
			End If

			' JavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
			Dim JavaProgLoc = GetJavaProgLoc()
			If String.IsNullOrEmpty(JavaProgLoc) Then
				Return String.Empty
			End If

			' Determine the path to MSGF+
			' It is important that you pass "MSGFDB" to this function, even if mMSGFPlus = True
			' The reason?  The AM_MSGFDB_PlugIn uses "MSGFDB" when creating the ToolVersionInfo file
			' We need to keep the name the same since the PeptideHitResultsProcessor (and possibly other software) expects the file to be named Tool_Version_Info_MSGFDB.txt
			Dim MSGFDbProgLoc = DetermineProgramLocation("MSGFDB", "MSGFDbProgLoc", AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils.MSGFPLUS_JAR_NAME)

			If String.IsNullOrEmpty(MSGFDbProgLoc) Then
				If String.IsNullOrEmpty(m_message) Then m_message = "Parameter 'MSGFDbProgLoc' not defined for this manager"
				Return String.Empty
			End If

			' Initialize mMSGFDBUtils
			mMSGFDBUtils = New AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils(m_mgrParams, m_jobParams, m_JobNum, m_WorkDir, m_DebugLevel, blnMSGFPlus:=True)

			Dim strTSVFilePath = mMSGFDBUtils.ConvertMZIDToTSV(JavaProgLoc, MSGFDbProgLoc, m_Dataset, strMZIDFileName)

			If Not String.IsNullOrEmpty(strTSVFilePath) Then
				' File successfully created

				If Not String.IsNullOrEmpty(suffixToAdd) Then
					Dim fiTSVFile = New FileInfo(strTSVFilePath)
					Dim newTSVPath = Path.Combine(fiTSVFile.Directory.FullName, Path.GetFileNameWithoutExtension(strTSVFilePath) & suffixToAdd & ".tsv")
					fiTSVFile.MoveTo(newTSVPath)
				End If

				Return strTSVFilePath
			End If

			If String.IsNullOrEmpty(m_message) Then
				m_message = "Error calling mMSGFDBUtils.ConvertMZIDToTSV; path not returned"
			End If

		Catch ex As Exception
			m_message = "Exception in ConvertMZIDToTSV"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
		End Try

		Return String.Empty

	End Function

	Private Function ParallelMSGFPlusMergeTSVFiles(
	 ByVal numberOfClonedSteps As Integer,
	 ByVal numberOfHitsPerScanToKeep As Integer,
	 <Out()> ByRef lstFilterPassingPeptides As SortedSet(Of String)) As IJobParams.CloseOutType

		lstFilterPassingPeptides = New SortedSet(Of String)
		Try

			Dim mergedFilePath = Path.Combine(m_WorkDir, m_Dataset & "_msgfdb.tsv")

			' Keys in this dictionary are column names, values are the 0-based column index
			Dim dctHeaderMapping = New Dictionary(Of String, Integer)

			' This dictionary keeps track of the top hit(s) for each scan/charge combo
			' Keys are scan_charge
			' Values are the clsMSGFPlusPSMs class, which keeps track of the top numberOfHitsPerScanToKeep hits for each scan/charge combo
			Dim dctScanChargeTopHits = New Dictionary(Of String, clsMSGFPlusPSMs)

			' This dictionary keeps track of the best score (lowest SpecEValue) for each scan/charge combo
			' Keys are scan_charge
			' Values the lowest SpecEValue for the scan/charge
			Dim dctScanChargeBestScore = New Dictionary(Of String, Double)

			Dim totalLinesProcessed As Int64 = 0
			Dim warningsLogged As Integer = 0

			Using swMergedFile = New StreamWriter(New FileStream(mergedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

				For iteration As Integer = 1 To numberOfClonedSteps

					Dim sourceFilePath = Path.Combine(m_WorkDir, m_Dataset & "_msgfdb_Part" & iteration & ".tsv")
					Dim linesRead As Integer = 0

					If m_DebugLevel >= 2 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Caching data from " & sourceFilePath)
					End If

					Using srSourceFile = New StreamReader(New FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
						While srSourceFile.Peek() > -1
							Dim strLineIn = srSourceFile.ReadLine()
							linesRead += 1
							totalLinesProcessed += 1

							If linesRead = 1 Then
								If iteration = 1 Then
									' Write the header line
									swMergedFile.WriteLine(strLineIn)

									Const IS_CASE_SENSITIVE As Boolean = False
									Dim lstHeaderNames = New List(Of String) From {"ScanNum", "Charge", "Peptide", "Protein", "SpecEValue"}
									dctHeaderMapping = clsGlobal.ParseHeaderLine(strLineIn, lstHeaderNames, IS_CASE_SENSITIVE)

									For Each headerName In lstHeaderNames
										If dctHeaderMapping(headerName) < 0 Then
											m_message = "Header " & headerName & " not found in " & Path.GetFileName(sourceFilePath) & "; unable to merge the MSGF+ .tsv files"
											Return IJobParams.CloseOutType.CLOSEOUT_FAILED
										End If
									Next

								End If
							Else
								Dim splitLine = strLineIn.Split(ControlChars.Tab)

								Dim scanNumber = splitLine(dctHeaderMapping("ScanNum"))
								Dim chargeState = splitLine(dctHeaderMapping("Charge"))

								Dim scanNumberValue As Integer
								Dim chargeStateValue As Integer
								Integer.TryParse(scanNumber, scanNumberValue)
								Integer.TryParse(chargeState, chargeStateValue)

								Dim scanChargeCombo = scanNumber & "_" & chargeState
								Dim peptide = splitLine(dctHeaderMapping("Peptide"))
								Dim protein = splitLine(dctHeaderMapping("Protein"))
								Dim specEValueText = splitLine(dctHeaderMapping("SpecEValue"))

								Dim specEValue As Double
								If Not Double.TryParse(specEValueText, specEValue) Then
									If warningsLogged < 10 Then
										clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "SpecEValue was not numeric: " & specEValueText & " in " & strLineIn)
										warningsLogged += 1

										If warningsLogged >= 10 Then
											clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Additional warnings will not be logged")
										End If
									End If

									Continue While
								End If

								Dim hitsForScan As clsMSGFPlusPSMs = Nothing
								Dim passesFilter As Boolean = False

								Dim udtPSM As clsMSGFPlusPSMs.udtPSMType
								udtPSM.Peptide = peptide
								udtPSM.SpecEValue = specEValue
								udtPSM.DataLine = strLineIn

								If dctScanChargeTopHits.TryGetValue(scanChargeCombo, hitsForScan) Then
									' Possibly store this value

									passesFilter = hitsForScan.AddPSM(udtPSM, protein)

									If passesFilter AndAlso specEValue < dctScanChargeBestScore(scanChargeCombo) Then
										dctScanChargeBestScore(scanChargeCombo) = specEValue
									End If

								Else
									' New entry for this scan/charge combo
									hitsForScan = New clsMSGFPlusPSMs(scanNumberValue, chargeStateValue, numberOfHitsPerScanToKeep)
									hitsForScan.AddPSM(udtPSM, protein)

									dctScanChargeTopHits.Add(scanChargeCombo, hitsForScan)
									dctScanChargeBestScore.Add(scanChargeCombo, specEValue)
								End If

							End If
						End While
					End Using

				Next

				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sorting results for " & dctScanChargeBestScore.Count & " lines of scan/charge combos")
				End If

				' Sort the data, then write to disk
				Dim lstScansByScore = From item In dctScanChargeBestScore Order By item.Value Select item.Key
				Dim filterPassingPSMCount As Integer = 0

				For Each scanChargeCombo In lstScansByScore

					Dim hitsForScan = dctScanChargeTopHits(scanChargeCombo)
					Dim lastPeptide As String = String.Empty

					For Each psm In hitsForScan.PSMs
						swMergedFile.WriteLine(psm.DataLine)

						If Not lstFilterPassingPeptides.Contains(psm.Peptide) Then
							lstFilterPassingPeptides.Add(psm.Peptide)
						End If

						If Not String.Equals(psm.Peptide, lastPeptide) Then
							filterPassingPSMCount += 1
							lastPeptide = psm.Peptide
						End If

					Next

				Next

				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Read " & totalLinesProcessed & " data lines from " & numberOfClonedSteps & " MSGF+ .tsv files; wrote " & filterPassingPSMCount & " PSMs to the merged file")
				End If

			End Using

			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS


		Catch ex As Exception
			m_message = "Error in ParallelMSGFPlusMergeTSVFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)

			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	Private Function ParallelMSGFPlusMergePepToProtMapFiles(
	  ByVal numberOfClonedSteps As Integer,
	  ByVal lstFilterPassingPeptides As SortedSet(Of String)) As IJobParams.CloseOutType

		Try
			Dim mergedFilePath = Path.Combine(m_WorkDir, m_Dataset & "_msgfdb_PepToProtMap.txt")
			Dim lstPeptideLinesToWrite = New List(Of String)
			Dim totalLinesProcessed As Int64 = 0

			Dim lstPepProtMappingWritten = New SortedSet(Of String)

			Dim lastPeptideFull As String = String.Empty
			Dim addCurrentPeptide As Boolean = False

			Using swMergedFile = New StreamWriter(New FileStream(mergedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

				For iteration As Integer = 1 To numberOfClonedSteps

					Dim sourceFilePath = Path.Combine(m_WorkDir, m_Dataset & "_msgfdb_Part" & iteration & "_PepToProtMap.txt")
					Dim linesRead As Integer = 0

					If m_DebugLevel >= 2 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Caching data from " & sourceFilePath)
					End If

					Using srSourceFile = New StreamReader(New FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
						While srSourceFile.Peek() > -1
							Dim strLineIn = srSourceFile.ReadLine()
							linesRead += 1
							totalLinesProcessed += 1

							If linesRead = 1 Then
								If iteration = 1 Then
									' Write the header line
									swMergedFile.WriteLine(strLineIn)
								End If
							Else
								Dim charIndex = strLineIn.IndexOf(ControlChars.Tab)
								If charIndex > 0 Then
									Dim peptideFull = strLineIn.Substring(0, charIndex)
									Dim peptide = clsMSGFPlusPSMs.RemovePrefixAndSuffix(peptideFull)

									If String.Equals(lastPeptideFull, peptideFull) OrElse lstFilterPassingPeptides.Contains(peptide) Then

										If Not String.Equals(lastPeptideFull, peptideFull) Then
											' Done processing the last peptide; we can now update lstPepProtMappingWritten to True for this peptide
											' to prevent it from getting added to the merged file again in the future

											If Not String.IsNullOrEmpty(lastPeptideFull) Then
												If Not lstPepProtMappingWritten.Contains(lastPeptideFull) Then
													lstPepProtMappingWritten.Add(lastPeptideFull)
												End If
											End If

											lastPeptideFull = String.Copy(peptideFull)
											addCurrentPeptide = Not lstPepProtMappingWritten.Contains(peptideFull)

										End If

										' Add this peptide if we didn't already add it during a previous iteration
										If addCurrentPeptide Then
											lstPeptideLinesToWrite.Add(strLineIn)
										End If
									End If

								End If
							End If
						End While
					End Using

				Next

				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sorting " & lstPeptideLinesToWrite.Count & " lines of data in ParallelMSGFPlusMergePepToProtMapFiles")
				End If

				' Sort the data, then write to disk
				lstPeptideLinesToWrite.Sort()

				For Each peptideLine In lstPeptideLinesToWrite
					swMergedFile.WriteLine(peptideLine)
				Next

				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Read " & totalLinesProcessed & " data lines from " & numberOfClonedSteps & " _PepToProtMap files; wrote " & lstPeptideLinesToWrite.Count & " data lines to the merged file")
				End If

			End Using

			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		Catch ex As Exception
			m_message = "Error in ParallelMSGFPlusMergePepToProtMapFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)

			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	''' <summary>
	''' Calls Ken's DLL to perform peptide hit extraction for Sequest data
	''' </summary>
	''' <returns>IJobParams.CloseOutType representing success or failure</returns>
	''' <remarks></remarks>
	Private Function PerformPeptideExtraction() As IJobParams.CloseOutType

		Dim Msg As String
		Dim Result As IJobParams.CloseOutType
		Dim PepExtractTool As New clsPeptideExtractWrapper(m_mgrParams, m_jobParams, m_StatusTools)

		'Run the extractor
		If m_DebugLevel > 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsExtractToolRunner.PerformPeptideExtraction(); Starting peptide extraction")
		End If
		Try
			Result = PepExtractTool.PerformExtraction
		Catch ex As Exception
			Msg = "clsExtractToolRunner.PerformPeptideExtraction(); Exception running extraction tool: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception running extraction tool")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) And _
		  (Result <> IJobParams.CloseOutType.CLOSEOUT_NO_DATA) Then
			'log error and return result calling routine handles the error appropriately
			Msg = "Error encountered during extraction"
			m_message = clsGlobal.AppendToComment(m_message, Msg)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.PerformPeptideExtraction(); " & Msg)
			Return Result
		End If

		'If there was a _syn.txt file created, but it contains no data, then we want to clean up and exit
		If Result = IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
			'log error and return result calling routine handles the error appropriately
			m_message = "No results above threshold"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return Result
		End If

	End Function

	''' <summary>
	''' Runs PeptideHitsResultsProcessor on Sequest output
	''' </summary>
	''' <returns>IJobParams.CloseOutType representing success or failure</returns>
	''' <remarks></remarks>
	Private Function RunPhrpForSequest() As IJobParams.CloseOutType

		Dim Msg As String
		Dim Result As IJobParams.CloseOutType
		Dim strSynFilePath As String

		m_PHRP = New clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams)

		'Run the processor
		If m_DebugLevel > 3 Then
			Msg = "clsExtractToolRunner.RunPhrpForSequest(); Starting PHRP"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
		End If
		Try
			Dim strTargetFilePath As String = Path.Combine(m_WorkDir, m_Dataset & "_syn.txt")
			strSynFilePath = String.Copy(strTargetFilePath)

			Result = m_PHRP.ExtractDataFromResults(strTargetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_SEQUEST)

		Catch ex As Exception
			Msg = "clsExtractToolRunner.RunPhrpForSequest(); Exception running PHRP: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
			Msg = "Error running PHRP"
			If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Validate that the mass errors are within tolerance
		Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
		If Not ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.Sequest, strParamFileName) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

	Private Function RunPhrpForXTandem() As IJobParams.CloseOutType

		Dim Msg As String
		Dim Result As IJobParams.CloseOutType
		Dim strSynFilePath As String

		m_PHRP = New clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams)

		'Run the processor
		If m_DebugLevel > 2 Then
			Msg = "clsExtractToolRunner.RunPhrpForXTandem(); Starting PHRP"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
		End If
		Try
			Dim strTargetFilePath As String = Path.Combine(m_WorkDir, m_Dataset & "_xt.xml")
			strSynFilePath = Path.Combine(m_WorkDir, m_Dataset & "_xt.txt")

			Result = m_PHRP.ExtractDataFromResults(strTargetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_XTANDEM)

		Catch ex As Exception
			Msg = "clsExtractToolRunner.RunPhrpForXTandem(); Exception running PHRP: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
			Msg = "Error running PHRP"
			If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Validate that the mass errors are within tolerance		
		' Use input.xml for the X!Tandem parameter file
		If Not ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.XTandem, "input.xml") Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

	Private Function RunPhrpForMSAlign() As IJobParams.CloseOutType

		Dim Msg As String

		Dim strTargetFilePath As String
		Dim strSynFilePath As String

		Dim Result As IJobParams.CloseOutType

		m_PHRP = New clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams)

		'Run the processor
		If m_DebugLevel > 3 Then
			Msg = "clsExtractToolRunner.RunPhrpForMSAlign(); Starting PHRP"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
		End If

		Try

			' Create the Synopsis file using the _MSAlign_ResultTable.txt file
			strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset & "_MSAlign_ResultTable.txt")
			strSynFilePath = Path.Combine(m_WorkDir, m_Dataset & "_msalign_syn.txt")

			Result = m_PHRP.ExtractDataFromResults(strTargetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSALIGN)

			If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
				Msg = "Error running PHRP"
				If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		Catch ex As Exception
			Msg = "clsExtractToolRunner.RunPhrpForMSAlign(); Exception running PHRP: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' Summarize the number of PSMs in _msalign_syn.txt
		Const eResultType As clsPHRPReader.ePeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSAlign
		Dim job As Integer = 0
		Dim blnPostResultsToDB As Boolean

		If Integer.TryParse(m_JobNum, job) Then
			blnPostResultsToDB = True
		Else
			blnPostResultsToDB = False
			Msg = "Job number is not numeric: " & m_JobNum & "; will not be able to post PSM results to the database"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
		End If

		Dim objSummarizer = New AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer(eResultType, m_Dataset, job, m_WorkDir)
		objSummarizer.MSGFThreshold = AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer.DEFAULT_MSGF_THRESHOLD
		objSummarizer.EValueThreshold = AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer.DEFAULT_EVALUE_THRESHOLD
		objSummarizer.FDRThreshold = AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer.DEFAULT_FDR_THRESHOLD

		objSummarizer.PostJobPSMResultsToDB = blnPostResultsToDB
		objSummarizer.SaveResultsToTextFile = False

		Dim blnSuccess = objSummarizer.ProcessMSGFResults()

		If Not blnSuccess Then
			If String.IsNullOrEmpty(objSummarizer.ErrorMessage) Then
				m_message = "Error summarizing the PSMs using clsMSGFResultsSummarizer"
			Else
				m_message &= "Error summarizing the PSMs: " & objSummarizer.ErrorMessage
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RunPhrpForMSAlign: " & m_message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Validate that the mass errors are within tolerance
		Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
		If Not ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MSAlign, strParamFileName) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

	Private Function RunPhrpForMODa(ByVal strFilteredMODaResultsFilePath As String) As IJobParams.CloseOutType


		Dim currentStep As String = "Initializing"

		Dim Msg As String

		Dim Result As IJobParams.CloseOutType

		Try

			m_PHRP = New clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams)

			'Run the processor
			If m_DebugLevel > 3 Then
				Msg = "clsExtractToolRunner.RunPhrpForMODa(); Starting PHRP"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
			End If

			Try
				' The goal:
				'   Create the _syn.txt files from the _moda.id.txt file

				currentStep = "Determining results file type based on the results file name"

				If Not File.Exists(strFilteredMODaResultsFilePath) Then
					m_message = "Filtered MODa results file not found: " & Path.GetFileName(strFilteredMODaResultsFilePath)
					Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
				End If

				Dim strSynFilePath = Path.Combine(m_WorkDir, m_Dataset & "_moda_syn.txt")

				' Create the Synopsis and First Hits files using the _moda.id.txt file
				Const CreatMODaFirstHitsFile As Boolean = True
				Const CreateMODaSynopsisFile As Boolean = True

				Result = m_PHRP.ExtractDataFromResults(strFilteredMODaResultsFilePath, CreatMODaFirstHitsFile, CreateMODaSynopsisFile, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MODA)

				If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
					Msg = "Error running PHRP"
					If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				' Confirm that the synopsis file was made
				If Not File.Exists(strSynFilePath) Then
					m_message = "Synopsis file not found: " & Path.GetFileName(strSynFilePath)
					Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA
				End If

				' Skip the _moda.id.txt file
				m_jobParams.AddResultFileToSkip(strFilteredMODaResultsFilePath)

			Catch ex As Exception
				Msg = "clsExtractToolRunner.RunPhrpForMODa(); Exception running PHRP: " & _
				 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
				m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

			'' Validate that the mass errors are within tolerance
			'Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
			'If Not ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MODa, strParamFileName) Then
			'	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			'Else
			'	Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			'End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RunPhrpForMODa at step " & currentStep, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	Private Function RunPhrpForMSGFDB() As IJobParams.CloseOutType

		Dim currentStep As String = "Initializing"

		Dim Msg As String

		Dim CreateMSGFDBFirstHitsFile As Boolean
		Dim CreateMSGFDBSynopsisFile As Boolean

		Dim strTargetFilePath As String
		Dim strSynFilePath As String

		Dim Result As IJobParams.CloseOutType

		Try

			m_PHRP = New clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams)

			'Run the processor
			If m_DebugLevel > 3 Then
				Msg = "clsExtractToolRunner.RunPhrpForMSGFDB(); Starting PHRP"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
			End If

			Try
				' The goal:
				'   Create the _fht.txt and _syn.txt files from the _msgfdb.txt file (which should already have been unzipped from the _msgfdb.zip file)
				'   or from the _msgfdb.tsv file

				currentStep = "Determining results file type based on the results file name"

				Dim splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", False)
				Dim numberOfClonedSteps = 1

				strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset & "_msgfdb.txt")
				If Not File.Exists(strTargetFilePath) Then
					' Processing MSGF+ results

					If splitFastaEnabled Then
						numberOfClonedSteps = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0)
					End If

					For iteration As Integer = 1 To numberOfClonedSteps
						currentStep = "Verifying that .tsv files exist; iteration " & iteration

						Dim suffixToAdd As String

						If splitFastaEnabled Then
							suffixToAdd = "_Part" & iteration
						Else
							suffixToAdd = String.Empty
						End If

						strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset & "_msgfdb" & suffixToAdd & ".tsv")

						If Not File.Exists(strTargetFilePath) Then
							' Need to create the .tsv file
							currentStep = "Creating .tsv file " & strTargetFilePath

							strTargetFilePath = ConvertMZIDToTSV(suffixToAdd)
							If String.IsNullOrEmpty(strTargetFilePath) Then
								Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
							End If
						End If
					Next

					If splitFastaEnabled Then

						currentStep = "Merging Parallel MSGF+ results"

						Dim eResult As IJobParams.CloseOutType

						' Keys in this dictionary are peptide sequences; values indicate whether the peptide (and its associated proteins) has been written to the merged _PepToProtMap.txt file
						Dim lstFilterPassingPeptides As SortedSet(Of String) = Nothing

						Dim numberOfHitsPerScanToKeep = m_jobParams.GetJobParameter("MergeResultsToKeepPerScan", 2)
						If numberOfHitsPerScanToKeep < 1 Then numberOfHitsPerScanToKeep = 1

						' Merge the TSV files (keeping the top scoring hit (or hits) for each scan)
						currentStep = "Merging the TSV files"
						eResult = ParallelMSGFPlusMergeTSVFiles(numberOfClonedSteps, numberOfHitsPerScanToKeep, lstFilterPassingPeptides)

						If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
							Return eResult
						End If

						' Merge the _PepToProtMap files (making sure we don't have any duplicates, and only keeping peptides that passed the filters)
						currentStep = "Merging the _PepToProtMap files"
						eResult = ParallelMSGFPlusMergePepToProtMapFiles(numberOfClonedSteps, lstFilterPassingPeptides)

						If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
							Return eResult
						End If

						strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset & "_msgfdb.tsv")
					End If

				End If

				strSynFilePath = Path.Combine(m_WorkDir, m_Dataset & "_msgfdb_syn.txt")

				' Create the Synopsis and First Hits files using the _msgfdb.txt file
				CreateMSGFDBFirstHitsFile = True
				CreateMSGFDBSynopsisFile = True

				Result = m_PHRP.ExtractDataFromResults(strTargetFilePath, CreateMSGFDBFirstHitsFile, CreateMSGFDBSynopsisFile, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSGFDB)

				If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
					Msg = "Error running PHRP"
					If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				If Not splitFastaEnabled Then
					Try
						' Delete the _msgfdb.txt or _msgfdb.tsv file
						File.Delete(strTargetFilePath)
					Catch ex As Exception
						' Ignore errors here
					End Try
				End If

			Catch ex As Exception
				Msg = "clsExtractToolRunner.RunPhrpForMSGFDB(); Exception running PHRP: " & _
				 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
				m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

			' Validate that the mass errors are within tolerance
			Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
			If Not ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MSGFDB, strParamFileName) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RunPhrpForMSGFDB at step " & currentStep, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	Private Function RunPhrpForInSpecT() As IJobParams.CloseOutType

		Dim Msg As String

		Dim CreateInspectFirstHitsFile As Boolean
		Dim CreateInspectSynopsisFile As Boolean

		Dim strTargetFilePath As String
		Dim strSynFilePath As String

		Dim blnSuccess As Boolean
		Dim Result As IJobParams.CloseOutType

		m_PHRP = New clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams)

		'Run the processor
		If m_DebugLevel > 3 Then
			Msg = "clsExtractToolRunner.RunPhrpForInSpecT(); Starting PHRP"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
		End If

		Try
			' The goal:
			'   Get the _fht.txt and _FScore_fht.txt files from the _inspect.txt file in the _inspect_fht.zip file
			'   Get the other files from the _inspect.txt file in the_inspect.zip file

			' Extract _inspect.txt from the _inspect_fht.zip file
			strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset & "_inspect_fht.zip")
			blnSuccess = MyBase.UnzipFile(strTargetFilePath)

			If Not blnSuccess Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Create the First Hits files using the _inspect.txt file
			CreateInspectFirstHitsFile = True
			CreateInspectSynopsisFile = False
			strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset & "_inspect.txt")
			Result = m_PHRP.ExtractDataFromResults(strTargetFilePath, CreateInspectFirstHitsFile, CreateInspectSynopsisFile, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_INSPECT)

			If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
				Msg = "Error running PHRP"
				If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Delete the _inspect.txt file
			File.Delete(strTargetFilePath)

			Threading.Thread.Sleep(250)


			' Extract _inspect.txt from the _inspect.zip file
			strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset & "_inspect.zip")
			blnSuccess = MyBase.UnzipFile(strTargetFilePath)

			If Not blnSuccess Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Create the Synopsis files using the _inspect.txt file
			CreateInspectFirstHitsFile = False
			CreateInspectSynopsisFile = True
			strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset & "_inspect.txt")
			strSynFilePath = Path.Combine(m_WorkDir, m_Dataset & "_inspect_syn.txt")

			Result = m_PHRP.ExtractDataFromResults(strTargetFilePath, CreateInspectFirstHitsFile, CreateInspectSynopsisFile, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_INSPECT)

			If (Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS) Then
				Msg = "Error running PHRP"
				If Not String.IsNullOrWhiteSpace(m_PHRP.ErrMsg) Then Msg &= "; " & m_PHRP.ErrMsg
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Try
				' Delete the _inspect.txt file
				File.Delete(strTargetFilePath)
			Catch ex As Exception
				' Ignore errors here
			End Try

		Catch ex As Exception
			Msg = "clsExtractToolRunner.RunPhrpForInSpecT(); Exception running PHRP: " & _
			 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' Validate that the mass errors are within tolerance
		Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
		If Not ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.Inspect, strParamFileName) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

	Protected Function RunPeptideProphet() As IJobParams.CloseOutType
		Const SYN_FILE_MAX_SIZE_MB As Integer = 200
		Const PEPPROPHET_RESULT_FILE_SUFFIX As String = "_PepProphet.txt"

		Dim Msg As String
		Dim fiSynFile As FileInfo

		Dim SynFile As String
		Dim strFileList() As String
		Dim strBaseName As String
		Dim strSynFileNameAndSize As String

		Dim strPepProphetOutputFilePath As String

		Dim eResult As IJobParams.CloseOutType
		Dim blnIgnorePeptideProphetErrors As Boolean

		Dim intFileIndex As Integer
		Dim sngParentSynFileSizeMB As Single
		Dim blnSuccess As Boolean

		blnIgnorePeptideProphetErrors = m_jobParams.GetJobParameter("IgnorePeptideProphetErrors", False)

		Dim progLoc As String = m_mgrParams.GetParam("PeptideProphetRunnerProgLoc")

		' verify that program file exists
		If Not File.Exists(progLoc) Then
			If progLoc.Length = 0 Then
				m_message = "Manager parameter PeptideProphetRunnerProgLoc is not defined in the Manager Control DB"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Else
				m_message = "Cannot find PeptideProphetRunner program file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & progLoc)
			End If
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		m_PeptideProphet = New clsPeptideProphetWrapper(progLoc)

		If m_DebugLevel >= 3 Then
			Msg = "clsExtractToolRunner.RunPeptideProphet(); Starting Peptide Prophet"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
		End If

		SynFile = Path.Combine(m_WorkDir, m_Dataset & "_syn.txt")

		'Check to see if Syn file exists
		fiSynFile = New FileInfo(SynFile)
		If Not fiSynFile.Exists Then
			Msg = "clsExtractToolRunner.RunPeptideProphet(); Syn file " & SynFile & " not found; unable to run peptide prophet"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Check the size of the Syn file
		' If it is too large, then we will need to break it up into multiple parts, process each part separately, and then combine the results
		sngParentSynFileSizeMB = CSng(fiSynFile.Length / 1024.0 / 1024.0)
		If sngParentSynFileSizeMB <= SYN_FILE_MAX_SIZE_MB Then
			ReDim strFileList(0)
			strFileList(0) = fiSynFile.FullName
		Else
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Synopsis file is " & sngParentSynFileSizeMB.ToString("0.0") & " MB, which is larger than the maximum size for peptide prophet (" & SYN_FILE_MAX_SIZE_MB & " MB); splitting into multiple sections")
			End If

			' File is too large; split it into multiple chunks
			ReDim strFileList(0)
			blnSuccess = SplitFileRoundRobin(fiSynFile.FullName, SYN_FILE_MAX_SIZE_MB * 1024 * 1024, True, strFileList)

			If blnSuccess Then
				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Synopsis file was split into " & strFileList.Length & " sections by SplitFileRoundRobin")
				End If
			Else
				Msg = "Error splitting synopsis file that is over " & SYN_FILE_MAX_SIZE_MB & " MB in size"

				If blnIgnorePeptideProphetErrors Then
					Msg &= "; Ignoring the error since 'IgnorePeptideProphetErrors' = True"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
					Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

			End If
		End If

		'Setup Peptide Prophet and run for each file in strFileList
		For intFileIndex = 0 To strFileList.Length - 1
			m_PeptideProphet.InputFile = strFileList(intFileIndex)
			m_PeptideProphet.Enzyme = "tryptic"
			m_PeptideProphet.OutputFolderPath = m_WorkDir
			m_PeptideProphet.DebugLevel = m_DebugLevel

			fiSynFile = New FileInfo(strFileList(intFileIndex))
			strSynFileNameAndSize = fiSynFile.Name & " (file size = " & (fiSynFile.Length / 1024.0 / 1024.0).ToString("0.00") & " MB"
			If strFileList.Length > 1 Then
				strSynFileNameAndSize &= "; parent syn file is " & sngParentSynFileSizeMB.ToString("0.00") & " MB)"
			Else
				strSynFileNameAndSize &= ")"
			End If

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Running peptide prophet on file " & strSynFileNameAndSize)
			End If

			eResult = m_PeptideProphet.CallPeptideProphet()

			If eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then

				' Make sure the Peptide Prophet output file was actually created
				strPepProphetOutputFilePath = Path.Combine(m_PeptideProphet.OutputFolderPath, _
				  Path.GetFileNameWithoutExtension(strFileList(intFileIndex)) & _
				  PEPPROPHET_RESULT_FILE_SUFFIX)

				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Peptide prophet processing complete; checking for file " & strPepProphetOutputFilePath)
				End If

				If Not File.Exists(strPepProphetOutputFilePath) Then

					Msg = "clsExtractToolRunner.RunPeptideProphet(); Peptide Prophet output file not found for synopsis file " & strSynFileNameAndSize
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)

					Msg = m_PeptideProphet.ErrMsg
					If Msg.Length > 0 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
					End If

					If blnIgnorePeptideProphetErrors Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True")
					Else
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "To ignore this error, update this job to use a settings file that has 'IgnorePeptideProphetErrors' set to True")
						eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
						Exit For
					End If
				End If
			Else
				Msg = "clsExtractToolRunner.RunPeptideProphet(); Error running Peptide Prophet on file " & strSynFileNameAndSize & _
				   ": " & m_PeptideProphet.ErrMsg
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)

				If blnIgnorePeptideProphetErrors Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True")
				Else
					eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
					Exit For
				End If
			End If

		Next

		If eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS OrElse blnIgnorePeptideProphetErrors Then
			If strFileList.Length > 1 Then

				' Delete each of the temporary synopsis files
				DeleteTemporaryFiles(strFileList)

				' We now need to recombine the peptide prophet result files

				' Update strFileList() to have the peptide prophet result file names
				strBaseName = Path.Combine(m_PeptideProphet.OutputFolderPath, Path.GetFileNameWithoutExtension(SynFile))

				For intFileIndex = 0 To strFileList.Length - 1
					strFileList(intFileIndex) = strBaseName & "_part" & (intFileIndex + 1).ToString & PEPPROPHET_RESULT_FILE_SUFFIX

					' Add this file to the global delete list
					m_jobParams.AddResultFileToSkip(strFileList(intFileIndex))
				Next intFileIndex

				' Define the final peptide prophet output file name
				strPepProphetOutputFilePath = strBaseName & PEPPROPHET_RESULT_FILE_SUFFIX

				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Combining " & strFileList.Length & " separate Peptide Prophet result files to create " & Path.GetFileName(strPepProphetOutputFilePath))
				End If

				blnSuccess = InterleaveFiles(strFileList, strPepProphetOutputFilePath, True)

				' Delete each of the temporary peptide prophet result files
				DeleteTemporaryFiles(strFileList)

				If blnSuccess Then
					eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS
				Else
					Msg = "Error interleaving the peptide prophet result files (FileCount=" & strFileList.Length & ")"
					If blnIgnorePeptideProphetErrors Then
						Msg &= "; Ignoring the error since 'IgnorePeptideProphetErrors' = True"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
						eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS
					Else
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
						eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
					End If
				End If
			End If

		End If

		Return eResult

	End Function

	''' <summary>
	''' Deletes each file in strFileList()
	''' </summary>
	''' <param name="strFileList">Full paths to files to delete</param>
	''' <remarks></remarks>
	Private Sub DeleteTemporaryFiles(ByVal strFileList() As String)
		Dim intFileIndex As Integer

		Threading.Thread.Sleep(1000)					   'Delay for 1 second
		PRISM.Processes.clsProgRunner.GarbageCollectNow()

		' Delete each file in strFileList
		For intFileIndex = 0 To strFileList.Length - 1
			If m_DebugLevel >= 5 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting file " & strFileList(intFileIndex))
			End If
			Try
				File.Delete(strFileList(intFileIndex))
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting file " & Path.GetFileName(strFileList(intFileIndex)) & ": " & ex.Message)
			End Try
		Next intFileIndex

	End Sub

	''' <summary>
	''' Reads each file in strFileList() line by line, writing the lines to strCombinedFilePath
	''' Can also check for a header line on the first line; if a header line is found in the first file,
	''' then the header is also written to the combined file
	''' </summary>
	''' <param name="strFileList">Files to combine</param>
	''' <param name="strCombinedFilePath">File to create</param>
	''' <param name="blnLookForHeaderLine">When true, then looks for a header line by checking if the first column contains a number</param>
	''' <returns>True if success; false if failure</returns>
	''' <remarks></remarks>
	Protected Function InterleaveFiles(ByRef strFileList() As String, _
	  ByVal strCombinedFilePath As String, _
	  ByVal blnLookForHeaderLine As Boolean) As Boolean

		Dim Msg As String
		Dim intIndex As Integer

		Dim intFileCount As Integer
		Dim srInFiles() As StreamReader
		Dim swOutFile As StreamWriter

		Dim strLineIn As String
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
				If File.Exists(strFileList(intIndex)) Then
					srInFiles(intIndex) = New StreamReader(New FileStream(strFileList(intIndex), FileMode.Open, FileAccess.Read, FileShare.Read))
				Else
					' File not found; unable to continue
					Msg = "Source peptide prophet file not found, unable to continue: " & strFileList(intIndex)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
					Return False
				End If
			Next

			' Create the output file

			swOutFile = New StreamWriter(New FileStream(strCombinedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

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


		Catch ex As Exception
			Msg = "Exception in clsExtractToolRunner.InterleaveFiles: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception in InterleaveFiles")
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Reads strSrcFilePath line-by-line and splits into multiple files such that none of the output 
	''' files has length greater than lngMaxSizeBytes. Can also check for a header line on the first line;
	''' if a header line is found, then all of the split files will be assigned the same header line
	''' </summary>
	''' <param name="strSrcFilePath">FilePath to parse</param>
	''' <param name="lngMaxSizeBytes">Maximum size of each file</param>
	''' <param name="blnLookForHeaderLine">When true, then looks for a header line by checking if the first column contains a number</param>
	''' <param name="strSplitFileList">Output array listing the full paths to the split files that were created</param>
	''' <returns>True if success, false if failure</returns>
	''' <remarks></remarks>
	Private Function SplitFileRoundRobin(ByVal strSrcFilePath As String, _
	 ByVal lngMaxSizeBytes As Int64, _
	 ByVal blnLookForHeaderLine As Boolean, _
	 ByRef strSplitFileList() As String) As Boolean

		Dim fiFileInfo As FileInfo
		Dim strBaseName As String

		Dim intLinesRead As Integer
		Dim intTargetFileIndex As Integer

		Dim Msg As String
		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim srInFile As StreamReader
		Dim swOutFiles() As StreamWriter

		Dim intSplitCount As Integer
		Dim intIndex As Integer

		Dim blnProcessLine As Boolean
		Dim blnSuccess As Boolean

		Try
			fiFileInfo = New FileInfo(strSrcFilePath)
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
				srInFile = New StreamReader(New FileStream(fiFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))

				' Create each of the output files
				ReDim strSplitFileList(intSplitCount - 1)
				ReDim swOutFiles(intSplitCount - 1)

				strBaseName = Path.Combine(fiFileInfo.DirectoryName, Path.GetFileNameWithoutExtension(fiFileInfo.Name))

				For intIndex = 0 To intSplitCount - 1
					strSplitFileList(intIndex) = strBaseName & "_part" & (intIndex + 1).ToString & Path.GetExtension(fiFileInfo.Name)
					swOutFiles(intIndex) = New StreamWriter(New FileStream(strSplitFileList(intIndex), FileMode.Create, FileAccess.Write, FileShare.Read))
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


		Catch ex As Exception
			Msg = "Exception in clsExtractToolRunner.SplitFileRoundRobin: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Exception in SplitFileRoundRobin")
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim blnSuccess As Boolean

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		' Lookup the version of the PeptideHitResultsProcessor
		Try

			Dim progLoc As String = m_mgrParams.GetParam("PHRPProgLoc")
			Dim diPHRP As DirectoryInfo = New DirectoryInfo(progLoc)

			' verify that program file exists
			If diPHRP.Exists Then
				MyBase.StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, Path.Combine(diPHRP.FullName, "PeptideHitResultsProcessor.dll"))
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PHRP folder not found at " & progLoc)
				Return False
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for the PeptideHitResultsProcessor: " & ex.Message)
			Return False
		End Try


		If m_jobParams.GetParam("ResultType") = clsAnalysisResources.RESULT_TYPE_SEQUEST Then
			'Sequest result type

			' Lookup the version of the PeptideFileExtractor
			If Not StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, "PeptideFileExtractor") Then
				Return False
			End If

			' Lookup the version of the PeptideProphetRunner

			Dim strPeptideProphetRunnerLoc As String = m_mgrParams.GetParam("PeptideProphetRunnerProgLoc")
			Dim ioPeptideProphetRunner As FileInfo = New FileInfo(strPeptideProphetRunnerLoc)

			If ioPeptideProphetRunner.Exists() Then
				' Lookup the version of the PeptideProphetRunner
				blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, ioPeptideProphetRunner.FullName)
				If Not blnSuccess Then Return False

				' Lookup the version of the PeptideProphetLibrary
				blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, Path.Combine(ioPeptideProphetRunner.DirectoryName, "PeptideProphetLibrary.dll"))
				If Not blnSuccess Then Return False
			End If

		End If

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo))
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	Protected Function ValidatePHRPResultMassErrors(ByVal strInputFilePath As String, ByVal eResultType As clsPHRPReader.ePeptideHitResultType, ByVal strSearchEngineParamFileName As String) As Boolean

		Dim blnSuccess As Boolean

		Try
			Dim oValidator As clsPHRPMassErrorValidator

			oValidator = New clsPHRPMassErrorValidator(m_Dataset, m_WorkDir, m_DebugLevel)

			blnSuccess = oValidator.ValidatePHRPResultMassErrors(strInputFilePath, eResultType, strSearchEngineParamFileName)
			If Not blnSuccess Then
				Dim toolName As String = m_jobParams.GetJobParameter("ToolName", "")

				If toolName.ToLower().StartsWith("inspect") Then
					' Ignore this error for inspect if running an unrestricted search
					Dim paramFileName As String = m_jobParams.GetJobParameter("ParmFileName", "")
					If paramFileName.IndexOf("Unrestrictive", StringComparison.OrdinalIgnoreCase) >= 0 Then
						blnSuccess = True
					End If
				End If

				If Not blnSuccess Then
					m_message = oValidator.ErrorMessage
				End If

			End If

		Catch ex As Exception
			m_message = "Exception calling ValidatePHRPResultMassErrors"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function
#End Region

#Region "Event handlers"
	Private Sub m_PeptideProphet_PeptideProphetRunning(ByVal PepProphetStatus As String, ByVal PercentComplete As Single) Handles m_PeptideProphet.PeptideProphetRunning
		Const PEPPROPHET_DETAILED_LOG_INTERVAL_SECONDS As Integer = 60
		Static dtLastPepProphetStatusLog As DateTime = DateTime.UtcNow.Subtract(New TimeSpan(0, 0, PEPPROPHET_DETAILED_LOG_INTERVAL_SECONDS * 2))

		m_progress = SEQUEST_PROGRESS_PHRP_DONE + CSng(PercentComplete / 3.0)
		m_StatusTools.UpdateAndWrite(m_progress)

		If m_DebugLevel >= 4 Then
			If DateTime.UtcNow.Subtract(dtLastPepProphetStatusLog).TotalSeconds >= PEPPROPHET_DETAILED_LOG_INTERVAL_SECONDS Then
				dtLastPepProphetStatusLog = DateTime.UtcNow
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Running peptide prophet: " & PepProphetStatus & "; " & PercentComplete & "% complete")
			End If
		End If
	End Sub

	Private Sub m_PHRP_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles m_PHRP.ProgressChanged
		Const PHRP_LOG_INTERVAL_SECONDS As Integer = 180
		Const PHRP_DETAILED_LOG_INTERVAL_SECONDS As Integer = 20

		Static dtLastPHRPStatusLog As DateTime = DateTime.UtcNow.Subtract(New TimeSpan(0, 0, PHRP_DETAILED_LOG_INTERVAL_SECONDS * 2))

		m_progress = SEQUEST_PROGRESS_EXTRACTION_DONE + CSng(percentComplete / 3.0)
		m_StatusTools.UpdateAndWrite(m_progress)

		If m_DebugLevel >= 1 Then
			If DateTime.UtcNow.Subtract(dtLastPHRPStatusLog).TotalSeconds >= PHRP_DETAILED_LOG_INTERVAL_SECONDS And m_DebugLevel >= 3 OrElse _
			   DateTime.UtcNow.Subtract(dtLastPHRPStatusLog).TotalSeconds >= PHRP_LOG_INTERVAL_SECONDS Then
				dtLastPHRPStatusLog = DateTime.UtcNow
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Running PHRP: " & taskDescription & "; " & percentComplete & "% complete")
			End If
		End If
	End Sub
#End Region

End Class
