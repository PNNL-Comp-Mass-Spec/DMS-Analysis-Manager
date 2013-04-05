'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/12/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerMSAlignHistone
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running MSAlign Histone analysis
	'*********************************************************************************************************

#Region "Constants and Enums"
	Protected Const MSAlign_CONSOLE_OUTPUT As String = "MSAlign_ConsoleOutput.txt"
	Protected Const MSAlign_Report_CONSOLE_OUTPUT As String = "MSAlign_Report_ConsoleOutput.txt"
	Protected Const MSAlign_JAR_NAME As String = "MsAlignPipeline.jar"

	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected Const OUTPUT_FILE_EXTENSION_PTM_SEARCH As String = "PTM_SEARCH_RESULT"	' XML file created by MsAlignPipeline.jar; detailed results
	Protected Const OUTPUT_FILE_EXTENSION_TOP_RESULT As String = "TOP_RESULT"			' XML file created by MsAlignPipeline.jar; filtered version of the PTM_SEARCH_RESULT file with the top hit for each spectrum
	Protected Const OUTPUT_FILE_EXTENSION_E_VALUE_RESULT As String = "E_VALUE_RESULT"	' XML file created by MsAlignPipeline.jar; filtered version of the PTM_SEARCH_RESULT file with E-Values assigned
	Protected Const OUTPUT_FILE_EXTENSION_OUTPUT_RESULT As String = "OUTPUT_RESULT"		' XML file created by MsAlignPipeline.jar; new version of the E_VALUE_RESULT file with Species_ID assigned

	Protected Const RESULT_TABLE_FILE_EXTENSION As String = "OUTPUT_TABLE"				' Tab-delimited text file created by MsAlignPipeline.jar; same content as the OUTPUT_RESULT file
	Protected Const RESULT_TABLE_NAME_SUFFIX As String = "_MSAlign_ResultTable.txt"		' This DMS plugin will rename the DatasetName.OUTPUT_TABLE file to DatasetName_MSAlign_ResultTable.txt

	Protected Const OUTPUT_FILE_EXTENSION_FAST_FILTER_COMBINED As String = "FAST_FILTER_COMBINED"	' XML file created by MsAlignPipeline.jar; we do not keep this file


	' Note that newer versions are assumed to have higher enum values
	Protected Enum eMSAlignVersionType
		v0pt9 = 0
	End Enum
#End Region

#Region "Structures"

	Protected Structure udtInputPropertyValuesType
		Public FastaFileName As String
		Public SpectrumFileName As String
		Public Sub Clear()
			FastaFileName = String.Empty
			SpectrumFileName = String.Empty
		End Sub
	End Structure

#End Region

#Region "Module Variables"

	Protected mToolVersionWritten As Boolean
	Protected mMSAlignVersion As String

	Protected mMSAlignProgLoc As String
	Protected mConsoleOutputErrorMsg As String

	Protected mMSAlignWorkFolderPath As String
	Protected mInputPropertyValues As udtInputPropertyValuesType

	Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
	''' <summary>
	''' Runs MSAlign tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType
		Dim CmdStr As String
		Dim intJavaMemorySize As Integer
		Dim eMSalignVersion As eMSAlignVersionType

		Dim result As IJobParams.CloseOutType
		Dim blnProcessingError As Boolean = False

		Dim eResult As IJobParams.CloseOutType
		Dim blnSuccess As Boolean

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSAlignHistone.RunTool(): Enter")
			End If

			' Verify that program files exist

			' JavaProgLoc will typically be "C:\Program Files\Java\jre6\bin\Java.exe"
			' Note that we need to run MSAlign with a 64-bit version of Java since it prefers to use 2 or more GB of ram
			Dim JavaProgLoc As String = m_mgrParams.GetParam("JavaLoc")
			If Not IO.File.Exists(JavaProgLoc) Then
				If JavaProgLoc.Length = 0 Then JavaProgLoc = "Parameter 'JavaLoc' not defined for this manager"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find Java: " & JavaProgLoc)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Determine the path to the MSAlign_Histone program
			' Note that 
			mMSAlignProgLoc = DetermineProgramLocation("MSAlign_Histone", "MSAlignHistoneProgLoc", IO.Path.Combine("jar", MSAlign_JAR_NAME))

			If String.IsNullOrWhiteSpace(mMSAlignProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Assume v0.9
			eMSalignVersion = eMSAlignVersionType.v0pt9

			' We will store the specific MSAlign version info in the database after the first line is written to file MSAlign_ConsoleOutput.txt

			mToolVersionWritten = False
			mMSAlignVersion = String.Empty
			mConsoleOutputErrorMsg = String.Empty

			' Clear InputProperties parameters
			mInputPropertyValues.Clear()
			mMSAlignWorkFolderPath = String.Empty

			' Copy the MS Align program files and associated files to the work directory
			' Note that this function will update mMSAlignWorkFolderPath
			If Not CopyMSAlignProgramFiles(mMSAlignProgLoc, eMSalignVersion) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Initialize the files in the input folder
			If Not InitializeInputFolder(mMSAlignWorkFolderPath, eMSalignVersion) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Read the MSAlign Parameter File
			Dim strParamFilePath As String = IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
			Dim strMSAlignCmdLineOptions As String = String.Empty

			blnSuccess = CreateMSAlignCommandLine(strParamFilePath, strMSAlignCmdLineOptions)
			If Not blnSuccess Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Unknown error parsing the MSAlign parameter file"
				End If
				Return result
			ElseIf String.IsNullOrEmpty(strMSAlignCmdLineOptions) Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Problem parsing MSAlign parameter file: command line switches are not present"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSAlign_Histone")

			' Lookup the amount of memory to reserve for Java; default to 2 GB 
			intJavaMemorySize = m_jobParams.GetJobParameter("MSAlignJavaMemorySize", 2000)
			If intJavaMemorySize < 512 Then intJavaMemorySize = 512

			'Set up and execute a program runner to run MSAlign_Histone
			CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -classpath jar\*; edu.iupui.msalign.align.histone.pipeline.MsAlignHistonePipelineConsole " & strMSAlignCmdLineOptions

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)

			CmdRunner = New clsRunDosProgram(mMSAlignWorkFolderPath)

			With CmdRunner
				.CreateNoWindow = True
				.CacheStandardOutput = False
				.EchoOutputToConsole = True

				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = IO.Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT)

			End With

			m_progress = PROGRESS_PCT_STARTING

			blnSuccess = CmdRunner.RunProgram(JavaProgLoc, CmdStr, "MSAlign_Histone", True)

			If Not mToolVersionWritten Then
				If String.IsNullOrWhiteSpace(mMSAlignVersion) Then
					ParseConsoleOutputFile(IO.Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT))
				End If
				mToolVersionWritten = StoreToolVersionInfo()
			End If

			If Not blnSuccess AndAlso String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
				' Parse the console output file one more time to see if an exception was logged
				ParseConsoleOutputFile(IO.Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT))
			End If

			If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
			End If


			If Not blnSuccess Then
				Dim Msg As String
				Msg = "Error running MSAlign_Histone"
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If CmdRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSAlign_Histone returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MSAlign_Histone failed (but exit code is 0)")
				End If

				blnProcessingError = True
				eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED

			Else

				' Make sure the output files were created
				If Not ValidateResultFiles() Then
					blnProcessingError = True
				Else
					' Create the HTML and XML files
					' Need to call MsAlignPipeline.jar again, but this time with a different classpath

					blnSuccess = MakeReportFiles(JavaProgLoc, strMSAlignCmdLineOptions, intJavaMemorySize)
					If Not blnSuccess Then blnProcessingError = True

					' Move the result files
					If Not MoveMSAlignResultFiles() Then
						blnProcessingError = True
					End If

					Dim strResultTableSourcePath As String
					strResultTableSourcePath = IO.Path.Combine(m_WorkDir, m_Dataset & "_" & RESULT_TABLE_FILE_EXTENSION)

					If Not blnProcessingError AndAlso IO.File.Exists(strResultTableSourcePath) Then

						' Make sure the _OUTPUT_TABLE.txt file is not empty
						' Make a copy of the OUTPUT_TABLE.txt file so that we can fix the header row (creating the RESULT_TABLE_NAME_SUFFIX file)

						If ValidateResultTableFile(eMSalignVersion, strResultTableSourcePath) Then
							eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS
						Else
							eResult = IJobParams.CloseOutType.CLOSEOUT_NO_DATA
						End If

					End If

					m_StatusTools.UpdateAndWrite(m_progress)
					If m_DebugLevel >= 3 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSAlign Search Complete")
					End If
				End If

			End If

			m_progress = PROGRESS_PCT_COMPLETE

			'Stop the job timer
			m_StopTime = System.DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			CmdRunner = Nothing

			'Make sure objects are released
			System.Threading.Thread.Sleep(2000)		'2 second delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			If blnProcessingError Then
				' Move the source files and any results to the Failed Job folder
				' Useful for debugging MSAlign problems
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
			m_message = "Error in MSAlignHistone->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return eResult

	End Function

	Protected Function CopyFastaCheckResidues(ByVal strSourceFilePath As String, ByVal strTargetFilePath As String) As Boolean
		Const RESIDUES_PER_LINE As Integer = 60

		Dim oReader As ProteinFileReader.FastaFileReader
		Dim reInvalidResidues As System.Text.RegularExpressions.Regex
		Dim strProteinResidues As String

		Dim intIndex As Integer
		Dim intResidueCount As Integer
		Dim intLength As Integer
		Dim intWarningCount As Integer = 0

		Try
			reInvalidResidues = New System.Text.RegularExpressions.Regex("[BJOUXZ]", Text.RegularExpressions.RegexOptions.Compiled)

			oReader = New ProteinFileReader.FastaFileReader()
			If Not oReader.OpenFile(strSourceFilePath) Then
				m_message = "Error opening fasta file in CopyFastaCheckResidues"
				Return False
			End If

			Using swNewFasta As System.IO.StreamWriter = New IO.StreamWriter(New IO.FileStream(strTargetFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))
				Do While oReader.ReadNextProteinEntry()

					swNewFasta.WriteLine(oReader.ProteinLineStartChar & oReader.HeaderLine)
					strProteinResidues = reInvalidResidues.Replace(oReader.ProteinSequence, "-")

					If intWarningCount < 5 AndAlso strProteinResidues.GetHashCode() <> oReader.ProteinSequence.GetHashCode() Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Changed invalid residues to '-' in protein " & oReader.ProteinName)
						intWarningCount += 1
					End If

					intIndex = 0
					intResidueCount = strProteinResidues.Length
					Do While intIndex < strProteinResidues.Length
						intLength = Math.Min(RESIDUES_PER_LINE, intResidueCount - intIndex)
						swNewFasta.WriteLine(strProteinResidues.Substring(intIndex, intLength))
						intIndex += RESIDUES_PER_LINE
					Loop

				Loop
			End Using

			oReader.CloseFile()

		Catch ex As Exception
			m_message = "Exception in CopyFastaCheckResidues"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

		Return True

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
			IO.File.Delete(IO.Path.Combine(m_WorkDir, m_Dataset & ".mzXML"))

			' Copy any search result files that are not empty from the MSAlign folder to the work directory
			Dim dctResultFiles As Generic.Dictionary(Of String, String)
			dctResultFiles = GetExpectedMSAlignResultFiles(m_Dataset)

			For Each kvItem As Generic.KeyValuePair(Of String, String) In dctResultFiles
				Dim fiSearchResultFile As IO.FileInfo = New IO.FileInfo(IO.Path.Combine(mMSAlignWorkFolderPath, kvItem.Key))

				If fiSearchResultFile.Exists AndAlso fiSearchResultFile.Length > 0 Then
					fiSearchResultFile.CopyTo(IO.Path.Combine(m_WorkDir, IO.Path.GetFileName(fiSearchResultFile.Name)))
				End If
			Next			

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
				strFolderPathToArchive = IO.Path.Combine(m_WorkDir, m_ResFolderName)
			End If
		End If

		' Copy the results folder to the Archive folder
		Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
		objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

	End Sub

	Private Function CopyMSAlignProgramFiles(ByVal strMSAlignJarFilePath As String, ByVal eMSalignVersion As eMSAlignVersionType) As Boolean

		Dim fiMSAlignJarFile As System.IO.FileInfo
		Dim diMSAlignSrc As System.IO.DirectoryInfo
		Dim diMSAlignWork As System.IO.DirectoryInfo

		Try
			fiMSAlignJarFile = New IO.FileInfo(strMSAlignJarFilePath)

			If Not fiMSAlignJarFile.Exists Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSAlign .Jar file not found: " & fiMSAlignJarFile.FullName)
				Return False
			End If

			' The source folder is one level up from the .Jar file
			diMSAlignSrc = New IO.DirectoryInfo(fiMSAlignJarFile.Directory.Parent.FullName)
			diMSAlignWork = New IO.DirectoryInfo(IO.Path.Combine(m_WorkDir, "MSAlign"))

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying MSAlign program file to the Work Directory")

			' Make sure the folder doesn't already exit
			If diMSAlignWork.Exists Then
				diMSAlignWork.Delete(True)
				System.Threading.Thread.Sleep(500)
			End If

			' Create the folder
			diMSAlignWork.Create()
			mMSAlignWorkFolderPath = diMSAlignWork.FullName

			' Create the subdirectories
			diMSAlignWork.CreateSubdirectory("html")
			diMSAlignWork.CreateSubdirectory("jar")
			diMSAlignWork.CreateSubdirectory("xml")
			diMSAlignWork.CreateSubdirectory("xsl")
			diMSAlignWork.CreateSubdirectory("etc")

			' Copy all files in the jar and xsl folders to the target
			Dim lstSubfolderNames As Generic.List(Of String) = New Generic.List(Of String)
			lstSubfolderNames.Add("jar")
			lstSubfolderNames.Add("xsl")
			lstSubfolderNames.Add("etc")

			For Each strSubFolder As String In lstSubfolderNames
				Dim strTargetSubfolder = IO.Path.Combine(diMSAlignWork.FullName, strSubFolder)

				Dim diSubfolder As System.IO.DirectoryInfo()
				diSubfolder = diMSAlignSrc.GetDirectories(strSubFolder)

				If diSubfolder.Length = 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Source MSAlign subfolder not found: " & strTargetSubfolder)
					Return False
				End If

				For Each fiFile As System.IO.FileInfo In diSubfolder(0).GetFiles()
					fiFile.CopyTo(IO.Path.Combine(strTargetSubfolder, fiFile.Name))
				Next

			Next

			' Copy the histone ptm XML files
			Dim fiSourceFiles As Generic.List(Of IO.FileSystemInfo)
			fiSourceFiles = diMSAlignSrc.GetFileSystemInfos("histone*_ptm.xml").ToList()

			For Each fiFile As IO.FileInfo In fiSourceFiles
				fiFile.CopyTo(IO.Path.Combine(diMSAlignWork.FullName, fiFile.Name))
			Next

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in CopyMSAlignProgramFiles: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Function CreateMSAlignCommandLine(ByVal strParamFilePath As String, ByRef strCommandLine As String) As Boolean

		' MSAlign_Histone syntax
		'
		' -a, --activation <CID|HCD|ETD|FILE>
		'        MS/MS activation type: use FILE for data set with several activation types.
		' -c  --cutoff <float>
		'        Cutoff value. (Use this option with -t).
		'        Default value is 0.01.
		' -e  --error <integer>
		'        Error tolerance in ppm.
		'        Default value 15.
		' -m, --modification <0|1|2>
		'        Number of modifications.
		'        Default value: 2.
		' -p, --protection <C0|C57|C58>
		'        Cystein protection.
		'        Default value: C0.
		' -r, --report <integer>
		'        Number of reported Protein-Spectrum-Matches for each spectrum.
		'        Default value 1.
		' -s, --search <TARGET|TARGET+DECOY>
		'        Searching against target or target+decoy (scrambled) protein database.
		' -t, --cutofftype <EVALUE|FDR>
		'        Use either EVALUE or FDR to filter out identified Protein-Spectrum-Matches.
		'        Default value EVALUE.


		' These key names must be lowercase
		Const INSTRUMENT_ACTIVATION_TYPE_KEY As String = "activation"
		Const SEARCH_TYPE_KEY As String = "search"

		Dim srInFile As System.IO.StreamReader
		Dim strLineIn As String

		Dim intEqualsIndex As Integer
		Dim strKeyName As String
		Dim strValue As String
		Dim dctParameterMap As Generic.Dictionary(Of String, String)

		strCommandLine = String.Empty

		Try
			' Initialize the dictionary that maps parameter names in the parameter file to command line switches
			dctParameterMap = New Generic.Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)
			dctParameterMap.Add("activation", "a")
			dctParameterMap.Add("search", "s")
			dctParameterMap.Add("protection", "p")
			dctParameterMap.Add("modification", "m")
			dctParameterMap.Add("error", "e")
			dctParameterMap.Add("cutoffType", "t")
			dctParameterMap.Add("cutoff", "c")
			dctParameterMap.Add("report", "r")

			' Open the parameter file
			srInFile = New IO.StreamReader(New IO.FileStream(strParamFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

			' The first two parameters on the command line are Fasta File name and input file name
			strCommandLine &= mInputPropertyValues.FastaFileName & " " & mInputPropertyValues.SpectrumFileName

			' Now append the parameters defined in the parameter file			
			Do While srInFile.Peek > -1
				strLineIn = srInFile.ReadLine()

				If strLineIn.TrimStart().StartsWith("#") OrElse String.IsNullOrWhiteSpace(strLineIn) Then
					' Comment line or blank line; skip it
				Else
					' Look for an equals sign
					intEqualsIndex = strLineIn.IndexOf("="c)

					If intEqualsIndex > 0 Then
						' Split the line on the equals sign
						strKeyName = strLineIn.Substring(0, intEqualsIndex).TrimEnd()
						If intEqualsIndex < strLineIn.Length - 1 Then
							strValue = strLineIn.Substring(intEqualsIndex + 1).Trim()
						Else
							strValue = String.Empty
						End If

						If strKeyName.ToLower() = INSTRUMENT_ACTIVATION_TYPE_KEY Then
							' If this is a bruker dataset, then we need to make sure that the value for this entry is not FILE
							' The reason is that the mzXML file created by Bruker's compass program does not include the scantype information (CID, ETD, etc.)
							Dim strToolName As String
							strToolName = m_jobParams.GetParam("ToolName")

							If strToolName = "MSAlign_Bruker" OrElse strToolName = "MSAlign_Histone_Bruker" Then
								If strValue.ToUpper() = "FILE" Then
									m_message = "Must specify an explicit scan type for " & strKeyName & " in the MSAlign parameter file (CID, HCD, or ETD)"

									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; this is required because Bruker-created mzXML files do not include activationMethod information in the precursorMz tag")

									srInFile.Close()

									Return False

								End If
							End If
						End If

						If strKeyName.ToLower() = SEARCH_TYPE_KEY Then
							If strValue.ToUpper() = "TARGET+DECOY" Then
								' Make sure the protein collection is not a Decoy protein collection
								Dim strProteinOptions As String
								strProteinOptions = m_jobParams.GetParam("ProteinOptions")

								If strProteinOptions.ToLower().Contains("seq_direction=decoy") Then
									m_message = "MSAlign parameter file contains searchType=TARGET+DECOY; protein options for this analysis job must contain seq_direction=forward, not seq_direction=decoy"

									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)

									srInFile.Close()

									Return False
								End If
							End If
						End If

						Dim strSwitch As String = String.Empty
						If dctParameterMap.TryGetValue(strKeyName, strSwitch) Then
							strCommandLine &= " -" & strSwitch & " " & strValue
						Else
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring unrecognized MSAlign_Histone parameter: " & strKeyName)
						End If

					Else
						' Unknown line format; skip it
					End If

				End If

			Loop

			srInFile.Close()

		Catch ex As Exception
			m_message = "Exception in CreateMSAlignCommandLine"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in CreateMSAlignCommandLine: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Function FilesMatch(ByVal strFilePath1 As String, ByVal strFilePath2 As String) As Boolean

		Dim blnFilesMatch As Boolean = False
		Try
			Dim fiFile1 As IO.FileInfo = New IO.FileInfo(strFilePath1)
			Dim fiFile2 As IO.FileInfo = New IO.FileInfo(strFilePath2)

			If fiFile1.Exists AndAlso fiFile2.Exists Then
				If fiFile1.Length = fiFile2.Length Then

					blnFilesMatch = True

					Using srInfile1 As IO.StreamReader = New IO.StreamReader(New IO.FileStream(fiFile1.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))
						Using srInfile2 As IO.StreamReader = New IO.StreamReader(New IO.FileStream(fiFile2.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))
							Do While srInfile1.Peek > -1
								If srInfile2.Peek < 0 Then
									blnFilesMatch = False
									Exit Do
								Else
									If srInfile1.ReadLine() <> srInfile2.ReadLine() Then
										blnFilesMatch = False
										Exit Do
									End If
								End If
							Loop
						End Using
					End Using

				End If
			End If
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in FilesMatch: " & ex.Message)
			blnFilesMatch = False
		End Try

		Return blnFilesMatch

	End Function

	Protected Function GetExpectedMSAlignResultFiles(ByVal strDatasetName As String) As Generic.Dictionary(Of String, String)
		' Keys in this dictionary are the expected file name
		' Values are the new name to rename the file to
		Dim dctResultFiles As Generic.Dictionary(Of String, String) = New Generic.Dictionary(Of String, String)
		Dim strBaseName As String = IO.Path.GetFileNameWithoutExtension(mInputPropertyValues.SpectrumFileName)

		dctResultFiles.Add(strBaseName & "." & OUTPUT_FILE_EXTENSION_PTM_SEARCH, strDatasetName & "_PTM_Search_Result.xml")
		dctResultFiles.Add(strBaseName & "." & OUTPUT_FILE_EXTENSION_TOP_RESULT, String.Empty)							' Don't keep this file since it's virtually identical to the E_VALUE_RESULT file
		dctResultFiles.Add(strBaseName & "." & OUTPUT_FILE_EXTENSION_E_VALUE_RESULT, strDatasetName & "_PTM_Search_Result_EValue.xml")
		dctResultFiles.Add(strBaseName & "." & OUTPUT_FILE_EXTENSION_OUTPUT_RESULT, strDatasetName & "_PTM_Search_Result_Final.xml")

		dctResultFiles.Add(strBaseName & "." & RESULT_TABLE_FILE_EXTENSION, strDatasetName & "_" & RESULT_TABLE_FILE_EXTENSION)

		Return dctResultFiles

	End Function

	Protected Function InitializeInputFolder(ByVal strMSAlignWorkFolderPath As String, ByVal eMSalignVersion As eMSAlignVersionType) As Boolean

		Dim fiFiles() As System.IO.FileInfo

		Try

			Dim fiSourceFolder As System.IO.DirectoryInfo
			fiSourceFolder = New System.IO.DirectoryInfo(m_WorkDir)

			' Copy the .Fasta file into the MSInput folder
			' MSAlign will crash if any non-standard residues are present (BJOUXZ)
			' Thus, we will read the source file with a reader and create a new fasta file

			' Define the path to the fasta file
			Dim OrgDbDir As String = m_mgrParams.GetParam("orgdbdir")
			Dim strFASTAFilePath As String = IO.Path.Combine(OrgDbDir, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

			Dim fiFastaFile As System.IO.FileInfo = New System.IO.FileInfo(strFASTAFilePath)

			If Not fiFastaFile.Exists Then
				' Fasta file not found
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Fasta file not found: " & fiFastaFile.FullName)
				Return False
			End If

			mInputPropertyValues.FastaFileName = String.Copy(fiFastaFile.Name)

			If Not CopyFastaCheckResidues(fiFastaFile.FullName, IO.Path.Combine(strMSAlignWorkFolderPath, mInputPropertyValues.FastaFileName)) Then
				If String.IsNullOrEmpty(m_message) Then m_message = "CopyFastaCheckResidues returned false"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			' Move the _msdeconv.msalign file to the MSAlign work folder
			fiFiles = fiSourceFolder.GetFiles("*" & clsAnalysisResourcesMSAlignHistone.MSDECONV_MSALIGN_FILE_SUFFIX)
			If fiFiles.Length = 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSAlign file not found in work directory")
				Return False
			Else
				mInputPropertyValues.SpectrumFileName = String.Copy(fiFiles(0).Name)
				fiFiles(0).MoveTo(IO.Path.Combine(strMSAlignWorkFolderPath, mInputPropertyValues.SpectrumFileName))
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in InitializeMSInputFolder: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Function MakeReportFiles(ByVal JavaProgLoc As String, ByVal strMSAlignCmdLineOptions As String, ByVal intJavaMemorySize As Integer) As Boolean

		Dim CmdStr As String
		Dim blnSuccess As Boolean

		Try

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating MSAlign_Histone Report Files")


			'Set up and execute a program runner to run MSAlign_Histone
			CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -classpath jar\*; edu.iupui.msalign.align.histone.view.HistoneHtmlConsole " & strMSAlignCmdLineOptions

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)

			CmdRunner = New clsRunDosProgram(mMSAlignWorkFolderPath)

			With CmdRunner
				.CreateNoWindow = True
				.CacheStandardOutput = False
				.EchoOutputToConsole = True

				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = IO.Path.Combine(m_WorkDir, MSAlign_Report_CONSOLE_OUTPUT)
			End With

			blnSuccess = CmdRunner.RunProgram(JavaProgLoc, CmdStr, "MSAlign_Histone", True)

			If Not blnSuccess Then
				Dim Msg As String
				Msg = "Error running MSAlign_Histone to create HTML and XML files"
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If CmdRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSAlign_Histone returned a non-zero exit code during report creation: " & CmdRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MSAlign_Histone failed during report creation (but exit code is 0)")
				End If
			Else
				m_jobParams.AddResultFileToSkip(MSAlign_Report_CONSOLE_OUTPUT)
			End If

		Catch ex As Exception
			m_message = "Exception creating MSAlign_Histone HTML and XML files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in MakeReportFiles: " & ex.Message)
			Return False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Parse the MSAlign console output file to determine the MSAlign version and to track the search progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output
		'
		' Start at Thu Apr 04 15:10:48 PDT 2013
		' MS-Align+ 0.9.0.16 2013-02-02
		' Fast filteration started.
		' Fast filteration finished.
		' Ptm search: Processing spectrum scan 4353...9% finished (0 minutes used).
		' Ptm search: Processing spectrum scan 4354...18% finished (1 minutes used).

		Static reExtractPercentFinished As New System.Text.RegularExpressions.Regex("(\d+)% finished", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
		Static dtLastProgressWriteTime As System.DateTime = System.DateTime.UtcNow

		Dim oMatch As System.Text.RegularExpressions.Match

		Try
			If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
				If m_DebugLevel >= 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
				End If

				Exit Sub
			End If

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
			End If


			Dim srInFile As System.IO.StreamReader
			Dim strLineIn As String
			Dim intLinesRead As Integer

			Dim intProgress As Int16
			Dim intActualProgress As Int16

			mConsoleOutputErrorMsg = String.Empty
			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

			intLinesRead = 0
			Do While srInFile.Peek() >= 0
				strLineIn = srInFile.ReadLine()
				intLinesRead += 1

				If Not String.IsNullOrWhiteSpace(strLineIn) Then
					If intLinesRead <= 2 AndAlso String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
						' Parse out the MSAlign version
						If strLineIn.ToLower.Contains("align") Then
							If m_DebugLevel >= 2 AndAlso String.IsNullOrWhiteSpace(mMSAlignVersion) Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSAlign version: " & strLineIn)
							End If

							mMSAlignVersion = String.Copy(strLineIn)
						Else
							If strLineIn.ToLower.Contains("error") OrElse strLineIn.Contains("[ java.lang") Then
								mConsoleOutputErrorMsg = "Error running MSAlign: " & strLineIn
							End If
						End If
					End If

					If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
						mConsoleOutputErrorMsg &= "; " & strLineIn
					Else

						' Update progress if the line starts with Processing spectrum
						If strLineIn.IndexOf("Processing spectrum") >= 0 Then
							oMatch = reExtractPercentFinished.Match(strLineIn)
							If oMatch.Success Then
								If Int16.TryParse(oMatch.Groups(1).Value, intProgress) Then
									intActualProgress = intProgress
								End If
							End If

						ElseIf strLineIn.Contains("[ java.lang") Then
							' This is likely an exception
							mConsoleOutputErrorMsg = "Error running MSAlign: " & strLineIn
						End If

					End If

				End If
			Loop

			srInFile.Close()

			If m_progress < intActualProgress Then
				m_progress = intActualProgress

				If m_DebugLevel >= 3 OrElse System.DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
					dtLastProgressWriteTime = System.DateTime.UtcNow
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
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String = String.Empty

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		strToolVersionInfo = String.Copy(mMSAlignVersion)

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(New System.IO.FileInfo(mMSAlignProgLoc))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	''' <summary>
	''' Reads the console output file and removes the majority of the percent finished messages
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub TrimConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		Static reExtractScan As New System.Text.RegularExpressions.Regex("Processing spectrum scan (\d+)", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
		Dim oMatch As System.Text.RegularExpressions.Match

		Try
			If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
				If m_DebugLevel >= 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
				End If

				Exit Sub
			End If

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Trimming console output file at " & strConsoleOutputFilePath)
			End If

			Dim srInFile As System.IO.StreamReader
			Dim swOutFile As System.IO.StreamWriter

			Dim strLineIn As String
			Dim blnKeepLine As Boolean

			Dim intScanNumber As Integer
			Dim strMostRecentProgressLine As String = String.Empty
			Dim strMostRecentProgressLineWritten As String = String.Empty

			Dim intScanNumberOutputThreshold As Integer

			Dim strTrimmedFilePath As String
			strTrimmedFilePath = strConsoleOutputFilePath & ".trimmed"

			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))
			swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strTrimmedFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

			intScanNumberOutputThreshold = 0
			Do While srInFile.Peek() >= 0
				strLineIn = srInFile.ReadLine()
				blnKeepLine = True

				oMatch = reExtractScan.Match(strLineIn)
				If oMatch.Success Then
					If Integer.TryParse(oMatch.Groups(1).Value, intScanNumber) Then
						If intScanNumber < intScanNumberOutputThreshold Then
							blnKeepLine = False
						Else
							' Write out this line and bump up intScanNumberOutputThreshold by 100
							intScanNumberOutputThreshold += 100
							strMostRecentProgressLineWritten = String.Copy(strLineIn)
						End If
					End If
					strMostRecentProgressLine = String.Copy(strLineIn)

				ElseIf strLineIn.StartsWith("Deconvolution finished") Then
					' Possibly write out the most recent progress line
					If String.Compare(strMostRecentProgressLine, strMostRecentProgressLineWritten) <> 0 Then
						swOutFile.WriteLine(strMostRecentProgressLine)
					End If
				End If

				If blnKeepLine Then
					swOutFile.WriteLine(strLineIn)
				End If
			Loop

			srInFile.Close()
			swOutFile.Close()

			' Wait 500 msec, then swap the files
			System.Threading.Thread.Sleep(500)

			Try
				System.IO.File.Delete(strConsoleOutputFilePath)
				System.IO.File.Move(strTrimmedFilePath, strConsoleOutputFilePath)
			Catch ex As Exception
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error replacing original console output file (" & strConsoleOutputFilePath & ") with trimmed version: " & ex.Message)
				End If
			End Try

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error trimming console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub

	Protected Function MoveMSAlignResultFiles() As Boolean

		Dim dctResultsFilesToMove As Generic.Dictionary(Of String, String)
		Dim blnProcessingError As Boolean = False

		Dim strEValueResultFilePath As String = String.Empty
		Dim strFinalResultFilePath As String = String.Empty

		Try
			dctResultsFilesToMove = GetExpectedMSAlignResultFiles(m_Dataset)

			For Each kvItem As Generic.KeyValuePair(Of String, String) In dctResultsFilesToMove
				Dim fiSearchResultFile As IO.FileInfo = New IO.FileInfo(IO.Path.Combine(mMSAlignWorkFolderPath, kvItem.Key))

				If Not fiSearchResultFile.Exists Then
					' Note that ValidateResultFiles should have already logged the missing files

				Else
					' Copy the results file to the work directory
					' Rename the file as we copy it

					If String.IsNullOrEmpty(kvItem.Value) Then
						' Skip this file
					Else					
						Dim strTargetFilePath As String
						strTargetFilePath = IO.Path.Combine(m_WorkDir, kvItem.Value)

						fiSearchResultFile.CopyTo(strTargetFilePath, True)

						If kvItem.Key.EndsWith(OUTPUT_FILE_EXTENSION_E_VALUE_RESULT) Then
							strEValueResultFilePath = strTargetFilePath
						ElseIf kvItem.Key.EndsWith(OUTPUT_FILE_EXTENSION_OUTPUT_RESULT) Then
							strFinalResultFilePath = strTargetFilePath
						End If

					End If

				End If
			Next

			' Zip the Html and XML folders
			ZipMSAlignResultFolder("html")
			ZipMSAlignResultFolder("XML")

			' Skip the E_VALUE_RESULT file if it is identical to the OUTPUT_RESULT file
			If Not String.IsNullOrEmpty(strEValueResultFilePath) AndAlso Not String.IsNullOrEmpty(strFinalResultFilePath) Then

				If FilesMatch(strEValueResultFilePath, strFinalResultFilePath) Then
					m_jobParams.AddResultFileToSkip(IO.Path.GetFileName(strEValueResultFilePath))
				End If

			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ValidateAndCopyResultFiles: " & ex.Message)
			Return False
		End Try

		If blnProcessingError Then
			Return False
		Else
			Return True
		End If

	End Function

	Protected Function ValidateResultFiles() As Boolean

		Dim dctResultFiles As Generic.Dictionary(Of String, String)
		Dim blnProcessingError As Boolean = False

		Try
			dctResultFiles = GetExpectedMSAlignResultFiles(m_Dataset)

			For Each kvItem As Generic.KeyValuePair(Of String, String) In dctResultFiles
				Dim fiSearchResultFile As IO.FileInfo = New IO.FileInfo(IO.Path.Combine(mMSAlignWorkFolderPath, kvItem.Key))

				If Not fiSearchResultFile.Exists Then
					Dim Msg As String
					Msg = "MSAlign results file not found (" & kvItem.Key & ")"

					If Not blnProcessingError Then
						' This is the first missing file; update the base-class comment
						m_message = clsGlobal.AppendToComment(m_message, Msg)
					End If

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & " (" & fiSearchResultFile.FullName & ")" & ", job " & m_JobNum)
					blnProcessingError = True
				End If
			Next

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ValidateResultFiles: " & ex.Message)
			Return False
		End Try

		If blnProcessingError Then
			Return False
		Else
			Return True
		End If

	End Function

	Protected Function ValidateResultTableFile(ByVal eMSalignVersion As eMSAlignVersionType, ByVal strSourceFilePath As String) As Boolean

		Dim strOutputFilePath As String

		Dim strLineIn As String
		Dim blnValidDataFound As Boolean
		Dim intLinesRead As Integer

		Try
			blnValidDataFound = False
			intLinesRead = 0

			strOutputFilePath = IO.Path.Combine(m_WorkDir, m_Dataset & RESULT_TABLE_NAME_SUFFIX)

			If Not System.IO.File.Exists(strSourceFilePath) Then
				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSAlign OUTPUT_TABLE file not found: " & strSourceFilePath)
				End If
				If String.IsNullOrEmpty(m_message) Then
					m_message = "MSAlign OUTPUT_TABLE file not found"
				End If
				Return False
			End If

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Validating that the MSAlign OUTPUT_TABLE file is not empty")
			End If

			' Open the input file
			Using srInFile As IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strSourceFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

				' Create the output file
				Using swOutFile As IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strOutputFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

					Do While srInFile.Peek > -1
						strLineIn = srInFile.ReadLine
						intLinesRead += 1

						If Not String.IsNullOrEmpty(strLineIn) Then

							If intLinesRead = 1 AndAlso strLineIn.EndsWith("FDR" & ControlChars.Tab) Then
								' The header line is missing the final column header; add it
								strLineIn &= "FragMethod"
							End If

							If Not blnValidDataFound Then

								Dim strSplitLine() As String
								strSplitLine = strLineIn.Split(ControlChars.Tab)

								If strSplitLine.Length > 1 Then
									' The first column has the source .msalign file name
									' The second column has Prsm_ID

									' Look for an integer in the second column
									Dim intValue As Integer
									If Integer.TryParse(strSplitLine(1), intValue) Then
										' Integer found; line is valid
										blnValidDataFound = True
									End If
								End If

							End If

							swOutFile.WriteLine(strLineIn)
						End If

					Loop

				End Using

			End Using

			If Not blnValidDataFound Then
				Dim Msg As String
				Msg = "MSAlign OUTPUT_TABLE file is empty"
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)
				Return False
			Else
				' Don't keep the original output table; only the new file we just created
				m_jobParams.AddResultFileToSkip(IO.Path.GetFileName(strSourceFilePath))
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ValidateResultTableFile: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Private Sub UpdateStatusRunning(ByVal sngPercentComplete As Single)
		m_progress = sngPercentComplete
		m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
	End Sub

	Protected Function ZipMSAlignResultFolder(ByVal strFolderName As String) As Boolean

		Dim objZipper As Ionic.Zip.ZipFile
		Dim strTargetFilePath As String
		Dim strSourceFolderPath As String

		Try
			strTargetFilePath = IO.Path.Combine(m_WorkDir, m_Dataset & "_MSAlign_Results_" & strFolderName.ToUpper() & ".zip")
			strSourceFolderPath = IO.Path.Combine(mMSAlignWorkFolderPath, strFolderName)

			' Confirm that the folder has one or more files or subfolders
			Dim diSourceFolder As New System.IO.DirectoryInfo(strSourceFolderPath)
			If diSourceFolder.GetFileSystemInfos.Length = 0 Then
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSAlign results folder is empty; nothing to zip: " & strSourceFolderPath)
				End If
				Return False
			End If

			If m_DebugLevel >= 1 Then
				Dim strLogMessage As String = "Zipping " & strFolderName.ToUpper() & " folder at " & strSourceFolderPath

				If m_DebugLevel >= 2 Then
					strLogMessage &= ": " & strTargetFilePath
				End If
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strLogMessage)

			End If

			objZipper = New Ionic.Zip.ZipFile(strTargetFilePath)
			objZipper.AddDirectory(strSourceFolderPath)
			objZipper.Save()
			System.Threading.Thread.Sleep(500)

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ZipMSAlignResultFolder: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

#End Region

#Region "Event Handlers"

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
		Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow
		Static dtLastConsoleOutputParse As System.DateTime = System.DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 5 seconds)
		If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = System.DateTime.UtcNow
			UpdateStatusRunning(m_progress)
		End If

		If System.DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = System.DateTime.UtcNow

			ParseConsoleOutputFile(IO.Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT))

			If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSAlignVersion) Then
				mToolVersionWritten = StoreToolVersionInfo()
			End If

		End If

	End Sub

#End Region

End Class
