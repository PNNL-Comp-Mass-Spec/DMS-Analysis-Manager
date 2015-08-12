'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/12/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerMSAlign
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running MSAlign analysis
	'*********************************************************************************************************

#Region "Constants and Enums"
	Protected Const MSAlign_CONSOLE_OUTPUT As String = "MSAlign_ConsoleOutput.txt"
	Protected Const MSAlign_JAR_NAME As String = "MSAlign.jar"

	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected Const RESULT_TABLE_NAME_SUFFIX As String = "_MSAlign_ResultTable.txt"
	Protected Const RESULT_TABLE_NAME_LEGACY As String = "result_table.txt"

	Protected Const RESULT_DETAILS_NAME_SUFFIX As String = "_MSAlign_ResultDetails.txt"
	Protected Const RESULT_DETAILS_NAME_LEGACY As String = "result.txt"

	' Note that newer versions are assumed to have higher enum values
	Protected Enum eMSAlignVersionType
		Unknown = 0
		v0pt5 = 1
		v0pt6 = 2
		v0pt7 = 3
	End Enum
#End Region

#Region "Structures"

	Protected Structure udtInputPropertyValuesType
		Public FastaFileName As String
		Public SpectrumFileName As String
		Public ResultTableFileName As String
		Public ResultDetailsFileName As String
		Public Sub Clear()
			FastaFileName = String.Empty
			SpectrumFileName = String.Empty
			ResultTableFileName = String.Empty
			ResultDetailsFileName = String.Empty
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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSAlign.RunTool(): Enter")
			End If

			' Verify that program files exist

			' JavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
			' Note that we need to run MSAlign with a 64-bit version of Java since it prefers to use 2 or more GB of ram
			Dim JavaProgLoc = GetJavaProgLoc()
			If String.IsNullOrEmpty(JavaProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Determine the path to the MSAlign program
			' Note that 
			mMSAlignProgLoc = DetermineProgramLocation("MSAlign", "MSAlignProgLoc", System.IO.Path.Combine("jar", MSAlign_JAR_NAME))

			If String.IsNullOrWhiteSpace(mMSAlignProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			eMSalignVersion = eMSAlignVersionType.Unknown
			If mMSAlignProgLoc.Contains(System.IO.Path.DirectorySeparatorChar & "v0.5" & System.IO.Path.DirectorySeparatorChar) Then
				eMSalignVersion = eMSAlignVersionType.v0pt5
			ElseIf mMSAlignProgLoc.Contains(System.IO.Path.DirectorySeparatorChar & "v0.6.") Then
				eMSalignVersion = eMSAlignVersionType.v0pt6
			ElseIf mMSAlignProgLoc.Contains(System.IO.Path.DirectorySeparatorChar & "v0.7.") Then
				eMSalignVersion = eMSAlignVersionType.v0pt7
			Else
				' Assume v0.7
				eMSalignVersion = eMSAlignVersionType.v0pt7
			End If

			' Store the MSAlign version info in the database after the first line is written to file MSAlign_ConsoleOutput.txt
			' (only valid for MSAlign 0.6.2 or newer)

			mToolVersionWritten = False
			mMSAlignVersion = String.Empty
			mConsoleOutputErrorMsg = String.Empty

			' Clear InputProperties parameters
			mInputPropertyValues.Clear()
			mMSAlignWorkFolderPath = String.Empty

			' Copy the MS Align program files and associated files to the work directory
			If Not CopyMSAlignProgramFiles(mMSAlignProgLoc, eMSalignVersion) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Initialize the MSInput folder
			If Not InitializeMSInputFolder(mMSAlignWorkFolderPath, eMSalignVersion) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSAlign")

			' Lookup the amount of memory to reserve for Java; default to 2 GB 
			intJavaMemorySize = m_jobParams.GetJobParameter("MSAlignJavaMemorySize", 2000)
			If intJavaMemorySize < 512 Then intJavaMemorySize = 512

			'Set up and execute a program runner to run MSAlign
			If eMSalignVersion = eMSAlignVersionType.v0pt5 Then
				CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -classpath jar\malign.jar;jar\* edu.ucsd.msalign.spec.web.Pipeline .\"
			Else
				CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -classpath jar\*; edu.ucsd.msalign.align.console.MsAlignPipeline .\"
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)

			CmdRunner = New clsRunDosProgram(mMSAlignWorkFolderPath)

			With CmdRunner
				.CreateNoWindow = True
				.CacheStandardOutput = False
				.EchoOutputToConsole = True

				If eMSalignVersion = eMSAlignVersionType.v0pt5 Then
					.WriteConsoleOutputToFile = False
				Else
					.WriteConsoleOutputToFile = True
					.ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT)
				End If

			End With

			m_progress = PROGRESS_PCT_STARTING

			blnSuccess = CmdRunner.RunProgram(JavaProgLoc, CmdStr, "MSAlign", True)

			If Not mToolVersionWritten Then
				If String.IsNullOrWhiteSpace(mMSAlignVersion) Then
					ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT))
				End If
				mToolVersionWritten = StoreToolVersionInfo()
			End If

			If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
			End If


			If Not blnSuccess Then
				Dim Msg As String
				Msg = "Error running MSAlign"
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If CmdRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSAlign returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MSAlign failed (but exit code is 0)")
				End If

				blnProcessingError = True
				eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED

			Else
				' Make sure the output files were created
				If Not ValidateAndCopyResultFiles(eMSalignVersion) Then
					blnProcessingError = True
				End If

				Dim strResultTableFilePath As String
				strResultTableFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & RESULT_TABLE_NAME_SUFFIX)

				If eMSalignVersion = eMSAlignVersionType.v0pt5 Then
					' Add a header to the _ResultTable.txt file
					AddResultTableHeaderLine(strResultTableFilePath)
				End If

				' Make sure the _ResultTable.txt file is not empty
				If blnProcessingError Then
					eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
				Else
					If ValidateResultTableFile(strResultTableFilePath) Then
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

			m_progress = PROGRESS_PCT_COMPLETE

			'Stop the job timer
			m_StopTime = System.DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			CmdRunner = Nothing

			'Make sure objects are released
			System.Threading.Thread.Sleep(500)        ' 500 msec delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			If eMSalignVersion <> eMSAlignVersionType.v0pt5 Then
				' Trim the console output file to remove the majority of the % finished messages
				TrimConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT))
			End If

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
			m_message = "Error in MSAlignPlugin->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return eResult

	End Function

	Protected Function AddResultTableHeaderLine(ByVal strSourceFilePath As String) As Boolean

		Try
			If Not System.IO.File.Exists(strSourceFilePath) Then Return False

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Adding header line to MSAlign_ResultTable.txt file")
			End If

            Dim strTargetFilePath As String
            strTargetFilePath = strSourceFilePath & ".tmp"

            ' Open the input file and
            ' create the output file
            Using srInFile = New System.IO.StreamReader(New System.IO.FileStream(strSourceFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite)),
                  swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strTargetFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

                Dim strHeaderLine As String
                strHeaderLine = "Prsm_ID" & ControlChars.Tab & _
                  "Spectrum_ID" & ControlChars.Tab & _
                  "Protein_Sequence_ID" & ControlChars.Tab & _
                  "Spectrum_ID" & ControlChars.Tab & _
                  "Scan(s)" & ControlChars.Tab & _
                  "#peaks" & ControlChars.Tab & _
                  "Charge" & ControlChars.Tab & _
                  "Precursor_mass" & ControlChars.Tab & _
                  "Protein_name" & ControlChars.Tab & _
                  "Protein_mass" & ControlChars.Tab & _
                  "First_residue" & ControlChars.Tab & _
                  "Last_residue" & ControlChars.Tab & _
                  "Peptide" & ControlChars.Tab & _
                  "#unexpected_modifications" & ControlChars.Tab & _
                  "#matched_peaks" & ControlChars.Tab & _
                  "#matched_fragment_ions" & ControlChars.Tab & _
                  "E-value"

                swOutFile.WriteLine(strHeaderLine)

                Do While Not srInFile.EndOfStream
                    swOutFile.WriteLine(srInFile.ReadLine)
                Loop

            End Using

            System.Threading.Thread.Sleep(500)

            ' Delete the source file, then rename the new file to match the source file
            System.IO.File.Delete(strSourceFilePath)
            System.Threading.Thread.Sleep(500)

            System.IO.File.Move(strTargetFilePath, strSourceFilePath)

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in AddResultTableHeaderLine: " & ex.Message)
            Return False
        End Try

		Return True

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

			Using swNewFasta As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strTargetFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))
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
			System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & ".mzXML"))
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
				strFolderPathToArchive = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
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
			fiMSAlignJarFile = New System.IO.FileInfo(strMSAlignJarFilePath)

			If Not fiMSAlignJarFile.Exists Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSAlign .Jar file not found: " & fiMSAlignJarFile.FullName)
				Return False
			End If

			' The source folder is one level up from the .Jar file
			diMSAlignSrc = New System.IO.DirectoryInfo(fiMSAlignJarFile.Directory.Parent.FullName)
			diMSAlignWork = New System.IO.DirectoryInfo(System.IO.Path.Combine(m_WorkDir, "MSAlign"))

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
			diMSAlignWork.CreateSubdirectory("msinput")
			diMSAlignWork.CreateSubdirectory("msoutput")
			diMSAlignWork.CreateSubdirectory("xml")
			diMSAlignWork.CreateSubdirectory("xsl")
			If eMSalignVersion <> eMSAlignVersionType.v0pt5 Then
				diMSAlignWork.CreateSubdirectory("etc")
			End If

			' Copy all files in the jar and xsl folders to the target
			Dim lstSubfolderNames As New System.Collections.Generic.List(Of String)()
			lstSubfolderNames.Add("jar")
			lstSubfolderNames.Add("xsl")
			If eMSalignVersion <> eMSAlignVersionType.v0pt5 Then
				lstSubfolderNames.Add("etc")
			End If

			For Each strSubFolder As String In lstSubfolderNames
				Dim strTargetSubfolder = System.IO.Path.Combine(diMSAlignWork.FullName, strSubFolder)

				Dim diSubfolder As System.IO.DirectoryInfo()
				diSubfolder = diMSAlignSrc.GetDirectories(strSubFolder)

				If diSubfolder.Length = 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Source MSAlign subfolder not found: " & strTargetSubfolder)
					Return False
				End If

				For Each ioFile As System.IO.FileInfo In diSubfolder(0).GetFiles()
					ioFile.CopyTo(System.IO.Path.Combine(strTargetSubfolder, ioFile.Name))
				Next

			Next

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in CopyMSAlignProgramFiles: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Function CreateInputPropertiesFile(ByVal strParamFilePath As String, ByVal strMSInputFolderPath As String, eMSalignVersion As eMSAlignVersionType) As Boolean

		Const DB_FILENAME_LEGACY As String = "database"
		Const DB_FILENAME As String = "databaseFileName"
		Const SPEC_FILENAME As String = "spectrumFileName"
		Const TABLE_OUTPUT_FILENAME As String = "tableOutputFileName"
		Const DETAIL_OUTPUT_FILENAME As String = "detailOutputFileName"

		' These key names must be lowercase
		Const INSTRUMENT_TYPE_KEY As String = "instrument"			' This only applies to v0.5 of MSAlign
		Const INSTRUMENT_ACTIVATION_TYPE_KEY As String = "activation"
		Const SEARCH_TYPE_KEY As String = "searchtype"
		Const CUTOFF_TYPE_KEY As String = "cutofftype"
		Const CUTOFF_KEY As String = "cutoff"

        Dim strLineIn As String

		Dim intEqualsIndex As Integer
		Dim strKeyName As String
		Dim strValue As String
		Dim dctLegacyKeyMap As System.Collections.Generic.Dictionary(Of String, String)
		Dim blnEValueCutoffType As Boolean = False

		Try
			' Initialize the dictionary that maps new names to legacy names
			' Version 0.5 used the legacy names, e.g. it used "threshold" instead of "eValueThreshold"
			dctLegacyKeyMap = New System.Collections.Generic.Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)
			dctLegacyKeyMap.Add("errorTolerance", "fragmentTolerance")
			dctLegacyKeyMap.Add("eValueThreshold", "threshold")
			dctLegacyKeyMap.Add("shiftNumber", "shifts")
			dctLegacyKeyMap.Add("cysteineProtection", "protection")
			dctLegacyKeyMap.Add("activation", "instrument")

			' Note: Starting with version 0.7, the "eValueThreshold" parameter was replaced with two new parameters:
			' cutoffType and cutoff

            Dim strOutputFilePath As String = System.IO.Path.Combine(strMSInputFolderPath, "input.properties")

            ' Open the input file and
            ' Create the output file
            Using srInFile = New System.IO.StreamReader(New System.IO.FileStream(strParamFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite)),
                  swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strOutputFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

                ' Write out the database name and input file name
                If eMSalignVersion = eMSAlignVersionType.v0pt5 Then
                    swOutFile.WriteLine(DB_FILENAME_LEGACY & "=" & mInputPropertyValues.FastaFileName)
                    ' Input file name is assumed to be input_data
                Else
                    swOutFile.WriteLine(DB_FILENAME & "=" & mInputPropertyValues.FastaFileName)
                    swOutFile.WriteLine(SPEC_FILENAME & "=" & mInputPropertyValues.SpectrumFileName)
                End If

                Do While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()

                    If strLineIn.TrimStart().StartsWith("#") OrElse String.IsNullOrWhiteSpace(strLineIn) Then
                        ' Comment line or blank line; write it out as-is
                        swOutFile.WriteLine(strLineIn)
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

                            If strKeyName.ToLower() = INSTRUMENT_ACTIVATION_TYPE_KEY OrElse strKeyName.ToLower() = INSTRUMENT_TYPE_KEY Then
                                ' If this is a bruker dataset, then we need to make sure that the value for this entry is not FILE
                                ' The reason is that the mzXML file created by Bruker's compass program does not include the scantype information (CID, ETD, etc.)
                                Dim strToolName As String
                                strToolName = m_jobParams.GetParam("ToolName")

                                If strToolName = "MSAlign_Bruker" Then
                                    If strValue.ToUpper() = "FILE" Then
                                        m_message = "Must specify an explicit scan type for " & strKeyName & " in the MSAlign parameter file (CID, HCD, or ETD)"

                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; this is required because Bruker-created mzXML files do not include activationMethod information in the precursorMz tag")

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

                                        Return False
                                    End If
                                End If
                            End If

                            ' Examine the key name to determine what to do
                            Select Case strKeyName.ToLower()
                                Case DB_FILENAME_LEGACY.ToLower(), DB_FILENAME.ToLower(), SPEC_FILENAME.ToLower()
                                    ' Skip this line; we defined it above
                                Case TABLE_OUTPUT_FILENAME.ToLower(), DETAIL_OUTPUT_FILENAME.ToLower()
                                    ' Skip this line; we'll define it later							
                                Case Else

                                    If eMSalignVersion = eMSAlignVersionType.v0pt5 Then
                                        ' Running a legacy version; rename the keys

                                        Dim strLegacyKeyName As String = String.Empty
                                        If dctLegacyKeyMap.TryGetValue(strKeyName, strLegacyKeyName) Then

                                            If strLegacyKeyName = "protection" Then
                                                ' Need to update the value
                                                Select Case strValue.ToUpper()
                                                    Case "C57"
                                                        strValue = "Carbamidoemetylation"
                                                    Case "C58"
                                                        strValue = "Carboxymethylation"
                                                    Case Else
                                                        ' Includes "C0"
                                                        strValue = "None"
                                                End Select

                                            ElseIf strLegacyKeyName = "instrument" Then
                                                If strValue = "FILE" Then
                                                    ' Legacy mode does not support "FILE"
                                                    ' Auto-switch to CID and log a warning message
                                                    strValue = "CID"
                                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Using instrument mode 'CID' since v0.5 of MSAlign does not support reading the activation mode from the msalign file")
                                                End If
                                            ElseIf strKeyName.ToLower() = CUTOFF_TYPE_KEY Then
                                                If strValue.ToUpper() = "EVALUE" Then
                                                    blnEValueCutoffType = True
                                                End If
                                            ElseIf strKeyName.ToLower() = CUTOFF_KEY Then
                                                ' v0.5 doesn't support the cutoff parameter
                                                ' If the parameter file had cutoffType=EVALUE then we're OK; otherwise abort
                                                If blnEValueCutoffType Then
                                                    strLegacyKeyName = "threshold"
                                                Else
                                                    m_message = "MSAlign parameter file contains a non-EValue cutoff value; this is not compatible with MSAlign v0.5"
                                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                                                    Return False
                                                End If

                                            End If

                                            swOutFile.WriteLine(strLegacyKeyName & "=" & strValue)
                                        End If
                                    Else

                                        If eMSalignVersion >= eMSAlignVersionType.v0pt7 AndAlso strKeyName.ToLower() = "eValueThreshold" Then
                                            ' v0.7 and up use cutoffType and cutoff instead of eValueThreshold
                                            swOutFile.WriteLine("cutoffType=EVALUE")
                                            swOutFile.WriteLine("cutoff=" & strValue)

                                        ElseIf eMSalignVersion = eMSAlignVersionType.v0pt6 AndAlso strKeyName.ToLower() = CUTOFF_TYPE_KEY Then
                                            If strValue.ToUpper() = "EVALUE" Then
                                                blnEValueCutoffType = True
                                            End If
                                        ElseIf eMSalignVersion = eMSAlignVersionType.v0pt6 AndAlso strKeyName.ToLower() = CUTOFF_KEY Then
                                            If blnEValueCutoffType Then
                                                ' v0.6 doesn't support the cutoff parameter, just eValueThreshold
                                                swOutFile.WriteLine("eValueThreshold=" & strValue)
                                            Else
                                                m_message = "MSAlign parameter file contains a non-EValue cutoff value; this is not compatible with MSAlign v0.6"
                                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                                                Return False
                                            End If
                                        Else
                                            ' Write out as-is
                                            swOutFile.WriteLine(strLineIn)
                                        End If

                                    End If
                            End Select
                        Else
                            ' Unknown line format; skip it
                        End If

                    End If

                Loop


                If eMSalignVersion = eMSAlignVersionType.v0pt5 Then
                    mInputPropertyValues.ResultTableFileName = RESULT_TABLE_NAME_LEGACY
                    mInputPropertyValues.ResultDetailsFileName = RESULT_DETAILS_NAME_LEGACY
                Else
                    mInputPropertyValues.ResultTableFileName = m_Dataset & RESULT_TABLE_NAME_SUFFIX
                    mInputPropertyValues.ResultDetailsFileName = m_Dataset & RESULT_DETAILS_NAME_SUFFIX
                End If

                If eMSalignVersion <> eMSAlignVersionType.v0pt5 Then
                    swOutFile.WriteLine(TABLE_OUTPUT_FILENAME & "=" & mInputPropertyValues.ResultTableFileName)
                    swOutFile.WriteLine(DETAIL_OUTPUT_FILENAME & "=" & mInputPropertyValues.ResultDetailsFileName)
                End If

            End Using

            ' Copy the newly created input.properties file to the work directory
            System.IO.File.Copy(strOutputFilePath, System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileName(strOutputFilePath)), True)

        Catch ex As Exception
            m_message = "Exception in CreateInputPropertiesFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in CreateInputPropertiesFile: " & ex.Message)
            Return False
        End Try

		Return True

	End Function


	Protected Function InitializeMSInputFolder(ByVal strMSAlignWorkFolderPath As String, ByVal eMSalignVersion As eMSAlignVersionType) As Boolean

		Dim strMSInputFolderPath As String
		Dim fiFiles() As System.IO.FileInfo

		Try
			strMSInputFolderPath = System.IO.Path.Combine(strMSAlignWorkFolderPath, "msinput")
			Dim fiSourceFolder As System.IO.DirectoryInfo
			fiSourceFolder = New System.IO.DirectoryInfo(m_WorkDir)

			' Copy the .Fasta file into the MSInput folder
			' MSAlign will crash if any non-standard residues are present (BJOUXZ)
			' Thus, we will read the source file with a reader and create a new fasta file

			' Define the path to the fasta file
			Dim OrgDbDir As String = m_mgrParams.GetParam("orgdbdir")
			Dim strFASTAFilePath As String = System.IO.Path.Combine(OrgDbDir, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

			Dim fiFastaFile As System.IO.FileInfo = New System.IO.FileInfo(strFASTAFilePath)

			If Not fiFastaFile.Exists Then
				' Fasta file not found
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Fasta file not found: " & fiFastaFile.FullName)
				Return False
			End If

			mInputPropertyValues.FastaFileName = String.Copy(fiFastaFile.Name)

			If Not CopyFastaCheckResidues(fiFastaFile.FullName, System.IO.Path.Combine(strMSInputFolderPath, mInputPropertyValues.FastaFileName)) Then
				If String.IsNullOrEmpty(m_message) Then m_message = "CopyFastaCheckResidues returned false"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			' Move the _msdeconv.msalign file to the MSInput folder
			fiFiles = fiSourceFolder.GetFiles("*" & clsAnalysisResourcesMSAlign.MSDECONV_MSALIGN_FILE_SUFFIX)
			If fiFiles.Length = 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSAlign file not found in work directory")
				Return False
			Else
				If eMSalignVersion = eMSAlignVersionType.v0pt5 Then
					' Rename the file to input_data when we move it
					mInputPropertyValues.SpectrumFileName = "input_data"
				Else
					mInputPropertyValues.SpectrumFileName = String.Copy(fiFiles(0).Name)
				End If
				fiFiles(0).MoveTo(System.IO.Path.Combine(strMSInputFolderPath, mInputPropertyValues.SpectrumFileName))
			End If

			Dim strParamFilePath As String = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))

			If Not CreateInputPropertiesFile(strParamFilePath, strMSInputFolderPath, eMSalignVersion) Then
				Return False
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in InitializeMSInputFolder: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Parse the MSAlign console output file to determine the MSAlign version and to track the search progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output (v0.5 does not have console output)
		'
		' Initializing indexes...
		' Processing spectrum scan 660...         0% finished (0 minutes used).
		' Processing spectrum scan 1329...        1% finished (0 minutes used).
		' Processing spectrum scan 1649...        1% finished (0 minutes used).

        Static reExtractPercentFinished As New Text.RegularExpressions.Regex("(\d+)% finished", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)

        Dim oMatch As Text.RegularExpressions.Match

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

            Dim strLineIn As String
			Dim intLinesRead As Integer

			Dim intProgress As Int16
			Dim intActualProgress As Int16

			mConsoleOutputErrorMsg = String.Empty
			
            Using srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

                intLinesRead = 0
                Do While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()
                    intLinesRead += 1

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then
                        If intLinesRead = 1 Then
                            ' Parse out the MSAlign version
                            If strLineIn.ToLower.Contains("align") Then
                                If m_DebugLevel >= 2 AndAlso String.IsNullOrWhiteSpace(mMSAlignVersion) Then
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSAlign version: " & strLineIn)
                                End If

                                mMSAlignVersion = String.Copy(strLineIn)
                            Else
                                If strLineIn.ToLower.Contains("error") Then
                                    If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
                                        mConsoleOutputErrorMsg = "Error running MSAlign:"
                                    End If
                                    mConsoleOutputErrorMsg &= "; " & strLineIn
                                End If
                            End If
                        End If

                        ' Update progress if the line starts with Processing spectrum
                        If strLineIn.StartsWith("Processing spectrum") Then
                            oMatch = reExtractPercentFinished.Match(strLineIn)
                            If oMatch.Success Then
                                If Int16.TryParse(oMatch.Groups(1).Value, intProgress) Then
                                    intActualProgress = intProgress
                                End If
                            End If

                        ElseIf String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
                            If strLineIn.ToLower.StartsWith("error") Then
                                mConsoleOutputErrorMsg &= "; " & strLineIn
                            End If
                        End If
                    End If
                Loop

            End Using

            If m_progress < intActualProgress Then
                m_progress = intActualProgress
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
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(New System.IO.FileInfo(mMSAlignProgLoc))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
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

            Dim strLineIn As String
			Dim blnKeepLine As Boolean

			Dim intScanNumber As Integer
			Dim strMostRecentProgressLine As String = String.Empty
			Dim strMostRecentProgressLineWritten As String = String.Empty

			Dim intScanNumberOutputThreshold As Integer

			Dim strTrimmedFilePath As String
			strTrimmedFilePath = strConsoleOutputFilePath & ".trimmed"

            Using srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite)),
                  swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strTrimmedFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

                intScanNumberOutputThreshold = 0
                Do While Not srInFile.EndOfStream
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

            End Using

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

	Protected Function ValidateAndCopyResultFiles(ByVal eMSalignVersion As eMSAlignVersionType) As Boolean

		Dim strResultsFolderPath As String = System.IO.Path.Combine(mMSAlignWorkFolderPath, "msoutput")
		Dim lstResultsFilesToMove As System.Collections.Generic.List(Of String) = New System.Collections.Generic.List(Of String)
		Dim blnProcessingError As Boolean = False

		Try
			lstResultsFilesToMove.Add(System.IO.Path.Combine(strResultsFolderPath, mInputPropertyValues.ResultTableFileName))
			lstResultsFilesToMove.Add(System.IO.Path.Combine(strResultsFolderPath, mInputPropertyValues.ResultDetailsFileName))

			For Each strResultFilePath As String In lstResultsFilesToMove
				If Not System.IO.File.Exists(strResultFilePath) Then
					Dim Msg As String
					Msg = "MSAlign results file not found (" & System.IO.Path.GetFileName(strResultFilePath) & ")"

					If Not blnProcessingError Then
						' This is the first missing file; update the base-class comment
						m_message = clsGlobal.AppendToComment(m_message, Msg)
					End If

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & " (" & strResultFilePath & ")" & ", job " & m_JobNum)
					blnProcessingError = True
				Else
					' Copy the results file to the work directory
					Dim strSourceFileName As String = System.IO.Path.GetFileName(strResultFilePath)
					Dim strTargetFileName As String
					strTargetFileName = String.Copy(strSourceFileName)

					If eMSalignVersion = eMSAlignVersionType.v0pt5 Then
						' Rename the file when we copy it
						Select Case strSourceFileName
							Case RESULT_TABLE_NAME_LEGACY
								strTargetFileName = m_Dataset & RESULT_TABLE_NAME_SUFFIX
							Case RESULT_DETAILS_NAME_LEGACY
								strTargetFileName = m_Dataset & RESULT_DETAILS_NAME_SUFFIX
						End Select
					End If

					System.IO.File.Copy(strResultFilePath, System.IO.Path.Combine(m_WorkDir, strTargetFileName), True)

				End If
			Next

			' Zip the Html and XML folders
			ZipMSAlignResultFolder("html")
			ZipMSAlignResultFolder("XML")

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

	Protected Function ValidateResultTableFile(ByVal strSourceFilePath As String) As Boolean

        Dim strLineIn As String

		Try
			Dim blnValidFile As Boolean
			blnValidFile = False

			If Not System.IO.File.Exists(strSourceFilePath) Then
				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSAlign_ResultTable.txt file not found: " & strSourceFilePath)
				End If
				Return False
			End If

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Validating that the MSAlign_ResultTable.txt file is not empty")
			End If

			' Open the input file
            Using srInFile = New System.IO.StreamReader(New System.IO.FileStream(strSourceFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

                Do While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine

                    If Not String.IsNullOrEmpty(strLineIn) Then

                        Dim strSplitLine() As String
                        strSplitLine = strLineIn.Split(ControlChars.Tab)

                        If strSplitLine.Length > 1 Then
                            ' Look for an integer in the first or second column
                            ' Version 0.5 and 0.6 had Prsm_ID in the first column
                            ' Version 0.7 moved Prsm_ID to the second column
                            Dim intValue As Integer
                            If Integer.TryParse(strSplitLine(1), intValue) OrElse Integer.TryParse(strSplitLine(0), intValue) Then
                                ' Integer found; line is valid
                                blnValidFile = True
                                Exit Do
                            End If
                        End If
                    End If
                Loop

            End Using

            If Not blnValidFile Then
                Dim Msg As String
                Msg = "MSAlign_ResultTable.txt file is empty"
                m_message = clsGlobal.AppendToComment(m_message, Msg)

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)
                Return False
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ValidateResultTableFile: " & ex.Message)
            Return False
        End Try

		Return True

	End Function
	
	Protected Function ZipMSAlignResultFolder(ByVal strFolderName As String) As Boolean

		Dim objZipper As Ionic.Zip.ZipFile
		Dim strTargetFilePath As String
		Dim strSourceFolderPath As String

		Try
			strTargetFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_MSAlign_Results_" & strFolderName.ToUpper() & ".zip")
			strSourceFolderPath = System.IO.Path.Combine(mMSAlignWorkFolderPath, strFolderName)

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

		Static dtLastConsoleOutputParse As System.DateTime = System.DateTime.UtcNow

        UpdateStatusFile()

		If System.DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = System.DateTime.UtcNow

			ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT))

			If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSAlignVersion) Then
				mToolVersionWritten = StoreToolVersionInfo()
			End If

            LogProgress("MSAlign")

        End If

    End Sub

#End Region

End Class
