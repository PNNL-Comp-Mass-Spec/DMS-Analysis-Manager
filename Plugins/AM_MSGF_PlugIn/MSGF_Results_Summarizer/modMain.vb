Option Strict On

' This program parses MSGF synopsis file results to summarize the number of identified peptides and proteins
' It creates a text result file and posts the results to the DMS database
'
' -------------------------------------------------------------------------------
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Program started in February, 2012
'
' E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
' Website: http://ncrr.pnnl.gov/ or http://www.sysbio.org/resources/staff/
' -------------------------------------------------------------------------------
' 
Imports PHRPReader

Module modMain
	Public Const PROGRAM_DATE As String = "November 1, 2012"

	Private mMSGFSynFilePath As String = String.Empty
	Private mInputFolderPath As String = String.Empty
	Private mOutputFolderPath As String = String.Empty

	Private mDatasetName As String = String.Empty

	Private mJob As Integer = 0
	Private mSaveResultsAsText As Boolean = True
	Private mPostResultsToDb As Boolean = False

	Public Function Main() As Integer
		' Returns 0 if no error, error code if an error

		Dim intReturnCode As Integer
		Dim objParseCommandLine As New clsParseCommandLine
		Dim blnProceed As Boolean

		Dim blnSuccess As Boolean

		intReturnCode = 0

		Try
			blnProceed = False
			If objParseCommandLine.ParseCommandLine Then
				If SetOptionsUsingCommandLineParameters(objParseCommandLine) Then blnProceed = True
			End If

			If Not blnProceed OrElse _
			   objParseCommandLine.NeedToShowHelp Then
				ShowProgramHelp()
				intReturnCode = -1
			ElseIf (objParseCommandLine.ParameterCount + objParseCommandLine.NonSwitchParameterCount = 0) Then
				ShowProgramHelp()
				intReturnCode = -1
			ElseIf String.IsNullOrEmpty(mMSGFSynFilePath) AndAlso String.IsNullOrEmpty(mInputFolderPath) Then
				ShowErrorMessage("Must define either the MSGFSynFilePath or InputFolderPath")
				ShowProgramHelp()
				Return -1
			Else

				blnSuccess = SummarizeMSGFResults()

				If Not blnSuccess Then
					intReturnCode = -1
				End If

			End If

		Catch ex As Exception
			Console.WriteLine("Error occurred in modMain->Main: " & ControlChars.NewLine & ex.Message)
			intReturnCode = -1
		End Try

		Return intReturnCode

	End Function

	Private Function GetAppVersion() As String
		Return System.Reflection.Assembly.GetExecutingAssembly.GetName.Version.ToString & " (" & PROGRAM_DATE & ")"
	End Function

	Private Function SummarizeMSGFResults() As Boolean

		Dim dctFileSuffixes As System.Collections.Generic.Dictionary(Of String, clsPHRPReader.ePeptideHitResultType)
		Dim objEnum As System.Collections.Generic.Dictionary(Of String, clsPHRPReader.ePeptideHitResultType).Enumerator

		Dim objSummarizer As AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer

		Dim eResultType As clsPHRPReader.ePeptideHitResultType

		Dim fiSourceFile As System.IO.FileInfo

		Dim blnAppendToResultsFile As Boolean
		Dim blnSuccess As Boolean = False

		Try
			' Initialize a dictionary object that will be used to either find the appropriate input file, or determine the file type of the specified input file
			dctFileSuffixes = New System.Collections.Generic.Dictionary(Of String, clsPHRPReader.ePeptideHitResultType)

			dctFileSuffixes.Add("_xt_MSGF.txt", clsPHRPReader.ePeptideHitResultType.XTandem)
			dctFileSuffixes.Add("_msgfdb_syn_MSGF.txt", clsPHRPReader.ePeptideHitResultType.MSGFDB)
			dctFileSuffixes.Add("_inspect_syn_MSGF.txt", clsPHRPReader.ePeptideHitResultType.Inspect)
			dctFileSuffixes.Add("_syn_MSGF.txt", clsPHRPReader.ePeptideHitResultType.Sequest)

			eResultType = clsPHRPReader.ePeptideHitResultType.Unknown

			If String.IsNullOrWhiteSpace(mMSGFSynFilePath) Then
				If String.IsNullOrWhiteSpace(mInputFolderPath) Then
					ShowErrorMessage("Must define either the MSGFSynFilePath or InputFolderPath; unable to continue")
					Return False
				End If

				Dim diFolder As System.IO.DirectoryInfo = New System.IO.DirectoryInfo(mInputFolderPath)
				If Not diFolder.Exists Then
					ShowErrorMessage("Input folder not found: " & diFolder.FullName)
					Return False
				End If

				' Determine the input file path by looking for the expected files in mInputFolderPath
				Dim strSuffixesSearched As String = String.Empty

				objEnum = dctFileSuffixes.GetEnumerator()
				While objEnum.MoveNext
					Dim fiFiles() As System.IO.FileInfo
					fiFiles = diFolder.GetFiles("*" & objEnum.Current.Key)

					If fiFiles.Length > 0 Then
						' Match found
						mMSGFSynFilePath = fiFiles(0).FullName
						eResultType = objEnum.Current.Value
						Exit While
					End If

					If String.IsNullOrEmpty(strSuffixesSearched) Then
						strSuffixesSearched = objEnum.Current.Key
					Else
						strSuffixesSearched &= ", " & objEnum.Current.Key
					End If
				End While

				If eResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
					Dim strMsg As String = _
					  "Did not find any files in the source folder with the expected file name suffixes" & ControlChars.NewLine & _
					  "Looked for " & strSuffixesSearched & " in " & ControlChars.NewLine & _
					  diFolder.FullName

					ShowErrorMessage(strMsg)
					Return False
				End If

			Else
				' Determine the result type of mMSGFSynFilePath

				eResultType = clsPHRPReader.AutoDetermineResultType(mMSGFSynFilePath)

				If eResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
					objEnum = dctFileSuffixes.GetEnumerator()
					While objEnum.MoveNext
						If mMSGFSynFilePath.ToLower().EndsWith(objEnum.Current.Key.ToLower()) Then
							' Match found
							eResultType = objEnum.Current.Value
							Exit While
						End If
					End While
				End If

				If eResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
					ShowErrorMessage("Unable to determine result type from input file name: " & mMSGFSynFilePath)
					Return False
				End If
			End If


			fiSourceFile = New System.IO.FileInfo(mMSGFSynFilePath)
			If Not fiSourceFile.Exists Then
				ShowErrorMessage("Input file not found: " & fiSourceFile.FullName)
				Return False
			End If

			If String.IsNullOrWhiteSpace(mDatasetName) Then
				' Auto-determine the dataset name
				mDatasetName = clsPHRPReader.AutoDetermineDatasetName(fiSourceFile.Name, eResultType)

				If String.IsNullOrEmpty(mDatasetName) Then
					ShowErrorMessage("Unable to determine dataset name from input file name: " & mMSGFSynFilePath)
					Return False
				End If
			End If


			If mJob = 0 Then
				' Auto-determine the job number by looking for _Auto000000 in the parent folder name
				Dim intUnderscoreIndex As Integer
				Dim strNamePart As String

				intUnderscoreIndex = fiSourceFile.DirectoryName.LastIndexOf("_")

				If intUnderscoreIndex > 0 Then
					strNamePart = fiSourceFile.DirectoryName.Substring(intUnderscoreIndex + 1)
					If strNamePart.ToLower().StartsWith("auto") Then
						strNamePart = strNamePart.Substring(4)
						Integer.TryParse(strNamePart, mJob)
					End If
				End If

				If mJob = 0 Then
					Console.WriteLine()
					Console.WriteLine("Warning: unable to parse out the job number from " & fiSourceFile.DirectoryName)
					Console.WriteLine()
				End If
			End If

			objSummarizer = New AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer(eResultType, mDatasetName, mJob, fiSourceFile.Directory.FullName)
			objSummarizer.MSGFThreshold = AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer.DEFAULT_MSGF_THRESHOLD
			objSummarizer.FDRThreshold = AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer.DEFAULT_FDR_THRESHOLD

			objSummarizer.OutputFolderPath = mOutputFolderPath
			objSummarizer.PostJobPSMResultsToDB = mPostResultsToDb
			objSummarizer.SaveResultsToTextFile = mSaveResultsAsText

			blnAppendToResultsFile = False
			blnSuccess = objSummarizer.ProcessMSGFResults()

			If Not blnSuccess Then
				If Not String.IsNullOrWhiteSpace(objSummarizer.ErrorMessage) Then
					ShowErrorMessage("Processing failed: " & objSummarizer.ErrorMessage)
				Else
					ShowErrorMessage("Processing failed (unknown reason)")
				End If
			End If

			Console.WriteLine("MSGF Threshold: ".PadRight(25) & objSummarizer.MSGFThreshold.ToString("0.00E+00"))
			Console.WriteLine("FDR Threshold: ".PadRight(25) & objSummarizer.FDRThreshold.ToString("0.000"))
			Console.WriteLine("Spectra Searched: ".PadRight(25) & objSummarizer.SpectraSearched.ToString("#,##0"))
			Console.WriteLine()
			Console.WriteLine("Total PSMs (MSGF Filter): ".PadRight(32) & objSummarizer.TotalPSMsMSGF)
			Console.WriteLine("Unique Peptides (MSGF Filter): ".PadRight(32) & objSummarizer.UniquePeptideCountMSGF)
			Console.WriteLine("Unique Proteins (MSGF Filter): ".PadRight(32) & objSummarizer.UniqueProteinCountMSGF)

			Console.WriteLine("Total PSMs (FDR Filter): ".PadRight(32) & objSummarizer.TotalPSMsFDR)
			Console.WriteLine("Unique Peptides (FDR Filter): ".PadRight(32) & objSummarizer.UniquePeptideCountFDR)
			Console.WriteLine("Unique Proteins (FDR Filter): ".PadRight(32) & objSummarizer.UniqueProteinCountFDR)

			Console.WriteLine()

		Catch ex As Exception
			Console.WriteLine("Exception in SummarizeMSGFResults: " & ex.Message)
		End Try

		Return blnSuccess

	End Function

	Private Function SetOptionsUsingCommandLineParameters(ByVal objParseCommandLine As clsParseCommandLine) As Boolean
		' Returns True if no problems; otherwise, returns false

		Dim strValue As String = String.Empty
		Dim strValidParameters() As String = New String() {"I", "Folder", "Dataset", "Job", "O", "NoText", "DB"}

		Try
			' Make sure no invalid parameters are present
			If objParseCommandLine.InvalidParametersPresent(strValidParameters) Then
				Return False
			Else
				With objParseCommandLine
					' Query objParseCommandLine to see if various parameters are present
					If .RetrieveValueForParameter("I", strValue) Then
						mMSGFSynFilePath = strValue
					ElseIf .NonSwitchParameterCount > 0 Then
						mMSGFSynFilePath = .RetrieveNonSwitchParameter(0)
					End If

					If .RetrieveValueForParameter("Folder", strValue) Then mInputFolderPath = strValue

					If .RetrieveValueForParameter("Dataset", strValue) Then mDatasetName = strValue

					If .RetrieveValueForParameter("Job", strValue) Then
						If Not Integer.TryParse(strValue, mJob) Then
							ShowErrorMessage("Job number not numeric: " & strValue)
							Return False
						End If
					End If

					.RetrieveValueForParameter("O", mOutputFolderPath)

					If .RetrieveValueForParameter("NoText", strValue) Then
						mSaveResultsAsText = False
					End If

					If .RetrieveValueForParameter("DB", strValue) Then
						mPostResultsToDb = True
					End If

				End With

				Return True
			End If

		Catch ex As Exception
			Console.WriteLine("Error parsing the command line parameters: " & ControlChars.NewLine & ex.Message)
		End Try

		Return True

	End Function

	Private Sub ShowErrorMessage(ByVal strMessage As String)
		Dim strSeparator As String = "------------------------------------------------------------------------------"

		Console.WriteLine()
		Console.WriteLine(strSeparator)
		Console.WriteLine(strMessage)
		Console.WriteLine(strSeparator)
		Console.WriteLine()

	End Sub

	Private Sub ShowProgramHelp()

		Try

			Console.WriteLine("This program parses MSGF synopsis file results to summarize the number of identified peptides and proteins")
			Console.WriteLine("It creates a text result file and optionally posts the results to the DMS database")
			Console.WriteLine("Peptides are first filtered on MSGF_SpecProb < 1E-10")
			Console.WriteLine("They are next filtered on FDR < 1%")
			Console.WriteLine()
			Console.WriteLine("Program syntax #1:" & ControlChars.NewLine & System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location))
			Console.WriteLine(" [MSGFSynFilePath] [/Folder:InputFolderPath] [/Dataset:DatasetName] [/Job:JobNumber] [/O:OutputFolderPath] [/NoText] [/DB]")
			Console.WriteLine()
			Console.WriteLine("MSGFSynFilePath defines the data file to process, for example QC_Shew_11_06_pt5_c_21Feb12_Sphinx_11-08-09_syn_MSGF.txt")
			Console.WriteLine("The name of the source file will be auto-determined if the input folder is defined")
			Console.WriteLine()
			Console.WriteLine("Folder defines the input folder to process (and also to create the text result file in)")
			Console.WriteLine("Dataset defines the dataset name; if /Dataset is not used, then will auto-determine the dataset name")
			Console.WriteLine("Job defines the analysis job; if /Job is not provided, then will auto-determine the job number using the input folder name")
			Console.WriteLine("Use /O to define a custom output folder path")
			Console.WriteLine()
			Console.WriteLine("Use /NoText to specify that a text file not be created")
			Console.WriteLine("Use /DB to post results to DMS")
			Console.WriteLine()
			Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2012")
			Console.WriteLine("Version: " & GetAppVersion())
			Console.WriteLine()

			Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com")
			Console.WriteLine("Website: http://ncrr.pnnl.gov/ or http://www.sysbio.org/resources/staff/")
			Console.WriteLine()

			' Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
			System.Threading.Thread.Sleep(750)

		Catch ex As Exception
			Console.WriteLine("Error displaying the program syntax: " & ex.Message)
		End Try

	End Sub


End Module
