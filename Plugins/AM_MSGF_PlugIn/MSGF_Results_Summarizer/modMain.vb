Option Strict On

' This program parses MSGF synopsis file results to summarize the number of identified peptides and proteins
' It creates a text result file and posts the results to the DMS database
'
' -------------------------------------------------------------------------------
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Program started in February, 2012
'
' E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
' Website: http://omics.pnl.gov/ or http://www.sysbio.org/resources/staff/ or http://panomics.pnnl.gov/
' -------------------------------------------------------------------------------
' 
Imports PHRPReader
Imports System.IO

Module modMain
    Public Const PROGRAM_DATE As String = "February 17, 2016"

	Private mMSGFSynFilePath As String = String.Empty
	Private mInputFolderPath As String = String.Empty
	Private mOutputFolderPath As String = String.Empty

	Private mDatasetName As String = String.Empty
    Private mContactDatabase As Boolean = True

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

            If Not blnProceed OrElse objParseCommandLine.NeedToShowHelp Then
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
        Return Reflection.Assembly.GetExecutingAssembly.GetName.Version.ToString & " (" & PROGRAM_DATE & ")"
    End Function

    Private Function SummarizeMSGFResults() As Boolean

        Dim dctFileSuffixes As Dictionary(Of String, clsPHRPReader.ePeptideHitResultType)

        Dim objSummarizer As AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer

        Dim eResultType As clsPHRPReader.ePeptideHitResultType

        Dim fiSourceFile As FileInfo

        Dim blnSuccess = False

        Try
            ' Initialize a dictionary object that will be used to either find the appropriate input file, or determine the file type of the specified input file
            dctFileSuffixes = New Dictionary(Of String, clsPHRPReader.ePeptideHitResultType)

            dctFileSuffixes.Add("_xt_MSGF.txt", clsPHRPReader.ePeptideHitResultType.XTandem)
            dctFileSuffixes.Add("_msgfdb_syn_MSGF.txt", clsPHRPReader.ePeptideHitResultType.MSGFDB)
            dctFileSuffixes.Add("_inspect_syn_MSGF.txt", clsPHRPReader.ePeptideHitResultType.Inspect)
            dctFileSuffixes.Add("_syn_MSGF.txt", clsPHRPReader.ePeptideHitResultType.Sequest)
            dctFileSuffixes.Add("_msalign_syn.txt", clsPHRPReader.ePeptideHitResultType.MSAlign)
            dctFileSuffixes.Add("_mspath_syn.txt", clsPHRPReader.ePeptideHitResultType.MSPathFinder)

            eResultType = clsPHRPReader.ePeptideHitResultType.Unknown

            If String.IsNullOrWhiteSpace(mMSGFSynFilePath) Then
                If String.IsNullOrWhiteSpace(mInputFolderPath) Then
                    ShowErrorMessage("Must define either the MSGFSynFilePath or InputFolderPath; unable to continue")
                    Return False
                End If

                Dim diFolder As DirectoryInfo = New DirectoryInfo(mInputFolderPath)
                If Not diFolder.Exists Then
                    ShowErrorMessage("Input folder not found: " & diFolder.FullName)
                    Return False
                End If

                ' Determine the input file path by looking for the expected files in mInputFolderPath
                For Each suffixEntry In dctFileSuffixes
                    Dim fiFiles() As FileInfo
                    fiFiles = diFolder.GetFiles("*" & suffixEntry.Key)

                    If fiFiles.Length > 0 Then
                        ' Match found
                        mMSGFSynFilePath = fiFiles(0).FullName
                        eResultType = suffixEntry.Value
                        Exit For
                    End If
                Next

                Dim strSuffixesSearched = String.Join(", ", dctFileSuffixes.Keys.ToList())

                If eResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
                    Dim strMsg As String =
                      "Did not find any files in the source folder with the expected file name suffixes" & ControlChars.NewLine &
                      "Looked for " & strSuffixesSearched & " in " & ControlChars.NewLine &
                      diFolder.FullName

                    ShowErrorMessage(strMsg)
                    Return False
                End If

            Else
                ' Determine the result type of mMSGFSynFilePath

                eResultType = clsPHRPReader.AutoDetermineResultType(mMSGFSynFilePath)

                If eResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
                    For Each suffixEntry In dctFileSuffixes
                        If mMSGFSynFilePath.ToLower().EndsWith(suffixEntry.Key.ToLower()) Then
                            ' Match found
                            eResultType = suffixEntry.Value
                            Exit For
                        End If
                    Next
                End If

                If eResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
                    ShowErrorMessage("Unable to determine result type from input file name: " & mMSGFSynFilePath)
                    Return False
                End If
            End If


            fiSourceFile = New FileInfo(mMSGFSynFilePath)
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

                intUnderscoreIndex = fiSourceFile.DirectoryName.LastIndexOf("_", StringComparison.Ordinal)

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
            objSummarizer.EValueThreshold = AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer.DEFAULT_EVALUE_THRESHOLD
            objSummarizer.FDRThreshold = AnalysisManagerMSGFPlugin.clsMSGFResultsSummarizer.DEFAULT_FDR_THRESHOLD

            objSummarizer.OutputFolderPath = mOutputFolderPath
            objSummarizer.PostJobPSMResultsToDB = mPostResultsToDb
            objSummarizer.SaveResultsToTextFile = mSaveResultsAsText
            objSummarizer.DatasetName = mDatasetName
            objSummarizer.ContactDatabase = mContactDatabase

            blnSuccess = objSummarizer.ProcessMSGFResults()

            If Not blnSuccess Then
                If Not String.IsNullOrWhiteSpace(objSummarizer.ErrorMessage) Then
                    ShowErrorMessage("Processing failed: " & objSummarizer.ErrorMessage)
                Else
                    ShowErrorMessage("Processing failed (unknown reason)")
                End If
            End If

            Console.WriteLine("Result Type: ".PadRight(25) & objSummarizer.ResultTypeName)

            Dim strFilterText As String

            If objSummarizer.ResultType = clsPHRPReader.ePeptideHitResultType.MSAlign Then
                Console.WriteLine("EValue Threshold: ".PadRight(25) & objSummarizer.EValueThreshold.ToString("0.00E+00"))
                strFilterText = "EValue"
            Else
                Console.WriteLine("MSGF Threshold: ".PadRight(25) & objSummarizer.MSGFThreshold.ToString("0.00E+00"))
                strFilterText = "MSGF"
            End If

            Console.WriteLine("FDR Threshold: ".PadRight(25) & (objSummarizer.FDRThreshold * 100).ToString("0.0") & "%")
            Console.WriteLine("Spectra Searched: ".PadRight(25) & objSummarizer.SpectraSearched.ToString("#,##0"))
            Console.WriteLine()
            Console.WriteLine(("Total PSMs (" & strFilterText & " Filter): ").PadRight(35) & objSummarizer.TotalPSMsMSGF)
            Console.WriteLine(("Unique Peptides (" & strFilterText & " Filter): ").PadRight(35) & objSummarizer.UniquePeptideCountMSGF)
            Console.WriteLine(("Unique Proteins (" & strFilterText & " Filter): ").PadRight(35) & objSummarizer.UniqueProteinCountMSGF)

            Console.WriteLine("Total PSMs (FDR Filter): ".PadRight(35) & objSummarizer.TotalPSMsFDR)
            Console.WriteLine("Unique Peptides (FDR Filter): ".PadRight(35) & objSummarizer.UniquePeptideCountFDR)
            Console.WriteLine("Unique Proteins (FDR Filter): ".PadRight(35) & objSummarizer.UniqueProteinCountFDR)

            Console.WriteLine()

            Console.WriteLine("Percent MSn Scans No PSM: ".PadRight(38) & objSummarizer.PercentMSnScansNoPSM.ToString("0.0") & "%")
            Console.WriteLine("Maximum Scan Gap Adjacent MSn Scans: ".PadRight(38) & objSummarizer.MaximumScanGapAdjacentMSn)

            Console.WriteLine()

            Catch ex As Exception
            Console.WriteLine("Exception in SummarizeMSGFResults: " & ex.Message)
        End Try

        Return blnSuccess

    End Function

	Private Function SetOptionsUsingCommandLineParameters(ByVal objParseCommandLine As clsParseCommandLine) As Boolean
		' Returns True if no problems; otherwise, returns false

		Dim strValue As String = String.Empty
        Dim strValidParameters() As String = New String() {"I", "Folder", "Dataset", "Job", "O", "NoDatabase", "NoText", "DB"}

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

                    If .RetrieveValueForParameter("Folder", strValue) Then
                        mInputFolderPath = strValue
                    End If

                    If .RetrieveValueForParameter("Dataset", strValue) Then
                        mDatasetName = strValue
                    End If

					If .RetrieveValueForParameter("Job", strValue) Then
						If Not Integer.TryParse(strValue, mJob) Then
							ShowErrorMessage("Job number not numeric: " & strValue)
							Return False
						End If
					End If

					.RetrieveValueForParameter("O", mOutputFolderPath)

                    If .IsParameterPresent("NoDatabase") Then
                        mContactDatabase = False
                    End If

                    If .IsParameterPresent("NoText") Then
                        mSaveResultsAsText = False
                    End If

                    If .IsParameterPresent("DB") Then
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
		Const strSeparator As String = "------------------------------------------------------------------------------"

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
			Console.WriteLine("Program syntax:" & ControlChars.NewLine & Path.GetFileName(Reflection.Assembly.GetExecutingAssembly().Location))
			Console.WriteLine(" [MSGFSynFilePath] [/Folder:InputFolderPath] [/Dataset:DatasetName]")
			Console.WriteLine(" [/Job:JobNumber] [/O:OutputFolderPath] [/NoText] [/DB]")
			Console.WriteLine()
			Console.WriteLine("MSGFSynFilePath defines the data file to process, for example QC_Shew_11_06_pt5_c_21Feb12_Sphinx_11-08-09_syn_MSGF.txt")
			Console.WriteLine("The name of the source file will be auto-determined if the input folder is defined via /Folder")
			Console.WriteLine()
			Console.WriteLine("/Folder defines the input folder to process (and also to create the text result file in if /O is not used)")
			Console.WriteLine()
			Console.WriteLine("/Dataset defines the dataset name; if /Dataset is not used, then the name will be auto-determined")
			Console.WriteLine()
			Console.WriteLine("/Job defines the analysis job; if /Job is not provided, then will auto-determine the job number using the input folder name")
			Console.WriteLine()
			Console.WriteLine("Use /O to define a custom output folder path")
            Console.WriteLine()
            Console.WriteLine("Use /NoDatabase to indicate that DMS should not be contacted to lookup scan stats for the dataset")
            Console.WriteLine()
			Console.WriteLine("Use /NoText to specify that a text file not be created")
			Console.WriteLine("Use /DB to post results to DMS")
			Console.WriteLine()
			Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2012")
			Console.WriteLine("Version: " & GetAppVersion())
			Console.WriteLine()

			Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com")
			Console.WriteLine("Website: http://omics.pnl.gov/ or http://panomics.pnnl.gov/")
			Console.WriteLine()

			' Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
			Threading.Thread.Sleep(750)

		Catch ex As Exception
			Console.WriteLine("Error displaying the program syntax: " & ex.Message)
		End Try

	End Sub


End Module
