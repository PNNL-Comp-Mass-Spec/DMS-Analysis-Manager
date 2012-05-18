'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
' This class reads a tab-delimited text file (created by the Peptide File Extractor or by PHRP)
' and creates a tab-delimited text file suitable for processing by MSGF
' 
' The class must be derived by a sub-class customized for the specific analysis tool (Sequest, X!Tandem, Inspect, etc.)
'
'*********************************************************************************************************

Option Strict On

Public MustInherit Class clsMSGFInputCreator

#Region "Constants"
	Protected Const MSGF_INPUT_FILENAME_SUFFIX As String = "_MSGF_input.txt"
	Public Const MSGF_RESULT_FILENAME_SUFFIX As String = "_MSGF.txt"
#End Region

#Region "Module variables"
	Protected mDatasetName As String
	Protected mWorkDir As String
	Protected mPeptideHitResultType As PHRPReader.clsPHRPReader.ePeptideHitResultType

	Protected mSkippedLineInfo As System.Collections.Generic.SortedDictionary(Of Integer, System.Collections.Generic.List(Of String))

	Protected mDoNotFilterPeptides As Boolean

	' This dictionary is initially populated with a string constructed using
	' Scan plus "_" plus charge plus "_" plus the original peptide sequence in the PHRP file
	' It will contain an entry for every line written to the MSGF input file
	' It is later updated by AddUpdateMSGFResult() to store the properly formated MSGF result line for each entry
	' Finally, it will be used by CreateMSGFFirstHitsFile to create the MSGF file that corresponds to the first-hits file
	Protected mMSGFCachedResults As System.Collections.Generic.SortedDictionary(Of String, String)

	Protected mErrorMessage As String = String.Empty

	Protected mPHRPFirstHitsFilePath As String = String.Empty
	Protected mPHRPSynopsisFilePath As String = String.Empty

	Protected mMSGFInputFilePath As String = String.Empty
	Protected mMSGFResultsFilePath As String = String.Empty

	Protected mMSGFInputFileLineCount As Integer = 0

	' Note that this reader is instantiated and disposed of several times
	' We declare it here as a classwide variable so that we can attach the event handlers
	Protected WithEvents mPHRPReader As PHRPReader.clsPHRPReader

	Protected mLogFile As System.IO.StreamWriter

#End Region

#Region "Events"
	Public Event ErrorEvent(ByVal strErrorMessage As String)
	Public Event WarningEvent(ByVal strWarningMessage As String)
#End Region

#Region "Properties"

	Public Property DoNotFilterPeptides() As Boolean
		Get
			Return mDoNotFilterPeptides
		End Get
		Set(ByVal value As Boolean)
			mDoNotFilterPeptides = value
		End Set
	End Property

	Public ReadOnly Property ErrorMessage() As String
		Get
			Return mErrorMessage
		End Get
	End Property

	Public ReadOnly Property MSGFInputFileLineCount() As Integer
		Get
			Return mMSGFInputFileLineCount
		End Get
	End Property

	Public ReadOnly Property MSGFInputFilePath() As String
		Get
			Return mMSGFInputFilePath
		End Get
	End Property

	Public ReadOnly Property MSGFResultsFilePath() As String
		Get
			Return mMSGFResultsFilePath
		End Get
	End Property

	Public ReadOnly Property PHRPFirstHitsFilePath() As String
		Get
			Return mPHRPFirstHitsFilePath
		End Get
	End Property

	Public ReadOnly Property PHRPSynopsisFilePath() As String
		Get
			Return mPHRPSynopsisFilePath
		End Get
	End Property

#End Region

	''' <summary>
	''' constructor
	''' </summary>
	''' <param name="strDatasetName">Dataset Name</param>
	''' <param name="strWorkDir">Working directory</param>
	''' <param name="eResultType">PeptideHit result type</param>
	''' <remarks></remarks>
	Public Sub New(ByVal strDatasetName As String, ByVal strWorkDir As String, ByVal eResultType As PHRPReader.clsPHRPReader.ePeptideHitResultType)

		mDatasetName = strDatasetName
		mWorkDir = strWorkDir
		mPeptideHitResultType = eResultType

		mErrorMessage = String.Empty

		mSkippedLineInfo = New System.Collections.Generic.SortedDictionary(Of Integer, System.Collections.Generic.List(Of String))

		mMSGFCachedResults = New System.Collections.Generic.SortedDictionary(Of String, String)

		' Initialize the file paths
		InitializeFilePaths()

		UpdateMSGFInputOutputFilePaths()
	End Sub

#Region "Functions to be defined in derived classes"
	Protected MustOverride Sub InitializeFilePaths()
	Protected MustOverride Function PassesFilters(ByRef objPSM As PHRPReader.clsPSM) As Boolean
#End Region

	Public Sub AddUpdateMSGFResult(ByRef strScanNumber As String, _
	 ByRef strCharge As String, _
	 ByRef strPeptide As String, _
	 ByRef strMSGFResultData As String)

		Try
			mMSGFCachedResults.Item(ConstructMSGFResultCode(strScanNumber, strCharge, strPeptide)) = strMSGFResultData
		Catch ex As Exception
			' Entry not found; this is unexpected; we will only report the error at the console
			LogError("Entry not found in mMSGFCachedResults for " & ConstructMSGFResultCode(strScanNumber, strCharge, strPeptide))
		End Try

	End Sub

	Protected Function AppendText(ByVal strText As String, ByVal strAddnl As String) As String
		Return AppendText(strText, strAddnl, ": ")
	End Function

	Protected Function AppendText(ByVal strText As String, ByVal strAddnl As String, ByVal strDelimiter As String) As String
		If String.IsNullOrWhiteSpace(strAddnl) Then
			Return strText
		Else
			Return strText & strDelimiter & strAddnl
		End If
	End Function

	Public Sub CloseLogFileNow()
		If Not mLogFile Is Nothing Then
			mLogFile.Close()
			mLogFile = Nothing

			PRISM.Processes.clsProgRunner.GarbageCollectNow()
			System.Threading.Thread.Sleep(100)
		End If
	End Sub

	Protected Function CombineIfValidFile(strFolder As String, strFile As String) As String
		If Not String.IsNullOrWhiteSpace(strFile) Then
			Return System.IO.Path.Combine(strFolder, strFile)
		Else
			Return String.Empty
		End If
	End Function

	Protected Function ConstructMSGFResultCode(ByVal intScanNumber As Integer, _
	 ByVal intCharge As Integer, _
	 ByRef strPeptide As String) As String

		Return intScanNumber.ToString & "_" & intCharge.ToString & "_" & strPeptide

	End Function

	Protected Function ConstructMSGFResultCode(ByRef strScanNumber As String, _
	 ByRef strCharge As String, _
	 ByRef strPeptide As String) As String

		Return strScanNumber & "_" & strCharge & "_" & strPeptide

	End Function

	''' <summary>
	''' Read the first-hits file and create a new, parallel file with the MSGF results
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function CreateMSGFFirstHitsFile() As Boolean

		Const MAX_WARNINGS_TO_REPORT As Integer = 10

		Dim strMSGFFirstHitsResults As String
		Dim strPeptideResultCode As String

		Dim strMSGFResultData As String = String.Empty

		Dim intMissingValueCount As Integer
		Dim strWarningMessage As String

		Try

			If String.IsNullOrEmpty(mPHRPFirstHitsFilePath) Then
				' This result type does not have a first-hits file
				Return True
			End If

			' Open the first-hits file
			mPHRPReader = New PHRPReader.clsPHRPReader(mPHRPFirstHitsFilePath, mPeptideHitResultType, blnLoadModsAndSeqInfo:=True, blnLoadMSGFResults:=False)
			mPHRPReader.EchoMessagesToConsole = True

			If Not mPHRPReader.CanRead Then
				ReportError(AppendText("Aborting since PHRPReader is not ready", mPHRPReader.ErrorMessage))
				Return False
			End If

			' Define the path to write the first-hits MSGF results to
			strMSGFFirstHitsResults = System.IO.Path.GetFileNameWithoutExtension(mPHRPFirstHitsFilePath) & MSGF_RESULT_FILENAME_SUFFIX
			strMSGFFirstHitsResults = System.IO.Path.Combine(mWorkDir, strMSGFFirstHitsResults)

			' Create the output file
			Using swMSGFFHTFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strMSGFFirstHitsResults, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

				' Write out the headers to swMSGFFHTFile
				WriteMSGFResultsHeaders(swMSGFFHTFile)

				intMissingValueCount = 0

				Do While mPHRPReader.MoveNext()

					Dim objPSM As PHRPReader.clsPSM
					objPSM = mPHRPReader.CurrentPSM

					strPeptideResultCode = ConstructMSGFResultCode(objPSM.ScanNumber, objPSM.Charge, objPSM.Peptide)

					If mMSGFCachedResults.TryGetValue(strPeptideResultCode, strMSGFResultData) Then
						If String.IsNullOrEmpty(strMSGFResultData) Then
							' Match text is empty
							' We should not write thie out to disk since it would result in empty columns

							strWarningMessage = "MSGF Results are empty for result code '" & strPeptideResultCode & "'; this is unexpected"
							intMissingValueCount += 1
							If intMissingValueCount <= MAX_WARNINGS_TO_REPORT Then
								If intMissingValueCount = MAX_WARNINGS_TO_REPORT Then
									strWarningMessage &= "; additional invalid entries will not be reported"
								End If
								ReportWarning(strWarningMessage)
							Else
								LogError(strWarningMessage)
							End If
						Else
							' Match found; write out the result
							swMSGFFHTFile.WriteLine(objPSM.ResultID & ControlChars.Tab & strMSGFResultData)
						End If

					Else
						' Match not found; this is unexpected

						strWarningMessage = "Match not found for first-hits entry with result code '" & strPeptideResultCode & "'; this is unexpected"

						' Report the first 10 times this happens
						intMissingValueCount += 1
						If intMissingValueCount <= MAX_WARNINGS_TO_REPORT Then
							If intMissingValueCount = MAX_WARNINGS_TO_REPORT Then
								strWarningMessage &= "; additional missing entries will not be reported"
							End If
							ReportWarning(strWarningMessage)
						Else
							LogError(strWarningMessage)
						End If

					End If

				Loop


			End Using	 ' First Hits MSGF writer

			mPHRPReader.Dispose()

		Catch ex As Exception
			ReportError("Error creating the MSGF first hits file: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Creates the input file for MSGF
	''' Will contain filter passing peptides from the synopsis file, plus all peptides 
	''' in the first-hits file that are not filter passing in the synopsis file
	''' If the synopsis file does not exist, then simply processes the first-hits file
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function CreateMSGFInputFileUsingPHRPResultFiles() As Boolean

		Dim strMzXMLFileName As String = String.Empty
		Dim blnSuccess As Boolean = False

		Try
			If String.IsNullOrEmpty(mDatasetName) Then
				ReportError("Dataset name is undefined; unable to continue")
				Return False
			End If

			If String.IsNullOrEmpty(mWorkDir) Then
				ReportError("Working directory is undefined; unable to continue")
				Return False
			End If

			' mzXML filename is dataset plus .mzXML
			' Note that the jrap reader used by MSGF may fail if the .mzXML filename is capitalized differently than this (i.e., it cannot be .mzxml)
			strMzXMLFileName = mDatasetName & ".mzXML"

			' Create the MSGF Input file that we will write data to
			Using swMSGFInputFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(mMSGFInputFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

				' Write out the headers:  #SpectrumFile  Title  Scan#  Annotation  Charge  Protein_First  Result_ID  Data_Source
				' Note that we're storing the original peptide sequence in the "Title" column, while the marked up sequence (with mod masses) goes in the "Annotation" column
				swMSGFInputFile.WriteLine(clsMSGFRunner.MSGF_RESULT_COLUMN_SpectrumFile & ControlChars.Tab & _
				  clsMSGFRunner.MSGF_RESULT_COLUMN_Title & ControlChars.Tab & _
				  clsMSGFRunner.MSGF_RESULT_COLUMN_ScanNumber & ControlChars.Tab & _
				  clsMSGFRunner.MSGF_RESULT_COLUMN_Annotation & ControlChars.Tab & _
				  clsMSGFRunner.MSGF_RESULT_COLUMN_Charge & ControlChars.Tab & _
				  clsMSGFRunner.MSGF_RESULT_COLUMN_Protein_First & ControlChars.Tab & _
				  clsMSGFRunner.MSGF_RESULT_COLUMN_Result_ID & ControlChars.Tab & _
				  clsMSGFRunner.MSGF_RESULT_COLUMN_Data_Source & ControlChars.Tab & _
				  clsMSGFRunner.MSGF_RESULT_COLUMN_Collision_Mode)

				' Initialize some tracking variables
				mMSGFInputFileLineCount = 1

				mSkippedLineInfo.Clear()

				mMSGFCachedResults.Clear()

				If Not String.IsNullOrEmpty(mPHRPSynopsisFilePath) AndAlso System.IO.File.Exists(mPHRPSynopsisFilePath) Then
					' Read the synopsis file data
					mPHRPReader = New PHRPReader.clsPHRPReader(mPHRPSynopsisFilePath, mPeptideHitResultType, blnLoadModsAndSeqInfo:=True, blnLoadMSGFResults:=False)
					mPHRPReader.EchoMessagesToConsole = True

					' Report any errors cached during instantiation of mPHRPReader
					For Each strMessage As String In mPHRPReader.ErrorMessages
						ReportError(strMessage)
					Next
					mErrorMessage = String.Empty

					' Report any warnings cached during instantiation of mPHRPReader
					For Each strMessage As String In mPHRPReader.WarningMessages
						ReportWarning(strMessage)
					Next

					mPHRPReader.ClearErrors()
					mPHRPReader.ClearWarnings()

					If Not mPHRPReader.CanRead Then
						ReportError(AppendText("Aborting since PHRPReader is not ready", mPHRPReader.ErrorMessage))
						Return False
					End If

					ReadAndStorePHRPData(mPHRPReader, swMSGFInputFile, strMzXMLFileName, True)
					mPHRPReader.Dispose()

					blnSuccess = True
				End If

				If Not String.IsNullOrEmpty(mPHRPFirstHitsFilePath) AndAlso System.IO.File.Exists(mPHRPFirstHitsFilePath) Then
					' Now read the first-hits file data

					mPHRPReader = New PHRPReader.clsPHRPReader(mPHRPFirstHitsFilePath, mPeptideHitResultType, blnLoadModsAndSeqInfo:=True, blnLoadMSGFResults:=False)
					mPHRPReader.EchoMessagesToConsole = True

					If Not mPHRPReader.CanRead Then
						ReportError(AppendText("Aborting since PHRPReader is not ready", mPHRPReader.ErrorMessage))
						Return False
					End If

					ReadAndStorePHRPData(mPHRPReader, swMSGFInputFile, strMzXMLFileName, False)
					mPHRPReader.Dispose()

					blnSuccess = True
				End If


			End Using

			If Not blnSuccess Then
				ReportError("Neither the _syn.txt nor the _fht.txt file was found")
			End If

		Catch ex As Exception
			ReportError("Error reading the PHRP result file to create the MSGF Input file: " & ex.Message)
			Return False
		End Try

		Return blnSuccess

	End Function

	Public Function GetSkippedInfoByResultId(ByVal intResultID As Integer) As System.Collections.Generic.List(Of String)

		Dim objSkipList As System.Collections.Generic.List(Of String) = Nothing

		If mSkippedLineInfo.TryGetValue(intResultID, objSkipList) Then
			Return objSkipList
		Else
			Return New System.Collections.Generic.List(Of String)()
		End If

	End Function

	Protected Sub LogError(ByVal strErrorMessage As String)

		Try
			If mLogFile Is Nothing Then
				Dim strErrorLogFilePath As String
				Dim blnWriteHeader As Boolean = True

				strErrorLogFilePath = System.IO.Path.Combine(mWorkDir, "MSGFInputCreator_Log.txt")

				If System.IO.File.Exists(strErrorLogFilePath) Then
					blnWriteHeader = False
				End If

				mLogFile = New System.IO.StreamWriter(New System.IO.FileStream(strErrorLogFilePath, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.ReadWrite))
				mLogFile.AutoFlush = True

				If blnWriteHeader Then
					mLogFile.WriteLine("Date" & ControlChars.Tab & "Message")
				End If
			End If

			mLogFile.WriteLine(System.DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt") & ControlChars.Tab & strErrorMessage)

		Catch ex As Exception
			RaiseEvent ErrorEvent("Error writing to MSGFInputCreator log file: " & ex.Message)
		End Try

	End Sub

	''' <summary>
	''' Read data from a synopsis file or first hits file
	''' Write filter-passing synopsis file data to the MSGF input file
	''' Write first-hits data to the MSGF input file only if it isn't in mMSGFCachedResults
	''' </summary>
	''' <param name="objReader"></param>
	''' <param name="swMSGFInputFile"></param>
	''' <param name="strMzXMLFileName"></param>
	''' <param name="blnParsingSynopsisFile"></param>
	''' <remarks></remarks>
	Private Sub ReadAndStorePHRPData(ByRef objReader As PHRPReader.clsPHRPReader, _
	  ByRef swMSGFInputFile As System.IO.StreamWriter, _
	  ByVal strMzXMLFileName As String, _
	  ByVal blnParsingSynopsisFile As Boolean)

		Dim strPeptideResultCode As String
		Dim strPHRPSource As String

		Dim blnSuccess As Boolean

		Dim intResultIDPrevious As Integer = 0
		Dim intScanNumberPrevious As Integer = 0
		Dim intChargePrevious As Integer = 0
		Dim strPeptidePrevious As String = String.Empty

		Dim blnPassesFilters As Boolean

		Dim objSkipList As System.Collections.Generic.List(Of String) = Nothing

		If blnParsingSynopsisFile Then
			strPHRPSource = clsMSGFRunner.MSGF_PHRP_DATA_SOURCE_SYN
		Else
			strPHRPSource = clsMSGFRunner.MSGF_PHRP_DATA_SOURCE_FHT
		End If

		objReader.SkipDuplicatePSMs = False

		Do While objReader.MoveNext()

			blnSuccess = True

			Dim objPSM As PHRPReader.clsPSM
			objPSM = objReader.CurrentPSM

			' Compute the result code; we'll use it later to search/populate mMSGFCachedResults
			strPeptideResultCode = ConstructMSGFResultCode(objPSM.ScanNumber, objPSM.Charge, objPSM.Peptide)

			If mDoNotFilterPeptides Then
				blnPassesFilters = True
			Else
				blnPassesFilters = PassesFilters(objPSM)
			End If

			If blnParsingSynopsisFile Then
				' Synopsis file 
				' Check for duplicate lines

				If blnPassesFilters Then
					' If this line is a duplicate of the previous line, then skip it
					' This happens in Sequest _syn.txt files where the line is repeated for all protein matches


					If intScanNumberPrevious = objPSM.ScanNumber AndAlso _
					   intChargePrevious = objPSM.Charge AndAlso _
					   strPeptidePrevious = objPSM.Peptide Then

						blnSuccess = False

						If mSkippedLineInfo.TryGetValue(intResultIDPrevious, objSkipList) Then
							objSkipList.Add(objPSM.ResultID & ControlChars.Tab & objPSM.ProteinFirst)
						Else
							objSkipList = New System.Collections.Generic.List(Of String)
							objSkipList.Add(objPSM.ResultID & ControlChars.Tab & objPSM.ProteinFirst)
							mSkippedLineInfo.Add(intResultIDPrevious, objSkipList)
						End If

					Else
						intResultIDPrevious = objPSM.ResultID
						intScanNumberPrevious = objPSM.ScanNumber
						intChargePrevious = objPSM.Charge
						strPeptidePrevious = String.Copy(objPSM.Peptide)
					End If

				End If

			Else
				' First-hits file
				' Use all data in the first-hits file, but skip it if it is already in mMSGFCachedResults

				blnPassesFilters = True

				If mMSGFCachedResults.ContainsKey(strPeptideResultCode) Then
					blnSuccess = False
				End If

			End If

			If blnSuccess And blnPassesFilters Then

				' The title column holds the original peptide sequence
				' If a peptide doesn't have any mods, then the Title column and the Annotation column will be identical

				swMSGFInputFile.WriteLine( _
				   strMzXMLFileName & ControlChars.Tab & _
				   objPSM.Peptide & ControlChars.Tab & _
				   objPSM.ScanNumber & ControlChars.Tab & _
				   objPSM.PeptideWithNumericMods & ControlChars.Tab & _
				   objPSM.Charge & ControlChars.Tab & _
				   objPSM.ProteinFirst & ControlChars.Tab & _
				   objPSM.ResultID & ControlChars.Tab & _
				   strPHRPSource & ControlChars.Tab & _
				   objPSM.CollisionMode)

				mMSGFInputFileLineCount += 1

				Try
					mMSGFCachedResults.Add(strPeptideResultCode, "")
				Catch ex As Exception
					' Key is already present; this is unexpected, but we can safely ignore this error
					LogError("Warning in ReadAndStorePHRPData: Key already defined in mMSGFCachedResults: " & strPeptideResultCode)
				End Try

			End If


		Loop


	End Sub

	Protected Sub ReportError(ByVal strErrorMessage As String)
		mErrorMessage = strErrorMessage
		LogError(mErrorMessage)
		RaiseEvent ErrorEvent(mErrorMessage)
	End Sub

	Protected Sub ReportWarning(ByVal strWarningMessage As String)
		LogError(strWarningMessage)
		RaiseEvent WarningEvent(strWarningMessage)
	End Sub


	''' <summary>
	''' Define the MSGF input and output file paths
	''' </summary>
	''' <remarks>This sub should be called after updating mPHRPResultFilePath</remarks>
	Protected Sub UpdateMSGFInputOutputFilePaths()
		mMSGFInputFilePath = System.IO.Path.Combine(mWorkDir, System.IO.Path.GetFileNameWithoutExtension(mPHRPSynopsisFilePath) & MSGF_INPUT_FILENAME_SUFFIX)
		mMSGFResultsFilePath = System.IO.Path.Combine(mWorkDir, System.IO.Path.GetFileNameWithoutExtension(mPHRPSynopsisFilePath) & MSGF_RESULT_FILENAME_SUFFIX)
	End Sub

	Public Sub WriteMSGFResultsHeaders(ByRef swOutFile As System.IO.StreamWriter)

		swOutFile.WriteLine("Result_ID" & ControlChars.Tab & _
		  "Scan" & ControlChars.Tab & _
		  "Charge" & ControlChars.Tab & _
		  "Protein" & ControlChars.Tab & _
		  "Peptide" & ControlChars.Tab & _
		  "SpecProb" & ControlChars.Tab & _
		  "Notes")
	End Sub

	Private Sub mPHRPReader_ErrorEvent(strErrorMessage As String) Handles mPHRPReader.ErrorEvent
		ReportError(strErrorMessage)
	End Sub

	Private Sub mPHRPReader_MessageEvent(strMessage As String) Handles mPHRPReader.MessageEvent
		Console.WriteLine(strMessage)
	End Sub

	Private Sub mPHRPReader_WarningEvent(strWarningMessage As String) Handles mPHRPReader.WarningEvent
		ReportWarning(strWarningMessage)
	End Sub
End Class
