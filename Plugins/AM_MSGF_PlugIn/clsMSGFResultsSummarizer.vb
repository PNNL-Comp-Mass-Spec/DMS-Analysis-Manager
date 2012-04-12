'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 02/14/2012
'
' This class reads an MSGF results file and accompanying peptide/protein map file
'  to count the number of peptides passing a given MSGF threshold
' Reports PSM count, unique peptide count, and unique protein count
' 
'*********************************************************************************************************

Option Strict On

Imports PHRPReader

Public Class clsMSGFResultsSummarizer

#Region "Constants and Enums"
	Public Const DEFAULT_MSGF_THRESHOLD As Double = 0.0000000001	' 1E-10

	Public Property DEFAULT_CONNECTION_STRING As String = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;"

	Protected Const STORE_JOB_PSM_RESULTS_SP_NAME As String = "StoreJobPSMStats"

#End Region

    Private mErrorMessage As String = String.Empty

    Private mMSGFThreshold As Double = DEFAULT_MSGF_THRESHOLD
	Private mPostJobPSMResultsToDB As Boolean = False

	Private mSaveResultsToTextFile As Boolean = True
	Private mOutputFolderPath As String = String.Empty

	Private mSpectraSearched As Integer = 0
	Private mTotalPSMs As Integer = 0
	Private mUniquePeptideCount As Integer = 0
	Private mUniqueProteinCount As Integer = 0

	Private mResultType As clsPHRPReader.ePeptideHitResultType
	Private mDatasetName As String
	Private mJob As Integer
	Private mWorkDir As String
	Private mConnectionString As String

	' The following is auto-determined in ProcessMSGFResults
	Private mMSGFSynopsisFileName As String = String.Empty

#Region "Properties"
	Public ReadOnly Property ErrorMessage As String
		Get
			If String.IsNullOrEmpty(mErrorMessage) Then
				Return String.Empty
			Else
				Return mErrorMessage
			End If
		End Get
	End Property

	Public Property MSGFThreshold As Double
		Get
			Return mMSGFThreshold
		End Get
		Set(value As Double)
			mMSGFThreshold = value
		End Set
	End Property

	Public ReadOnly Property MSGFSynopsisFileName As String
		Get
			If String.IsNullOrEmpty(mMSGFSynopsisFileName) Then
				Return String.Empty
			Else
				Return mMSGFSynopsisFileName
			End If
		End Get
	End Property

	Public Property OutputFolderPath() As String
		Get
			Return mOutputFolderPath
		End Get
		Set(value As String)
			mOutputFolderPath = value
		End Set
	End Property

	Public Property PostJobPSMResultsToDB() As Boolean
		Get
			Return mPostJobPSMResultsToDB
		End Get
		Set(value As Boolean)
			mPostJobPSMResultsToDB = value
		End Set
	End Property

	Public ReadOnly Property SpectraSearched As Integer
		Get
			Return mSpectraSearched
		End Get
	End Property

	Public ReadOnly Property TotalPSMs As Integer
		Get
			Return mTotalPSMs
		End Get
	End Property

	Public Property SaveResultsToTextFile() As Boolean
		Get
			Return mSaveResultsToTextFile
		End Get
		Set(value As Boolean)
			mSaveResultsToTextFile = value
		End Set
	End Property

	Public ReadOnly Property UniquePeptideCount As Integer
		Get
			Return mUniquePeptideCount
		End Get
	End Property

	Public ReadOnly Property UniqueProteinCount As Integer
		Get
			Return mUniqueProteinCount
		End Get
	End Property
#End Region

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="eResultType">Peptide Hit result type</param>
	''' <param name="strDatasetName">Dataset name</param>
	''' <param name="intJob">Job number</param>
	''' <param name="strSourceFolderPath">Source folder path</param>
	''' <remarks></remarks>
	Public Sub New(ByVal eResultType As clsPHRPReader.ePeptideHitResultType, ByVal strDatasetName As String, ByVal intJob As Integer, ByVal strSourceFolderPath As String)
		mResultType = eResultType
		mDatasetName = strDatasetName
		mJob = intJob
		mWorkDir = strSourceFolderPath
		mConnectionString = DEFAULT_CONNECTION_STRING
	End Sub

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="eResultType">Peptide Hit result type</param>
	''' <param name="strDatasetName">Dataset name</param>
	''' <param name="intJob">Job number</param>
	''' <param name="strSourceFolderPath">Source folder path</param>
	''' <param name="strConnectionString">DMS connection string</param>
	''' <remarks></remarks>
	Public Sub New(ByVal eResultType As clsPHRPReader.ePeptideHitResultType, ByVal strDatasetName As String, ByVal intJob As Integer, ByVal strSourceFolderPath As String, ByVal strConnectionString As String)
		mResultType = eResultType
		mDatasetName = strDatasetName
		mJob = intJob
		mWorkDir = strSourceFolderPath
		mConnectionString = strConnectionString
	End Sub

	Protected Function PostJobPSMResults() As Boolean
		Return PostJobPSMResults(mJob, mConnectionString, STORE_JOB_PSM_RESULTS_SP_NAME)
	End Function

	Protected Function ExamineFirstHitsFile(ByVal strFirstHitsFilePath As String) As Boolean

		Dim lstUniqueSpectra As System.Collections.Generic.Dictionary(Of String, Integer)
		Dim strScanChargeCombo As String

		Dim blnSuccess As Boolean = False

		Try

			' Initialize the list that will be used to track the number of spectra searched
			lstUniqueSpectra = New System.Collections.Generic.Dictionary(Of String, Integer)

			Using objReader As clsPHRPReader = New clsPHRPReader(strFirstHitsFilePath, blnLoadModDefs:=False, blnLoadMSGFResults:=False)
				While objReader.MoveNext()

					Dim objPSM As PHRPReader.clsPSM
					objPSM = objReader.CurrentPSM

					If objPSM.Charge >= 0 Then
						strScanChargeCombo = objPSM.ScanNumber.ToString() & "_" & objPSM.Charge.ToString()

						If Not lstUniqueSpectra.ContainsKey(strScanChargeCombo) Then
							lstUniqueSpectra.Add(strScanChargeCombo, 0)
						End If

					End If

				End While
			End Using

			mSpectraSearched = lstUniqueSpectra.Count

			blnSuccess = True

		Catch ex As Exception
			SetErrorMessage(ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Protected Function PostJobPSMResults(ByVal intJob As Integer, _
	 ByVal strConnectionString As String, _
	 ByVal strStoredProcedure As String) As Boolean

		Const MAX_RETRY_COUNT As Integer = 3

		Dim objCommand As System.Data.SqlClient.SqlCommand

		Dim blnSuccess As Boolean

		Try

			' Call stored procedure strStoredProcedure using connection string strConnectionString

			If String.IsNullOrWhiteSpace(strConnectionString) Then
				SetErrorMessage("Connection string empty in PostJobPSMResults")
				Return False
			End If

			If String.IsNullOrWhiteSpace(strStoredProcedure) Then
				strStoredProcedure = STORE_JOB_PSM_RESULTS_SP_NAME
			End If

			objCommand = New System.Data.SqlClient.SqlCommand()

			With objCommand
				.CommandType = CommandType.StoredProcedure
				.CommandText = strStoredProcedure

				.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
				.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

				.Parameters.Add(New SqlClient.SqlParameter("@Job", SqlDbType.Int))
				.Parameters.Item("@Job").Direction = ParameterDirection.Input
				.Parameters.Item("@Job").Value = intJob

				.Parameters.Add(New SqlClient.SqlParameter("@MSGFThreshold", SqlDbType.Float))
				.Parameters.Item("@MSGFThreshold").Direction = ParameterDirection.Input
				.Parameters.Item("@MSGFThreshold").Value = mMSGFThreshold

				.Parameters.Add(New SqlClient.SqlParameter("@SpectraSearched", SqlDbType.Int))
				.Parameters.Item("@SpectraSearched").Direction = ParameterDirection.Input
				.Parameters.Item("@SpectraSearched").Value = mSpectraSearched

				.Parameters.Add(New SqlClient.SqlParameter("@TotalPSMs", SqlDbType.Int))
				.Parameters.Item("@TotalPSMs").Direction = ParameterDirection.Input
				.Parameters.Item("@TotalPSMs").Value = mTotalPSMs

				.Parameters.Add(New SqlClient.SqlParameter("@UniquePeptides", SqlDbType.Int))
				.Parameters.Item("@UniquePeptides").Direction = ParameterDirection.Input
				.Parameters.Item("@UniquePeptides").Value = mUniquePeptideCount

				.Parameters.Add(New SqlClient.SqlParameter("@UniqueProteins", SqlDbType.Int))
				.Parameters.Item("@UniqueProteins").Direction = ParameterDirection.Input
				.Parameters.Item("@UniqueProteins").Value = mUniqueProteinCount

			End With

			'Execute the SP (retry the call up to 3 times)
			Dim ResCode As Integer
			Dim strErrorMessage As String = String.Empty
			ResCode = AnalysisManagerBase.clsGlobal.ExecuteSP(objCommand, strConnectionString, MAX_RETRY_COUNT, strErrorMessage)

			If ResCode = 0 Then
				blnSuccess = True
			Else
				SetErrorMessage("Error storing PSM Results in database, " & strStoredProcedure & " returned " & ResCode.ToString)
				If Not String.IsNullOrEmpty(strErrorMessage) Then
					mErrorMessage &= "; " & strErrorMessage
				End If
				blnSuccess = False
			End If

		Catch ex As System.Exception
			SetErrorMessage("Exception storing PSM Results in database: " & ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Public Function ProcessMSGFResults() As Boolean

		Dim strPHRPFirstHitsFileName As String
		Dim strPHRPFirstHitsFilePath As String

		Dim strPHRPSynopsisFileName As String
		Dim strPHRPSynopsisFilePath As String

		Dim blnSuccess As Boolean

		' The keys in this dictionary are Result_ID values
		' The values are the mapped protein name
		' We'll deal with multiple proteins for each peptide later when we parse the _ResultToSeqMap.txt and _SeqToProteinMap.txt files
		' If those files are not found, then we'll simply use the protein information stored in lstPSMs
		Dim lstPSMs As System.Collections.Generic.Dictionary(Of Integer, String)

		Try
			mErrorMessage = String.Empty
			mSpectraSearched = 0
			mTotalPSMs = 0
			mUniquePeptideCount = 0
			mUniqueProteinCount = 0

			'''''''''''''''''''''
			' Define the file paths
			'
			' We use the First-hits file to determine the number of MS/MS spectra that were searched (unique combo of charge and scan number)
			strPHRPFirstHitsFileName = clsPHRPReader.GetPHRPFirstHitsFileName(mResultType, mDatasetName)

			' We use the Synopsis file to count the number of peptides and proteins observed
			strPHRPSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(mResultType, mDatasetName)

			If mResultType = clsPHRPReader.ePeptideHitResultType.XTandem Then
				' X!Tandem results don't have first-hits files; use the Synopsis file instead to determine scan counts
				strPHRPFirstHitsFileName = strPHRPSynopsisFileName
			End If

			mMSGFSynopsisFileName = System.IO.Path.GetFileNameWithoutExtension(strPHRPSynopsisFileName) & clsMSGFInputCreator.MSGF_RESULT_FILENAME_SUFFIX

			strPHRPFirstHitsFilePath = System.IO.Path.Combine(mWorkDir, strPHRPFirstHitsFileName)
			strPHRPSynopsisFilePath = System.IO.Path.Combine(mWorkDir, strPHRPSynopsisFileName)

			If Not System.IO.File.Exists(strPHRPSynopsisFilePath) Then
				SetErrorMessage("File not found: " & strPHRPSynopsisFilePath)
				Return False
			End If

			'''''''''''''''''''''
			' Determine the number of MS/MS spectra searched
			'
			If System.IO.File.Exists(strPHRPFirstHitsFilePath) Then
				ExamineFirstHitsFile(strPHRPFirstHitsFilePath)
			End If

			'''''''''''''''''''''
			' Load the PSMs
			'
			lstPSMs = New System.Collections.Generic.Dictionary(Of Integer, String)
			blnSuccess = LoadPSMs(strPHRPSynopsisFilePath, lstPSMs)

			If blnSuccess Then
				Dim objReader As PHRPReader.clsPHRPSeqMapReader
				Dim objResultToSeqMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of Integer)) = Nothing
				Dim objSeqToProteinMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of String)) = Nothing

				' Load the protein information
				objReader = New PHRPReader.clsPHRPSeqMapReader(mDatasetName, mWorkDir, mResultType)
				blnSuccess = objReader.GetProteinMapping(objResultToSeqMap, objSeqToProteinMap)

				If blnSuccess Then
					' Summarize the results, counting the number of peptides, unique peptides, and proteins
					blnSuccess = SummarizeResults(lstPSMs, objResultToSeqMap, objSeqToProteinMap)
				End If

				If blnSuccess Then
					If mSaveResultsToTextFile Then
						' Note: Continue processing even if this step fails
						SaveResultsToFile()
					End If

					If mPostJobPSMResultsToDB Then
						blnSuccess = PostJobPSMResults()
					End If

				End If
			End If

			blnSuccess = True

		Catch ex As Exception
			SetErrorMessage(ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Protected Function LoadPSMs(ByVal strPHRPSynopsisFilePath As String, ByRef lstPSMs As System.Collections.Generic.Dictionary(Of Integer, String)) As Boolean

		Dim dblSpecProb As Double
		Dim blnSuccess As Boolean = False

		Try

			Using objPHRPReader As New PHRPReader.clsPHRPReader(strPHRPSynopsisFilePath, blnLoadModDefs:=False, blnLoadMSGFResults:=True)

				Do While objPHRPReader.MoveNext()

					Dim objPSM As PHRPReader.clsPSM
					objPSM = objPHRPReader.CurrentPSM

					If Double.TryParse(objPSM.MSGFSpecProb, dblSpecProb) Then

						If dblSpecProb <= mMSGFThreshold Then

							' Store in lstPSMs
							If Not lstPSMs.ContainsKey(objPSM.ResultID) Then
								lstPSMs.Add(objPSM.ResultID, objPSM.ProteinFirst)
							End If

						End If

					End If
				Loop			

			End Using

			blnSuccess = True

		Catch ex As Exception
			SetErrorMessage(ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Protected Function SaveResultsToFile() As Boolean

		Dim strOutputFilePath As String = "??"

		Try
			If Not String.IsNullOrEmpty(mOutputFolderPath) Then
				strOutputFilePath = mOutputFolderPath
			Else
				strOutputFilePath = mWorkDir
			End If
			strOutputFilePath = System.IO.Path.Combine(strOutputFilePath, mDatasetName & "_PSM_Stats.txt")

			Using swOutFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strOutputFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

				' Header line
				swOutFile.WriteLine( _
				  "Dataset" & ControlChars.Tab & _
				  "Job" & ControlChars.Tab & _
				  "MSGF_Threshold" & ControlChars.Tab & _
				  "Spectra_Searched" & ControlChars.Tab & _
				  "Total_PSMs" & ControlChars.Tab & _
				  "Unique_Peptides" & ControlChars.Tab & _
				  "Unique_Proteins")

				' Stats
				swOutFile.WriteLine( _
				 mDatasetName & ControlChars.Tab & _
				 mJob & ControlChars.Tab & _
				 mMSGFThreshold.ToString("0.00E+00") & ControlChars.Tab & _
				 mSpectraSearched & ControlChars.Tab & _
				 mTotalPSMs & ControlChars.Tab & _
				 mUniquePeptideCount & ControlChars.Tab & _
				 mUniqueProteinCount)

			End Using

		Catch ex As Exception
			SetErrorMessage("Exception saving results to " & strOutputFilePath & ": " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Private Sub SetErrorMessage(ByVal strMessage As String)
		Console.WriteLine(strMessage)
		mErrorMessage = strMessage
	End Sub

	''' <summary>
	''' Summarize the results by inter-relating lstPSMs, objResultToSeqMap, and objSeqToProteinMap
	''' </summary>
	''' <param name="lstPSMs"></param>
	''' <param name="objResultToSeqMap"></param>
	''' <param name="objSeqToProteinMap"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function SummarizeResults( _
	 ByRef lstPSMs As System.Collections.Generic.Dictionary(Of Integer, String), _
	 ByRef objResultToSeqMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of Integer)), _
	 ByRef objSeqToProteinMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of String))) As Boolean

		' lstPSMs only contains the filter-passing results (keys are ResultID, values are the first protein for each ResultID)
		' Link up with objResultToSeqMap to determine the unique number of filter-passing peptides
		' Link up with objSeqToProteinMap to determine the unique number of proteins

		' The Keys in this dictionary are SeqID values; the values are observation count
		Dim lstUniqueSequences As System.Collections.Generic.Dictionary(Of Integer, Integer)

		' The Keys in this dictionary are protein names; the values are observation count
		Dim lstUniqueProteins As System.Collections.Generic.Dictionary(Of String, Integer)

		Dim intObsCount As Integer
		Dim lstSeqIDs As System.Collections.Generic.List(Of Integer) = Nothing
		Dim lstProteins As System.Collections.Generic.List(Of String) = Nothing

		Try
			lstUniqueSequences = New System.Collections.Generic.Dictionary(Of Integer, Integer)
			lstUniqueProteins = New System.Collections.Generic.Dictionary(Of String, Integer)

			For Each objResultID As System.Collections.Generic.KeyValuePair(Of Integer, String) In lstPSMs

				If objResultToSeqMap.TryGetValue(objResultID.Key, lstSeqIDs) Then
					' Result found in _ResultToSeqMap.txt file

					For Each intSeqID As Integer In lstSeqIDs

						If lstUniqueSequences.TryGetValue(intSeqID, intObsCount) Then
							lstUniqueSequences(intSeqID) = intObsCount + 1
						Else
							lstUniqueSequences.Add(intSeqID, 1)
						End If

						' Lookup the proteins for this peptide
						If objSeqToProteinMap.TryGetValue(intSeqID, lstProteins) Then
							' Update the observation count for each protein

							For Each strProtein As String In lstProteins

								If lstUniqueProteins.TryGetValue(strProtein, intObsCount) Then
									lstUniqueProteins(strProtein) = intObsCount + 1
								Else
									lstUniqueProteins.Add(strProtein, 1)
								End If

							Next

						End If

					Next

				End If
			Next

			' Store the stats
			mTotalPSMs = lstPSMs.Count
			mUniquePeptideCount = lstUniqueSequences.Count
			mUniqueProteinCount = lstUniqueProteins.Count

		Catch ex As Exception
			SetErrorMessage("Exception summarizing results: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

End Class
