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

Public Class clsMSGFResultsSummarizer

#Region "Constants and Enums"
	Public Const DEFAULT_MSGF_THRESHOLD As Double = 0.0000000001	' 1E-10

    Public Const MSGF_RESULT_COLUMN_Result_ID As String = "Result_ID"
    Public Const MSGF_RESULT_COLUMN_Scan As String = "Scan"
    Public Const MSGF_RESULT_COLUMN_Charge As String = "Charge"
    Public Const MSGF_RESULT_COLUMN_Protein As String = "Protein"
    Public Const MSGF_RESULT_COLUMN_Peptide As String = "Peptide"
    Public Const MSGF_RESULT_COLUMN_SpecProb As String = "SpecProb"
    Public Const MSGF_RESULT_COLUMN_Notes As String = "Notes"

	Public Const SEQ_PROT_MAP_COLUMN_Unique_Seq_ID As String = "Unique_Seq_ID"
	Public Const SEQ_PROT_MAP_COLUMN_Cleavage_State As String = "Cleavage_State"
	Public Const SEQ_PROT_MAP_COLUMN_Terminus_State As String = "Terminus_State"
	Public Const SEQ_PROT_MAP_COLUMN_Protein_Name As String = "Protein_Name"
	Public Const SEQ_PROT_MAP_COLUMN_Protein_EValue As String = "Protein_Expectation_Value_Log(e)"
	Public Const SEQ_PROT_MAP_COLUMN_Protein_Intensity As String = "Protein_Intensity_Log(I)"

	Protected Const FHT_COLUMN_Result_ID As String = "ResultID"
	Protected Const FHT_COLUMN_Scan As String = "Scan"
	Protected Const FHT_COLUMN_Charge As String = "Charge"

	Protected Const FHT_COLUMN_Result_ID_Alt1 As String = "HitNum"		' Used by Sequest
	Protected Const FHT_COLUMN_Result_ID_Alt2 As String = "Result_ID"	' Used by X!Tandem
	Protected Const FHT_COLUMN_Scan_Alt As String = "ScanNum"			' Used by Sequest
	Protected Const FHT_COLUMN_Charge_Alt As String = "ChargeState"		' Used by Sequest

	Public Property DEFAULT_CONNECTION_STRING As String = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;"

	Protected Const STORE_JOB_PSM_RESULTS_SP_NAME As String = "StoreJobPSMStats"

#End Region

    Private mErrorMessage As String = String.Empty

    Private mMSGFThreshold As Double = DEFAULT_MSGF_THRESHOLD
	Private mPostJobPSMResultsToDB As Boolean = False
	Private mSaveResultsToTextFile As Boolean = True

	Private mSpectraSearched As Integer = 0
	Private mTotalPSMs As Integer = 0
	Private mUniquePeptideCount As Integer = 0
	Private mUniqueProteinCount As Integer = 0

	Private mResultType As clsMSGFRunner.ePeptideHitResultType
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

	Public Sub New(ByVal eResultType As clsMSGFRunner.ePeptideHitResultType, ByVal strDatasetName As String, ByVal intJob As Integer, ByVal strSourceFolderPath As String)
		mResultType = eResultType
		mDatasetName = strDatasetName
		mJob = intJob
		mWorkDir = strSourceFolderPath
		mConnectionString = DEFAULT_CONNECTION_STRING
	End Sub

	Public Sub New(ByVal eResultType As clsMSGFRunner.ePeptideHitResultType, ByVal strDatasetName As String, ByVal intJob As Integer, ByVal strSourceFolderPath As String, ByVal strConnectionString As String)
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

		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim blnHeaderLineParsed As Boolean = False
		Dim objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer)

		Dim lstUniqueSpectra As System.Collections.Generic.Dictionary(Of String, Integer)

		Dim intLinesRead As Integer
		Dim blnSkipLine As Boolean

		Dim intScan As Integer
		Dim intCharge As Integer
		Dim strScanChargeCombo As String

		Dim blnSuccess As Boolean = False

		Try

			' Initialize the list that will be used to track the number of spectra searched
			lstUniqueSpectra = New System.Collections.Generic.Dictionary(Of String, Integer)

			' Initialize the column mapping
			' Using a case-insensitive comparer
			objColumnHeaders = New System.Collections.Generic.SortedDictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

			' Initialize the column mapping
			objColumnHeaders.Add(FHT_COLUMN_Result_ID, -1)
			objColumnHeaders.Add(FHT_COLUMN_Scan, -1)
			objColumnHeaders.Add(FHT_COLUMN_Charge, -1)

			Using srFHTFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strFirstHitsFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				While srFHTFile.Peek > -1
					strLineIn = srFHTFile.ReadLine()
					intLinesRead += 1
					blnSkipLine = False

					If Not String.IsNullOrWhiteSpace(strLineIn) Then
						strSplitLine = strLineIn.Split(ControlChars.Tab)

						If Not blnHeaderLineParsed Then
							' Parse the header line to confirm the column ordering

							' Standardize some column names first
							For intIndex As Integer = 0 To strSplitLine.Length - 1
								Select Case strSplitLine(intIndex)
									Case FHT_COLUMN_Result_ID_Alt1, FHT_COLUMN_Result_ID_Alt2
										strSplitLine(intIndex) = FHT_COLUMN_Result_ID

									Case FHT_COLUMN_Scan_Alt
										strSplitLine(intIndex) = FHT_COLUMN_Scan

									Case FHT_COLUMN_Charge_Alt
										strSplitLine(intIndex) = FHT_COLUMN_Charge
								End Select
							Next

							clsMSGFInputCreator.ParseColumnHeaders(strSplitLine, objColumnHeaders)

							blnHeaderLineParsed = True
						End If

						If Not blnSkipLine AndAlso strSplitLine.Length >= 4 Then

							intScan = clsMSGFInputCreator.LookupColumnValue(strSplitLine, FHT_COLUMN_Scan, objColumnHeaders, -1)

							If intScan >= 0 Then

								intCharge = clsMSGFInputCreator.LookupColumnValue(strSplitLine, FHT_COLUMN_Charge, objColumnHeaders, -1)

								If intCharge >= 0 Then
									strScanChargeCombo = intScan.ToString() & "_" & intCharge.ToString()

									If Not lstUniqueSpectra.ContainsKey(strScanChargeCombo) Then
										lstUniqueSpectra.Add(strScanChargeCombo, 0)
									End If

								End If

							End If

						End If
					End If
				End While

			End Using

			mSpectraSearched = lstUniqueSpectra.Count

			blnSuccess = True

		Catch ex As Exception
			mErrorMessage = ex.Message
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
				mErrorMessage = "Connection string empty in PostJobPSMResults"
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
				mErrorMessage = "Error storing PSM Results in database, " & strStoredProcedure & " returned " & ResCode.ToString
				If Not String.IsNullOrEmpty(strErrorMessage) Then
					mErrorMessage &= "; " & strErrorMessage
				End If
				blnSuccess = False
			End If

		Catch ex As System.Exception
			mErrorMessage = "Exception storing PSM Results in database: " & ex.Message
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Public Function ProcessMSGFResults() As Boolean

		Dim strPHRPFirstHitsFileName As String
		Dim strPHRPFirstHitsFilePath As String

		Dim strPHRPSynopsisFileName As String
		Dim strMSGFResultsFilePath As String

		Dim blnSuccess As Boolean

		' The keys in this dictionary are Result_ID values
		' The values are the mapped protein name
		' We'll deal with multiple proteins for each peptide later when we parse the _ResultToSeqMap.txt and _SeqToProteinMap.txt files
		' If those files are not found, then we'll simply use the protein information stored in lstPSMs
		Dim lstPSMs As System.Collections.Generic.Dictionary(Of Integer, String)

		Dim objResultToSeqMap As System.Collections.Generic.SortedList(Of Integer, Integer)
		Dim objSeqToProteinMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of String))

		Try
			mErrorMessage = String.Empty
			mSpectraSearched = 0
			mTotalPSMs = 0
			mUniquePeptideCount = 0
			mUniqueProteinCount = 0

			' We use the First-hits file to determine the number of MS/MS spectra that were searched (unique combo of charge and scan number)
			strPHRPFirstHitsFileName = clsMSGFRunner.GetPHRPFirstHitsFileName(mResultType, mDatasetName)

			' We use the Synopsis file to count the number of peptides and proteins observed
			strPHRPSynopsisFileName = clsMSGFRunner.GetPHRPSynopsisFileName(mResultType, mDatasetName)

			If mResultType = clsMSGFRunner.ePeptideHitResultType.XTandem Then
				' X!Tandem results don't have first-hits files; use the Synopsis file
				strPHRPFirstHitsFileName = strPHRPSynopsisFileName
			End If

			mMSGFSynopsisFileName = System.IO.Path.GetFileNameWithoutExtension(strPHRPSynopsisFileName) & clsMSGFInputCreator.MSGF_RESULT_FILENAME_SUFFIX


			strPHRPFirstHitsFilePath = System.IO.Path.Combine(mWorkDir, strPHRPFirstHitsFileName)
			strMSGFResultsFilePath = System.IO.Path.Combine(mWorkDir, mMSGFSynopsisFileName)

			If Not System.IO.File.Exists(strMSGFResultsFilePath) Then
				mErrorMessage = "File not found: " & strMSGFResultsFilePath
				Return False
			End If

			If System.IO.File.Exists(strPHRPFirstHitsFilePath) Then
				' Determine the number of MS/MS spectra searched
				ExamineFirstHitsFile(strPHRPFirstHitsFilePath)
			End If

			lstPSMs = New System.Collections.Generic.Dictionary(Of Integer, String)
			blnSuccess = LoadMSGFResults(strMSGFResultsFilePath, lstPSMs)

			If blnSuccess Then
				objResultToSeqMap = New System.Collections.Generic.SortedList(Of Integer, Integer)
				objSeqToProteinMap = New System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of String))

				' Load the protein information
				blnSuccess = LoadProteinMapping(mResultType, objResultToSeqMap, objSeqToProteinMap)

				If blnSuccess Then
					' Summarize the results, counting the number of peptides, unique peptides, and proteins
					blnSuccess = SummarizeResults(lstPSMs, objResultToSeqMap, objSeqToProteinMap)
				End If

				If blnSuccess AndAlso mSaveResultsToTextFile Then
					blnSuccess = SaveResultsToFile()
				End If

				If blnSuccess AndAlso mPostJobPSMResultsToDB Then
					blnSuccess = PostJobPSMResults()
				End If
			End If

			blnSuccess = True

		Catch ex As Exception
			mErrorMessage = ex.Message
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Protected Function LoadMSGFResults(ByVal strMSGFResultsFilePath As String, ByRef lstPSMs As System.Collections.Generic.Dictionary(Of Integer, String)) As Boolean

		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim blnHeaderLineParsed As Boolean = False
		Dim objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer)

		Dim intLinesRead As Integer
		Dim blnSkipLine As Boolean

		Dim dblSpecProb As Double
		Dim intResultID As Integer
		Dim strProtein As String

		Dim blnSuccess As Boolean = False

		Try

			' Initialize the column mapping
			' Using a case-insensitive comparer
			objColumnHeaders = New System.Collections.Generic.SortedDictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

			' Define the default column mapping
			objColumnHeaders.Add(MSGF_RESULT_COLUMN_Result_ID, 0)
			objColumnHeaders.Add(MSGF_RESULT_COLUMN_Scan, 1)
			objColumnHeaders.Add(MSGF_RESULT_COLUMN_Charge, 2)
			objColumnHeaders.Add(MSGF_RESULT_COLUMN_Protein, 3)
			objColumnHeaders.Add(MSGF_RESULT_COLUMN_Peptide, 4)
			objColumnHeaders.Add(MSGF_RESULT_COLUMN_SpecProb, 5)
			objColumnHeaders.Add(MSGF_RESULT_COLUMN_Notes, 6)

			Using srMSGFResults As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strMSGFResultsFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				While srMSGFResults.Peek > -1
					strLineIn = srMSGFResults.ReadLine()
					intLinesRead += 1
					blnSkipLine = False

					If Not String.IsNullOrWhiteSpace(strLineIn) Then
						strSplitLine = strLineIn.Split(ControlChars.Tab)

						If Not blnHeaderLineParsed Then
							If Not clsMSGFInputCreator.IsNumber(strSplitLine(0)) Then
								' Parse the header line to confirm the column ordering
								clsMSGFInputCreator.ParseColumnHeaders(strSplitLine, objColumnHeaders)
								blnSkipLine = True
							End If
							blnHeaderLineParsed = True
						End If

						If Not blnSkipLine AndAlso strSplitLine.Length >= 4 Then

							dblSpecProb = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_SpecProb, objColumnHeaders, 10.0#)

							If dblSpecProb < 10 AndAlso dblSpecProb <= mMSGFThreshold Then

								intResultID = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Result_ID, objColumnHeaders, 0)
								strProtein = clsMSGFInputCreator.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Protein, objColumnHeaders)

								' Store in lstPSMs
								If Not lstPSMs.ContainsKey(intResultID) Then
									lstPSMs.Add(intResultID, strProtein)
								End If

							End If

						End If
					End If
				End While

			End Using

			blnSuccess = True

		Catch ex As Exception
			mErrorMessage = ex.Message
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function


	''' <summary>
	''' Load the proteins using the PHRP result files
	''' Applies to X!Tandem results and Sequest, Inspect, or MSGFDB Synopsis file results
	''' Does not apply to Sequest or MSGFDB First-Hits files
	''' </summary>
	''' <param name="eResultType"></param>
	''' <param name="objResultToSeqMap"></param>
	''' ''' <param name="objSeqToProteinMap"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function LoadProteinMapping(ByVal eResultType As clsMSGFRunner.ePeptideHitResultType, _
	 ByRef objResultToSeqMap As System.Collections.Generic.SortedList(Of Integer, Integer), _
	 ByRef objSeqToProteinMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of String))) As Boolean

		Dim strResultToSeqMapFilename As String = String.Empty
		Dim strSeqToProteinMapFilename As String = String.Empty

		Dim blnSuccess As Boolean

		Try

			strResultToSeqMapFilename = clsMSGFRunner.GetPHRPResultToSeqMapFileName(eResultType, mDatasetName)
			strSeqToProteinMapFilename = clsMSGFRunner.GetPHRPSeqToProteinMapFileName(eResultType, mDatasetName)

			If String.IsNullOrEmpty(strResultToSeqMapFilename) Then
				blnSuccess = False
			Else
				blnSuccess = LoadResultToSeqMapping(System.IO.Path.Combine(mWorkDir, strResultToSeqMapFilename), objResultToSeqMap)
			End If

			If blnSuccess AndAlso Not String.IsNullOrEmpty(strSeqToProteinMapFilename) Then
				blnSuccess = LoadSeqToProteinMapping(System.IO.Path.Combine(mWorkDir, strSeqToProteinMapFilename), objSeqToProteinMap)
			End If

		Catch ex As Exception
			mErrorMessage = "Exception loading protein results: " & ex.Message
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Load the Result to Seq mapping using the specified PHRP result file
	''' </summary>
	''' <param name="strFilePath"></param>
	''' <param name="objResultToSeqMap"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function LoadResultToSeqMapping(ByVal strFilePath As String, ByRef objResultToSeqMap As System.Collections.Generic.SortedList(Of Integer, Integer)) As Boolean

		Dim srInFile As System.IO.StreamReader

		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim intLinesRead As Integer

		Dim intResultID As Integer
		Dim intSeqID As Integer

		Try

			' Read the data from the result to sequence map file
			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

			intLinesRead = 0

			Do While srInFile.Peek >= 0
				strLineIn = srInFile.ReadLine
				intLinesRead += 1

				If Not String.IsNullOrEmpty(strLineIn) Then
					strSplitLine = strLineIn.Split(ControlChars.Tab)

					If strSplitLine.Length >= 2 Then

						' Parse out the numbers from the first two columns 
						' (the first line of the file is the header line, and it will get skipped)
						If Integer.TryParse(strSplitLine(0), intResultID) Then
							If Integer.TryParse(strSplitLine(1), intSeqID) Then

								If Not objResultToSeqMap.ContainsKey(intResultID) Then
									objResultToSeqMap.Add(intResultID, intSeqID)
								End If
							End If
						End If

					End If
				End If
			Loop

			srInFile.Close()

		Catch ex As Exception
			mErrorMessage = "Exception reading " & System.IO.Path.GetFileName(strFilePath) & ": " & ex.Message
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Load the Sequence to Protein mapping using the specified PHRP result file
	''' </summary>
	''' <param name="strFilePath"></param>
	''' <param name="objSeqToProteinMap"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function LoadSeqToProteinMapping(ByVal strFilePath As String, _
	  ByRef objSeqToProteinMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of String))) As Boolean

		Dim srInFile As System.IO.StreamReader

		Dim lstProteins As System.Collections.Generic.List(Of String) = Nothing

		Dim objColumnHeaders As System.Collections.Generic.SortedDictionary(Of String, Integer)

		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim strProtein As String
		Dim intLinesRead As Integer

		Dim intSeqID As Integer

		Dim blnHeaderLineParsed As Boolean
		Dim blnSkipLine As Boolean

		Try

			' Initialize the column mapping
			' Using a case-insensitive comparer
			objColumnHeaders = New System.Collections.Generic.SortedDictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

			' Define the default column mapping
			objColumnHeaders.Add(SEQ_PROT_MAP_COLUMN_Unique_Seq_ID, 0)
			objColumnHeaders.Add(SEQ_PROT_MAP_COLUMN_Cleavage_State, 1)
			objColumnHeaders.Add(SEQ_PROT_MAP_COLUMN_Terminus_State, 2)
			objColumnHeaders.Add(SEQ_PROT_MAP_COLUMN_Protein_Name, 3)
			objColumnHeaders.Add(SEQ_PROT_MAP_COLUMN_Protein_EValue, 4)
			objColumnHeaders.Add(SEQ_PROT_MAP_COLUMN_Protein_Intensity, 5)

			' Read the data from the sequence to protein map file
			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

			intLinesRead = 0

			Do While srInFile.Peek >= 0
				strLineIn = srInFile.ReadLine
				intLinesRead += 1
				blnSkipLine = False

				If Not String.IsNullOrEmpty(strLineIn) Then
					strSplitLine = strLineIn.Split(ControlChars.Tab)

					If Not blnHeaderLineParsed Then
						If strSplitLine(0).ToLower() = SEQ_PROT_MAP_COLUMN_Unique_Seq_ID.ToLower Then
							' Parse the header line to confirm the column ordering
							clsMSGFInputCreator.ParseColumnHeaders(strSplitLine, objColumnHeaders)
							blnSkipLine = True
						End If

						blnHeaderLineParsed = True
					End If

					If Not blnSkipLine AndAlso strSplitLine.Length >= 3 Then

						If Integer.TryParse(strSplitLine(0), intSeqID) Then

							strProtein = clsMSGFInputCreator.LookupColumnValue(strSplitLine, SEQ_PROT_MAP_COLUMN_Protein_Name, objColumnHeaders, String.Empty)

							If Not String.IsNullOrEmpty(strProtein) Then

								If objSeqToProteinMap.TryGetValue(intSeqID, lstProteins) Then
									If Not lstProteins.Contains(strProtein) Then
										lstProteins.Add(strProtein)
										objSeqToProteinMap(intSeqID) = lstProteins
									End If
								Else
									lstProteins = New System.Collections.Generic.List(Of String)
									lstProteins.Add(strProtein)
									objSeqToProteinMap.Add(intSeqID, lstProteins)
								End If

							End If

						End If

					End If

				End If
			Loop

			srInFile.Close()

		Catch ex As Exception
			mErrorMessage = "Exception reading " & System.IO.Path.GetFileName(strFilePath) & ": " & ex.Message
			Return False
		End Try

		Return True

	End Function

	Protected Function SaveResultsToFile() As Boolean

		Dim strOutputFilePath As String = "??"

		Try
			strOutputFilePath = System.IO.Path.Combine(mWorkDir, mDatasetName & "_PSM_Stats.txt")

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
			mErrorMessage = "Exception saving results to " & strOutputFilePath & ": " & ex.Message
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Summarize the results by inter-relating lstPSMs, objResultToSeqMap, and objSeqToProteinMap
	''' </summary>
	''' <param name="lstPSMs"></param>
	''' <param name="objResultToSeqMap"></param>
	''' <param name="objSeqToProteinMap"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function SummarizeResults(ByRef lstPSMs As System.Collections.Generic.Dictionary(Of Integer, String), _
	 ByRef objResultToSeqMap As System.Collections.Generic.SortedList(Of Integer, Integer), _
	 ByRef objSeqToProteinMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of String))) As Boolean

		' lstPSMs only contains the filter-passing results
		' Link up with objResultToSeqMap to determine the unique number of filter-passing peptides
		' Link up with objSeqToProteinMap to determine the unique number of proteins

		' The Keys are SeqID values; the values are observation count
		Dim lstUniqueSequences As System.Collections.Generic.Dictionary(Of Integer, Integer)

		' The Keys are protein names; the values are observation count
		Dim lstUniqueProteins As System.Collections.Generic.Dictionary(Of String, Integer)

		Dim intSeqID As Integer
		Dim intObsCount As Integer
		Dim lstProteins As System.Collections.Generic.List(Of String) = Nothing

		Try
			lstUniqueSequences = New System.Collections.Generic.Dictionary(Of Integer, Integer)
			lstUniqueProteins = New System.Collections.Generic.Dictionary(Of String, Integer)

			For Each objResultID As System.Collections.Generic.KeyValuePair(Of Integer, String) In lstPSMs

				If objResultToSeqMap.TryGetValue(objResultID.Key, intSeqID) Then
					' Result found in _ResultToSeqMap.txt file

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
				End If
			Next

			' Store the stats
			mTotalPSMs = lstPSMs.Count
			mUniquePeptideCount = lstUniqueSequences.Count
			mUniqueProteinCount = lstUniqueProteins.Count


		Catch ex As Exception
			mErrorMessage = "Exception summarizing results: " & ex.Message
			Return False
		End Try

		Return True

	End Function

End Class
