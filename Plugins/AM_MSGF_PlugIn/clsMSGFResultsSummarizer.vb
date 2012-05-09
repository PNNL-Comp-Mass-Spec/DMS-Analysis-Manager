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
	Public Const DEFAULT_FDR_THRESHOLD As Double = 0.01				' 1% FDR

	Public Property DEFAULT_CONNECTION_STRING As String = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;"

	Protected Const STORE_JOB_PSM_RESULTS_SP_NAME As String = "StoreJobPSMStats"
#End Region

#Region "Structures"
	Protected Structure udtPSMInfoType
		Public Protein As String
		Public FDR As Double
		Public MSGF As Double
	End Structure

	Protected Structure udtPSMStatsType
		Public TotalPSMs As Integer
		Public UniquePeptideCount As Integer
		Public UniqueProteinCount As Integer
		Public Sub Clear()
			TotalPSMs = 0
			UniquePeptideCount = 0
			UniqueProteinCount = 0
		End Sub
	End Structure

#End Region

#Region "Member variables"
	Private mErrorMessage As String = String.Empty

	Private mFDRThreshold As Double = DEFAULT_FDR_THRESHOLD
	Private mMSGFThreshold As Double = DEFAULT_MSGF_THRESHOLD
	Private mPostJobPSMResultsToDB As Boolean = False

	Private mSaveResultsToTextFile As Boolean = True
	Private mOutputFolderPath As String = String.Empty

	Private mSpectraSearched As Integer = 0

	Private mMSGFBasedCounts As udtPSMStatsType
	Private mFDRBasedCounts As udtPSMStatsType

	Private mResultType As clsPHRPReader.ePeptideHitResultType
	Private mDatasetName As String
	Private mJob As Integer
	Private mWorkDir As String
	Private mConnectionString As String

	' The following is auto-determined in ProcessMSGFResults
	Private mMSGFSynopsisFileName As String = String.Empty
#End Region

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

	Public Property FDRThreshold As Double
		Get
			Return mFDRThreshold
		End Get
		Set(value As Double)
			mFDRThreshold = value
		End Set
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

	Public ReadOnly Property TotalPSMsFDR As Integer
		Get
			Return mFDRBasedCounts.TotalPSMs
		End Get
	End Property

	Public ReadOnly Property TotalPSMsMSGF As Integer
		Get
			Return mMSGFBasedCounts.TotalPSMs
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

	Public ReadOnly Property UniquePeptideCountFDR As Integer
		Get
			Return mFDRBasedCounts.UniquePeptideCount
		End Get
	End Property

	Public ReadOnly Property UniquePeptideCountMSGF As Integer
		Get
			Return mMSGFBasedCounts.UniquePeptideCount
		End Get
	End Property

	Public ReadOnly Property UniqueProteinCountFDR As Integer
		Get
			Return mFDRBasedCounts.UniqueProteinCount
		End Get
	End Property

	Public ReadOnly Property UniqueProteinCountMSGF As Integer
		Get
			Return mMSGFBasedCounts.UniqueProteinCount
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

			Using objReader As clsPHRPReader = New clsPHRPReader(strFirstHitsFilePath, blnLoadModsAndSeqInfo:=False, blnLoadMSGFResults:=False)
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

	''' <summary>
	''' Either filter by MSGF or filter by FDR, then update the stats
	''' </summary>
	''' <param name="blnUsingMSGFFilter">When true, then filter by MSGF, otherwise filter by FDR</param>
	''' <param name="lstPSMs"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function FilterAndComputeStats(ByVal blnUsingMSGFFilter As Boolean, ByRef lstPSMs As System.Collections.Generic.Dictionary(Of Integer, udtPSMInfoType)) As Boolean

		Dim lstFilteredPSMs As System.Collections.Generic.Dictionary(Of Integer, udtPSMInfoType)
		lstFilteredPSMs = New System.Collections.Generic.Dictionary(Of Integer, udtPSMInfoType)

		Dim blnSuccess As Boolean

		If blnUsingMSGFFilter AndAlso mMSGFThreshold < 1 Then
			' Filter on MSGF
			blnSuccess = FilterPSMsByMSGF(mMSGFThreshold, lstPSMs, lstFilteredPSMs)
		Else
			' Keep all PSMs
			For Each kvEntry As System.Collections.Generic.KeyValuePair(Of Integer, udtPSMInfoType) In lstPSMs
				lstFilteredPSMs.Add(kvEntry.Key, kvEntry.Value)
			Next
			blnSuccess = True
		End If

		If Not blnUsingMSGFFilter AndAlso mFDRThreshold < 1 Then
			' Filter on FDR (we'll compute the FDR using Reverse Proteins, if necessary)
			blnSuccess = FilterPSMsByFDR(mFDRThreshold, lstFilteredPSMs)
		End If

		If blnSuccess Then
			Dim objReader As PHRPReader.clsPHRPSeqMapReader
			Dim lstResultToSeqMap As System.Collections.Generic.SortedList(Of Integer, Integer) = Nothing
			Dim lstSeqToProteinMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of PHRPReader.clsProteinInfo)) = Nothing
			Dim lstSeqInfo As System.Collections.Generic.SortedList(Of Integer, PHRPReader.clsSeqInfo) = Nothing

			' Load the protein information and associate with the data in lstFilteredPSMs
			objReader = New PHRPReader.clsPHRPSeqMapReader(mDatasetName, mWorkDir, mResultType)
			blnSuccess = objReader.GetProteinMapping(lstResultToSeqMap, lstSeqToProteinMap, lstSeqInfo)

			If blnSuccess Then
				' Summarize the results, counting the number of peptides, unique peptides, and proteins
				blnSuccess = SummarizeResults(blnUsingMSGFFilter, lstFilteredPSMs, lstResultToSeqMap, lstSeqToProteinMap)
			End If

		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Filter the data using mFDRThreshold
	''' </summary>
	''' <param name="dblFDRThreshold"></param>
	''' <param name="lstPSMs"></param>
	''' <returns>True if success; false if no reverse hits are present or if none of the data has MSGF values</returns>
	''' <remarks></remarks>
	Protected Function FilterPSMsByFDR(ByVal dblFDRThreshold As Double, ByRef lstPSMs As System.Collections.Generic.Dictionary(Of Integer, udtPSMInfoType)) As Boolean

		Dim lstResultIDtoFDRMap As System.Collections.Generic.Dictionary(Of Integer, Double)

		Dim blnAlreadyComputed As Boolean
		Dim blnValidMSGFData As Boolean

		blnAlreadyComputed = True
		For Each kvEntry As System.Collections.Generic.KeyValuePair(Of Integer, udtPSMInfoType) In lstPSMs
			If kvEntry.Value.FDR < 0 Then
				blnAlreadyComputed = False
				Exit For
			End If
		Next

		lstResultIDtoFDRMap = New System.Collections.Generic.Dictionary(Of Integer, Double)
		If blnAlreadyComputed Then
			For Each kvEntry As System.Collections.Generic.KeyValuePair(Of Integer, udtPSMInfoType) In lstPSMs
				lstResultIDtoFDRMap.Add(kvEntry.Key, kvEntry.Value.FDR)
			Next
		Else

			' Sort the data by ascending SpecProb, then step through the list and compute FDR
			' Use FDR = #Reverse / #Forward
			'
			' Alternative FDR formula is:  FDR = 2*#Reverse / (#Forward + #Reverse)
			' Since MSGFDB uses "#Reverse / #Forward" we'll use that here too
			'
			' If no reverse hits are present or if none of the data has MSGF values, then we'll clear lstPSMs and update mErrorMessage			

			' Populate a list with the MSGF values and ResultIDs so that we can step through the data and compute the FDR for each entry
			Dim lstMSGFtoResultIDMap As System.Collections.Generic.List(Of System.Collections.Generic.KeyValuePair(Of Double, Integer))
			lstMSGFtoResultIDMap = New System.Collections.Generic.List(Of System.Collections.Generic.KeyValuePair(Of Double, Integer))

			blnValidMSGFData = False
			For Each kvEntry As System.Collections.Generic.KeyValuePair(Of Integer, udtPSMInfoType) In lstPSMs
				lstMSGFtoResultIDMap.Add(New System.Collections.Generic.KeyValuePair(Of Double, Integer)(kvEntry.Value.MSGF, kvEntry.Key))
				If kvEntry.Value.MSGF < 1 Then blnValidMSGFData = True
			Next

			If Not blnValidMSGFData Then
				' None of the data has MSGF values; cannot compute FDR
				mErrorMessage = "Data does not contain MSGF values; cannot compute a decoy-based FDR"
				lstPSMs.Clear()
				Return False
			End If

			' Sort lstMSGFtoResultIDMap
			lstMSGFtoResultIDMap.Sort(New clsMSGFtoResultIDMapComparer)

			Dim intForwardResults As Integer = 0
			Dim intDecoyResults As Integer = 0
			Dim strProtein As String
			Dim lstMissedResultIDsAtStart As System.Collections.Generic.List(Of Integer)
			lstMissedResultIDsAtStart = New System.Collections.Generic.List(Of Integer)

			For Each kvEntry As System.Collections.Generic.KeyValuePair(Of Double, Integer) In lstMSGFtoResultIDMap
				strProtein = lstPSMs(kvEntry.Value).Protein.ToLower()

				' MTS reversed proteins                 'reversed[_]%'
				' MTS scrambled proteins                'scrambled[_]%'
				' X!Tandem decoy proteins               '%[:]reversed'
				' Inspect reversed/scrambled proteins   'xxx.%'
				' MSGFDB reversed proteins              'rev[_]%'

				If strProtein.StartsWith("reversed_") OrElse _
				   strProtein.StartsWith("scrambled_") OrElse _
				   strProtein.EndsWith(":reversed") OrElse _
				   strProtein.StartsWith("xxx.") OrElse _
				   strProtein.StartsWith("rev_") Then
					intDecoyResults += 1
				Else
					intForwardResults += 1
				End If

				If intForwardResults > 0 Then
					' Compute and store the FDR for this entry
					dblFDRThreshold = intDecoyResults / CDbl(intForwardResults)
					lstResultIDtoFDRMap.Add(kvEntry.Value, dblFDRThreshold)

					If lstMissedResultIDsAtStart.Count > 0 Then
						For Each intResultID As Integer In lstMissedResultIDsAtStart
							lstResultIDtoFDRMap.Add(intResultID, dblFDRThreshold)
						Next
						lstMissedResultIDsAtStart.Clear()
					End If
				Else
					' We cannot yet compute the FDR since all proteins up to this point are decoy proteins
					' Update lstMissedResultIDsAtStart
					lstMissedResultIDsAtStart.Add(kvEntry.Value)
				End If
			Next

			If intDecoyResults = 0 Then
				' We never encountered any decoy proteins; cannot compute FDR
				mErrorMessage = "Data does not contain decoy proteins; cannot compute a decoy-based FDR"
				lstPSMs.Clear()
				Return False
			End If
		End If


		' Remove entries from lstPSMs where .FDR is larger than mFDRThreshold
		Dim udtPSMInfo As udtPSMInfoType = New udtPSMInfoType

		For Each kvEntry As System.Collections.Generic.KeyValuePair(Of Integer, Double) In lstResultIDtoFDRMap
			If kvEntry.Value > mFDRThreshold Then
				lstPSMs.Remove(kvEntry.Key)
			Else
				If lstPSMs.TryGetValue(kvEntry.Key, udtPSMInfo) Then
					' Update the FDR value (this isn't really necessary, but it doesn't hurt do to so)
					udtPSMInfo.FDR = kvEntry.Value
					lstPSMs(kvEntry.Key) = udtPSMInfo
				End If
			End If
		Next

		Return True

	End Function

	Protected Function FilterPSMsByMSGF(ByVal dblMSGFThreshold As Double, ByRef lstPSMs As System.Collections.Generic.Dictionary(Of Integer, udtPSMInfoType), ByRef lstFilteredPSMs As System.Collections.Generic.Dictionary(Of Integer, udtPSMInfoType)) As Boolean

		lstFilteredPSMs.Clear()

		For Each kvEntry As System.Collections.Generic.KeyValuePair(Of Integer, udtPSMInfoType) In lstPSMs
			If kvEntry.Value.MSGF <= dblMSGFThreshold Then
				lstFilteredPSMs.Add(kvEntry.Key, kvEntry.Value)
			End If
		Next

		Return True

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

				.Parameters.Add(New SqlClient.SqlParameter("@FDRThreshold", SqlDbType.Float))
				.Parameters.Item("@FDRThreshold").Direction = ParameterDirection.Input
				.Parameters.Item("@FDRThreshold").Value = mFDRThreshold

				.Parameters.Add(New SqlClient.SqlParameter("@SpectraSearched", SqlDbType.Int))
				.Parameters.Item("@SpectraSearched").Direction = ParameterDirection.Input
				.Parameters.Item("@SpectraSearched").Value = mSpectraSearched

				.Parameters.Add(New SqlClient.SqlParameter("@TotalPSMs", SqlDbType.Int))
				.Parameters.Item("@TotalPSMs").Direction = ParameterDirection.Input
				.Parameters.Item("@TotalPSMs").Value = mMSGFBasedCounts.TotalPSMs

				.Parameters.Add(New SqlClient.SqlParameter("@UniquePeptides", SqlDbType.Int))
				.Parameters.Item("@UniquePeptides").Direction = ParameterDirection.Input
				.Parameters.Item("@UniquePeptides").Value = mMSGFBasedCounts.UniquePeptideCount

				.Parameters.Add(New SqlClient.SqlParameter("@UniqueProteins", SqlDbType.Int))
				.Parameters.Item("@UniqueProteins").Direction = ParameterDirection.Input
				.Parameters.Item("@UniqueProteins").Value = mMSGFBasedCounts.UniqueProteinCount

				.Parameters.Add(New SqlClient.SqlParameter("@TotalPSMsFDRFilter", SqlDbType.Int))
				.Parameters.Item("@TotalPSMsFDRFilter").Direction = ParameterDirection.Input
				.Parameters.Item("@TotalPSMsFDRFilter").Value = mFDRBasedCounts.TotalPSMs

				.Parameters.Add(New SqlClient.SqlParameter("@UniquePeptidesFDRFilter", SqlDbType.Int))
				.Parameters.Item("@UniquePeptidesFDRFilter").Direction = ParameterDirection.Input
				.Parameters.Item("@UniquePeptidesFDRFilter").Value = mFDRBasedCounts.UniquePeptideCount

				.Parameters.Add(New SqlClient.SqlParameter("@UniqueProteinsFDRFilter", SqlDbType.Int))
				.Parameters.Item("@UniqueProteinsFDRFilter").Direction = ParameterDirection.Input
				.Parameters.Item("@UniqueProteinsFDRFilter").Value = mFDRBasedCounts.UniqueProteinCount

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

	''' <summary>
	''' Process this dataset's synopsis file to determine the PSM stats
	''' </summary>
	''' <returns>True if success; false if an error</returns>
	''' <remarks></remarks>
	Public Function ProcessMSGFResults() As Boolean

		Dim strPHRPFirstHitsFileName As String
		Dim strPHRPFirstHitsFilePath As String

		Dim strPHRPSynopsisFileName As String
		Dim strPHRPSynopsisFilePath As String

		Dim blnFilterWithMSGF As Boolean

		Dim blnSuccess As Boolean
		Dim blnSuccessViaFDR As Boolean

		' The keys in this dictionary are Result_ID values
		' The values contain mapped protein name, FDR, and MSGF SpecProb
		' We'll deal with multiple proteins for each peptide later when we parse the _ResultToSeqMap.txt and _SeqToProteinMap.txt files
		' If those files are not found, then we'll simply use the protein information stored in lstPSMs
		Dim lstPSMs As System.Collections.Generic.Dictionary(Of Integer, udtPSMInfoType)

		Try
			mErrorMessage = String.Empty
			mSpectraSearched = 0
			mMSGFBasedCounts.Clear()
			mFDRBasedCounts.Clear()

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

			''''''''''''''''''''
			' Load the PSMs
			'
			lstPSMs = New System.Collections.Generic.Dictionary(Of Integer, udtPSMInfoType)
			blnSuccess = LoadPSMs(strPHRPSynopsisFilePath, lstPSMs)


			''''''''''''''''''''
			' Filter on MSGF and compute the stats
			'
			blnFilterWithMSGF = True
			blnSuccess = FilterAndComputeStats(blnFilterWithMSGF, lstPSMs)

			''''''''''''''''''''
			' Filter on FDR and compute the stats
			'
			blnFilterWithMSGF = False
			blnSuccessViaFDR = FilterAndComputeStats(blnFilterWithMSGF, lstPSMs)

			If blnSuccess OrElse blnSuccessViaFDR Then
				If mSaveResultsToTextFile Then
					' Note: Continue processing even if this step fails
					SaveResultsToFile()
				End If

				If mPostJobPSMResultsToDB Then
					blnSuccess = PostJobPSMResults()
				Else
					blnSuccess = True
				End If
			End If

		Catch ex As Exception
			SetErrorMessage(ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Protected Function LoadPSMs(ByVal strPHRPSynopsisFilePath As String, ByRef lstPSMs As System.Collections.Generic.Dictionary(Of Integer, udtPSMInfoType)) As Boolean

		Dim dblSpecProb As Double
		Dim dblMSGFSpecProb As Double

		Dim blnSuccess As Boolean = False
		Dim udtPSMInfo As udtPSMInfoType

		Try

			Using objPHRPReader As New PHRPReader.clsPHRPReader(strPHRPSynopsisFilePath, blnLoadModsAndSeqInfo:=False, blnLoadMSGFResults:=True)

				Do While objPHRPReader.MoveNext()

					Dim objPSM As PHRPReader.clsPSM
					objPSM = objPHRPReader.CurrentPSM

					If Double.TryParse(objPSM.MSGFSpecProb, dblSpecProb) Then

						' Store in lstPSMs
						If Not lstPSMs.ContainsKey(objPSM.ResultID) Then

							udtPSMInfo.Protein = objPSM.ProteinFirst
							If Double.TryParse(objPSM.MSGFSpecProb, dblMSGFSpecProb) Then
								udtPSMInfo.MSGF = dblMSGFSpecProb
							Else
								udtPSMInfo.MSGF = 1
							End If

							If mResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
								udtPSMInfo.FDR = objPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_FDR, -1)
								If udtPSMInfo.FDR < 0 Then
									udtPSMInfo.FDR = objPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_EFDR, -1)
								End If
							Else
								udtPSMInfo.FDR = -1
							End If

							lstPSMs.Add(objPSM.ResultID, udtPSMInfo)
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
				  "FDR_Threshold" & ControlChars.Tab & _
				  "Spectra_Searched" & ControlChars.Tab & _
				  "Total_PSMs_MSGF_Filtered" & ControlChars.Tab & _
				  "Unique_Peptides_MSGF_Filtered" & ControlChars.Tab & _
				  "Unique_Proteins_MSGF_Filtered" & ControlChars.Tab & _
				  "Total_PSMs_FDR_Filtered" & ControlChars.Tab & _
				  "Unique_Peptides_FDR_Filtered" & ControlChars.Tab & _
				  "Unique_Proteins_FDR_Filtered")

				' Stats
				swOutFile.WriteLine( _
				 mDatasetName & ControlChars.Tab & _
				 mJob & ControlChars.Tab & _
				 mMSGFThreshold.ToString("0.00E+00") & ControlChars.Tab & _
				 mFDRThreshold.ToString("0.000") & ControlChars.Tab & _
				 mSpectraSearched & ControlChars.Tab & _
				 mMSGFBasedCounts.TotalPSMs & ControlChars.Tab & _
				 mMSGFBasedCounts.UniquePeptideCount & ControlChars.Tab & _
				 mMSGFBasedCounts.UniqueProteinCount & ControlChars.Tab & _
				 mFDRBasedCounts.TotalPSMs & ControlChars.Tab & _
				 mFDRBasedCounts.UniquePeptideCount & ControlChars.Tab & _
				 mFDRBasedCounts.UniqueProteinCount)

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
	''' Summarize the results by inter-relating lstPSMs, lstResultToSeqMap, and lstSeqToProteinMap
	''' </summary>
	''' <param name="blnUsingMSGFFilter"></param>
	''' <param name="lstFilteredPSMs"></param>
	''' <param name="lstResultToSeqMap"></param>
	''' <param name="lstSeqToProteinMap"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function SummarizeResults(ByVal blnUsingMSGFFilter As Boolean, _
	  ByRef lstFilteredPSMs As System.Collections.Generic.Dictionary(Of Integer, udtPSMInfoType), _
	  ByRef lstResultToSeqMap As System.Collections.Generic.SortedList(Of Integer, Integer), _
	  ByRef lstSeqToProteinMap As System.Collections.Generic.SortedList(Of Integer, System.Collections.Generic.List(Of PHRPReader.clsProteinInfo))) As Boolean

		' lstPSMs only contains the filter-passing results (keys are ResultID, values are the first protein for each ResultID)
		' Link up with lstResultToSeqMap to determine the unique number of filter-passing peptides
		' Link up with lstSeqToProteinMap to determine the unique number of proteins

		' The Keys in this dictionary are SeqID values; the values are observation count
		Dim lstUniqueSequences As System.Collections.Generic.Dictionary(Of Integer, Integer)

		' The Keys in this dictionary are protein names; the values are observation count
		Dim lstUniqueProteins As System.Collections.Generic.Dictionary(Of String, Integer)

		Dim intObsCount As Integer
		Dim lstProteins As System.Collections.Generic.List(Of PHRPReader.clsProteinInfo) = Nothing

		Try
			lstUniqueSequences = New System.Collections.Generic.Dictionary(Of Integer, Integer)
			lstUniqueProteins = New System.Collections.Generic.Dictionary(Of String, Integer)

			For Each objResultID As System.Collections.Generic.KeyValuePair(Of Integer, udtPSMInfoType) In lstFilteredPSMs

				Dim intSeqID As Integer
				If lstResultToSeqMap.TryGetValue(objResultID.Key, intSeqID) Then
					' Result found in _ResultToSeqMap.txt file

					If lstUniqueSequences.TryGetValue(intSeqID, intObsCount) Then
						lstUniqueSequences(intSeqID) = intObsCount + 1
					Else
						lstUniqueSequences.Add(intSeqID, 1)
					End If

					' Lookup the proteins for this peptide
					If lstSeqToProteinMap.TryGetValue(intSeqID, lstProteins) Then
						' Update the observation count for each protein

						For Each objProtein As PHRPReader.clsProteinInfo In lstProteins

							If lstUniqueProteins.TryGetValue(objProtein.ProteinName, intObsCount) Then
								lstUniqueProteins(objProtein.ProteinName) = intObsCount + 1
							Else
								lstUniqueProteins.Add(objProtein.ProteinName, 1)
							End If

						Next

					End If

				End If
			Next

			' Store the stats
			If blnUsingMSGFFilter Then
				mMSGFBasedCounts.TotalPSMs = lstFilteredPSMs.Count
				mMSGFBasedCounts.UniquePeptideCount = lstUniqueSequences.Count
				mMSGFBasedCounts.UniqueProteinCount = lstUniqueProteins.Count
			Else
				mFDRBasedCounts.TotalPSMs = lstFilteredPSMs.Count
				mFDRBasedCounts.UniquePeptideCount = lstUniqueSequences.Count
				mFDRBasedCounts.UniqueProteinCount = lstUniqueProteins.Count
			End If			

		Catch ex As Exception
			SetErrorMessage("Exception summarizing results: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Class clsMSGFtoResultIDMapComparer
		Implements IComparer(Of System.Collections.Generic.KeyValuePair(Of Double, Integer))

		Public Function Compare(x As System.Collections.Generic.KeyValuePair(Of Double, Integer), y As System.Collections.Generic.KeyValuePair(Of Double, Integer)) As Integer Implements System.Collections.Generic.IComparer(Of System.Collections.Generic.KeyValuePair(Of Double, Integer)).Compare
			If x.Key < y.Key Then
				Return -1
			ElseIf x.Key > y.Key Then
				Return 1
			Else
				Return 0
			End If

		End Function
	End Class
End Class
