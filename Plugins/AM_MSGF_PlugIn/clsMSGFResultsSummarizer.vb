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

Imports System.Linq
Imports PHRPReader
Imports System.Data.SqlClient
Imports System.IO

Public Class clsMSGFResultsSummarizer

#Region "Constants and Enums"
	Public Const DEFAULT_MSGF_THRESHOLD As Double = 0.0000000001		' 1E-10
	Public Const DEFAULT_EVALUE_THRESHOLD As Double = 0.0001			' 1E-4   (only used when MSGF Scores are not available)
	Public Const DEFAULT_FDR_THRESHOLD As Double = 0.01					' 1% FDR

    Public Const DEFAULT_CONNECTION_STRING As String = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;"

	Protected Const STORE_JOB_PSM_RESULTS_SP_NAME As String = "StoreJobPSMStats"
	Protected Const UNKNOWN_MSGF_SPECPROB As Double = 10
	Protected Const UNKNOWN_EVALUE As Double = Double.MaxValue

#End Region

#Region "Structures"
	Protected Structure udtPSMInfoType
		Public Protein As String
		Public FDR As Double
		Public MSGF As Double			' MSGF SpecProb; will be UNKNOWN_MSGF_SPECPROB (10) if MSGF SpecProb is not available
		Public EValue As Double			' Only used when MSGF SpecProb is not available
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
	Private mEValueThreshold As Double = DEFAULT_EVALUE_THRESHOLD

	Private mPostJobPSMResultsToDB As Boolean = False

	Private mSaveResultsToTextFile As Boolean = True
	Private mOutputFolderPath As String = String.Empty

	Private mSpectraSearched As Integer = 0

	Private mMSGFBasedCounts As udtPSMStatsType
	Private mFDRBasedCounts As udtPSMStatsType

    Private ReadOnly mResultType As clsPHRPReader.ePeptideHitResultType
    Private ReadOnly mDatasetName As String
    Private ReadOnly mJob As Integer
    Private ReadOnly mWorkDir As String
    Private ReadOnly mConnectionString As String

    Private WithEvents mStoredProcedureExecutor As PRISM.DataBase.clsExecuteDatabaseSP

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

    Public Property EValueThreshold As Double
        Get
            Return mEValueThreshold
        End Get
        Set(value As Double)
            mEValueThreshold = value
        End Set
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

    Public ReadOnly Property ResultType As clsPHRPReader.ePeptideHitResultType
        Get
            Return mResultType
        End Get
    End Property

    Public ReadOnly Property ResultTypeName As String
        Get
            Return mResultType.ToString()
        End Get
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
        Me.New(eResultType, strDatasetName, intJob, strSourceFolderPath, DEFAULT_CONNECTION_STRING)
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

        mStoredProcedureExecutor = New PRISM.DataBase.clsExecuteDatabaseSP(mConnectionString)

    End Sub

    Protected Function ExamineFirstHitsFile(ByVal strFirstHitsFilePath As String) As Boolean

        Dim lstUniqueSpectra As Dictionary(Of String, Integer)
        Dim strScanChargeCombo As String

        Dim blnSuccess As Boolean

        Try

            ' Initialize the list that will be used to track the number of spectra searched
            lstUniqueSpectra = New Dictionary(Of String, Integer)

            Dim startupOptions As clsPHRPStartupOptions = clsMSGFInputCreator.GetMinimalMemoryPHRPStartupOptions()

            Using objReader = New clsPHRPReader(strFirstHitsFilePath, startupOptions)
                While objReader.MoveNext()

                    Dim objPSM As clsPSM
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
    ''' <param name="blnUsingMSGFOrEValueFilter">When true, then filter by MSGF or EValue, otherwise filter by FDR</param>
    ''' <param name="lstPSMs"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function FilterAndComputeStats(ByVal blnUsingMSGFOrEValueFilter As Boolean, ByRef lstPSMs As Dictionary(Of Integer, udtPSMInfoType)) As Boolean

        Dim lstFilteredPSMs As Dictionary(Of Integer, udtPSMInfoType)
        lstFilteredPSMs = New Dictionary(Of Integer, udtPSMInfoType)

        Dim blnSuccess As Boolean
        Dim blnFilterPSMs As Boolean = True

        If blnUsingMSGFOrEValueFilter Then
            If mResultType = clsPHRPReader.ePeptideHitResultType.MSAlign Then
                ' Filter on MSGF
                blnSuccess = FilterPSMsByEValue(mEValueThreshold, lstPSMs, lstFilteredPSMs)
            ElseIf mMSGFThreshold < 1 Then
                ' Filter on MSGF
                blnSuccess = FilterPSMsByMSGF(mMSGFThreshold, lstPSMs, lstFilteredPSMs)
            Else
                ' Do not filter
                blnFilterPSMs = False
            End If
        Else
            blnFilterPSMs = False
        End If

        If Not blnFilterPSMs Then
            ' Keep all PSMs
            For Each kvEntry As KeyValuePair(Of Integer, udtPSMInfoType) In lstPSMs
                lstFilteredPSMs.Add(kvEntry.Key, kvEntry.Value)
            Next
            blnSuccess = True
        End If

        If Not blnUsingMSGFOrEValueFilter AndAlso mFDRThreshold < 1 Then
            ' Filter on FDR (we'll compute the FDR using Reverse Proteins, if necessary)
            blnSuccess = FilterPSMsByFDR(lstFilteredPSMs)
        End If

        If blnSuccess Then
            Dim objReader As clsPHRPSeqMapReader
            Dim lstResultToSeqMap As SortedList(Of Integer, Integer) = Nothing
            Dim lstSeqToProteinMap As SortedList(Of Integer, List(Of clsProteinInfo)) = Nothing
            Dim lstSeqInfo As SortedList(Of Integer, clsSeqInfo) = Nothing

            ' Load the protein information and associate with the data in lstFilteredPSMs
            objReader = New clsPHRPSeqMapReader(mDatasetName, mWorkDir, mResultType)
            blnSuccess = objReader.GetProteinMapping(lstResultToSeqMap, lstSeqToProteinMap, lstSeqInfo)

            If blnSuccess Then
                ' Summarize the results, counting the number of peptides, unique peptides, and proteins
                blnSuccess = SummarizeResults(blnUsingMSGFOrEValueFilter, lstFilteredPSMs, lstResultToSeqMap, lstSeqToProteinMap)
            End If

        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Filter the data using mFDRThreshold
    ''' </summary>
    ''' <param name="lstPSMs"></param>
    ''' <returns>True if success; false if no reverse hits are present or if none of the data has MSGF values</returns>
    ''' <remarks></remarks>
    Protected Function FilterPSMsByFDR(ByRef lstPSMs As Dictionary(Of Integer, udtPSMInfoType)) As Boolean

        Dim lstResultIDtoFDRMap As Dictionary(Of Integer, Double)

        Dim blnFDRAlreadyComputed = True
        For Each kvEntry As KeyValuePair(Of Integer, udtPSMInfoType) In lstPSMs
            If kvEntry.Value.FDR < 0 Then
                blnFDRAlreadyComputed = False
                Exit For
            End If
        Next

        lstResultIDtoFDRMap = New Dictionary(Of Integer, Double)
        If blnFDRAlreadyComputed Then
            For Each kvEntry As KeyValuePair(Of Integer, udtPSMInfoType) In lstPSMs
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
            Dim lstMSGFtoResultIDMap As List(Of KeyValuePair(Of Double, Integer))
            lstMSGFtoResultIDMap = New List(Of KeyValuePair(Of Double, Integer))

            Dim blnValidMSGFOrEValue = False
            For Each kvEntry As KeyValuePair(Of Integer, udtPSMInfoType) In lstPSMs
                If kvEntry.Value.MSGF < UNKNOWN_MSGF_SPECPROB Then
                    lstMSGFtoResultIDMap.Add(New KeyValuePair(Of Double, Integer)(kvEntry.Value.MSGF, kvEntry.Key))
                    If kvEntry.Value.MSGF < 1 Then blnValidMSGFOrEValue = True
                Else
                    lstMSGFtoResultIDMap.Add(New KeyValuePair(Of Double, Integer)(kvEntry.Value.EValue, kvEntry.Key))
                    If kvEntry.Value.EValue < UNKNOWN_EVALUE Then blnValidMSGFOrEValue = True
                End If
            Next

            If Not blnValidMSGFOrEValue Then
                ' None of the data has MSGF values or E-Values; cannot compute FDR
                mErrorMessage = "Data does not contain MSGF values or EValues; cannot compute a decoy-based FDR"
                lstPSMs.Clear()
                Return False
            End If

            ' Sort lstMSGFtoResultIDMap
            lstMSGFtoResultIDMap.Sort(New clsMSGFtoResultIDMapComparer)

            Dim intForwardResults As Integer = 0
            Dim intDecoyResults As Integer = 0
            Dim strProtein As String
            Dim lstMissedResultIDsAtStart As List(Of Integer)
            lstMissedResultIDsAtStart = New List(Of Integer)

            For Each kvEntry As KeyValuePair(Of Double, Integer) In lstMSGFtoResultIDMap
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
                    Dim dblFDRThreshold = intDecoyResults / CDbl(intForwardResults)
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

        For Each kvEntry As KeyValuePair(Of Integer, Double) In lstResultIDtoFDRMap
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

    Protected Function FilterPSMsByEValue(
      ByVal dblEValueThreshold As Double,
      ByRef lstPSMs As Dictionary(Of Integer, udtPSMInfoType),
      ByRef lstFilteredPSMs As Dictionary(Of Integer, udtPSMInfoType)) As Boolean

        lstFilteredPSMs.Clear()

        Dim lstFilteredValues = From item In lstPSMs Where item.Value.EValue <= dblEValueThreshold

        For Each item In lstFilteredValues
            lstFilteredPSMs.Add(item.Key, item.Value)
        Next

        Return True

    End Function

    Protected Function FilterPSMsByMSGF(ByVal dblMSGFThreshold As Double, ByRef lstPSMs As Dictionary(Of Integer, udtPSMInfoType), ByRef lstFilteredPSMs As Dictionary(Of Integer, udtPSMInfoType)) As Boolean

        lstFilteredPSMs.Clear()

        Dim lstFilteredValues = From item In lstPSMs Where item.Value.MSGF <= dblMSGFThreshold

        For Each item In lstFilteredValues
            lstFilteredPSMs.Add(item.Key, item.Value)
        Next

        Return True

    End Function

    Protected Function PostJobPSMResults(ByVal intJob As Integer) As Boolean

        Const MAX_RETRY_COUNT As Integer = 3

        Dim objCommand As SqlCommand

        Dim blnSuccess As Boolean

        Try

            ' Call stored procedure StoreJobPSMStats in DMS5

            objCommand = New SqlCommand()

            With objCommand
                .CommandType = CommandType.StoredProcedure
                .CommandText = STORE_JOB_PSM_RESULTS_SP_NAME

                .Parameters.Add(New SqlParameter("@Return", SqlDbType.Int))
                .Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

                .Parameters.Add(New SqlParameter("@Job", SqlDbType.Int))
                .Parameters.Item("@Job").Direction = ParameterDirection.Input
                .Parameters.Item("@Job").Value = intJob

                .Parameters.Add(New SqlParameter("@MSGFThreshold", SqlDbType.Float))
                .Parameters.Item("@MSGFThreshold").Direction = ParameterDirection.Input

                If mResultType = clsPHRPReader.ePeptideHitResultType.MSAlign Then
                    .Parameters.Item("@MSGFThreshold").Value = mEValueThreshold
                Else
                    .Parameters.Item("@MSGFThreshold").Value = mMSGFThreshold
                End If

                .Parameters.Add(New SqlParameter("@FDRThreshold", SqlDbType.Float))
                .Parameters.Item("@FDRThreshold").Direction = ParameterDirection.Input
                .Parameters.Item("@FDRThreshold").Value = mFDRThreshold

                .Parameters.Add(New SqlParameter("@SpectraSearched", SqlDbType.Int))
                .Parameters.Item("@SpectraSearched").Direction = ParameterDirection.Input
                .Parameters.Item("@SpectraSearched").Value = mSpectraSearched

                .Parameters.Add(New SqlParameter("@TotalPSMs", SqlDbType.Int))
                .Parameters.Item("@TotalPSMs").Direction = ParameterDirection.Input
                .Parameters.Item("@TotalPSMs").Value = mMSGFBasedCounts.TotalPSMs

                .Parameters.Add(New SqlParameter("@UniquePeptides", SqlDbType.Int))
                .Parameters.Item("@UniquePeptides").Direction = ParameterDirection.Input
                .Parameters.Item("@UniquePeptides").Value = mMSGFBasedCounts.UniquePeptideCount

                .Parameters.Add(New SqlParameter("@UniqueProteins", SqlDbType.Int))
                .Parameters.Item("@UniqueProteins").Direction = ParameterDirection.Input
                .Parameters.Item("@UniqueProteins").Value = mMSGFBasedCounts.UniqueProteinCount

                .Parameters.Add(New SqlParameter("@TotalPSMsFDRFilter", SqlDbType.Int))
                .Parameters.Item("@TotalPSMsFDRFilter").Direction = ParameterDirection.Input
                .Parameters.Item("@TotalPSMsFDRFilter").Value = mFDRBasedCounts.TotalPSMs

                .Parameters.Add(New SqlParameter("@UniquePeptidesFDRFilter", SqlDbType.Int))
                .Parameters.Item("@UniquePeptidesFDRFilter").Direction = ParameterDirection.Input
                .Parameters.Item("@UniquePeptidesFDRFilter").Value = mFDRBasedCounts.UniquePeptideCount

                .Parameters.Add(New SqlParameter("@UniqueProteinsFDRFilter", SqlDbType.Int))
                .Parameters.Item("@UniqueProteinsFDRFilter").Direction = ParameterDirection.Input
                .Parameters.Item("@UniqueProteinsFDRFilter").Value = mFDRBasedCounts.UniqueProteinCount

                .Parameters.Add(New SqlParameter("@MSGFThresholdIsEValue", SqlDbType.TinyInt))
                .Parameters.Item("@MSGFThresholdIsEValue").Direction = ParameterDirection.Input
                If mResultType = clsPHRPReader.ePeptideHitResultType.MSAlign Then
                    .Parameters.Item("@MSGFThresholdIsEValue").Value = 1
                Else
                    .Parameters.Item("@MSGFThresholdIsEValue").Value = 0
                End If


            End With

            'Execute the SP (retry the call up to 3 times)
            Dim ResCode As Integer
            Dim strErrorMessage As String = String.Empty
            ResCode = mStoredProcedureExecutor.ExecuteSP(objCommand, MAX_RETRY_COUNT, strErrorMessage)

            If ResCode = 0 Then
                blnSuccess = True
            Else
                SetErrorMessage("Error storing PSM Results in database, " & STORE_JOB_PSM_RESULTS_SP_NAME & " returned " & ResCode.ToString)
                If Not String.IsNullOrEmpty(strErrorMessage) Then
                    mErrorMessage &= "; " & strErrorMessage
                End If
                blnSuccess = False
            End If

        Catch ex As Exception
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

        Dim blnUsingMSGFOrEValueFilter As Boolean

        Dim blnSuccess As Boolean
        Dim blnSuccessViaFDR As Boolean

        ' The keys in this dictionary are Result_ID values
        ' The values contain mapped protein name, FDR, and MSGF SpecProb
        ' We'll deal with multiple proteins for each peptide later when we parse the _ResultToSeqMap.txt and _SeqToProteinMap.txt files
        ' If those files are not found, then we'll simply use the protein information stored in lstPSMs
        Dim lstPSMs As Dictionary(Of Integer, udtPSMInfoType)

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

            If mResultType = clsPHRPReader.ePeptideHitResultType.XTandem Or
               mResultType = clsPHRPReader.ePeptideHitResultType.MSAlign Or
               mResultType = clsPHRPReader.ePeptideHitResultType.MODa Then
                ' These tools do not have first-hits files; use the Synopsis file instead to determine scan counts
                strPHRPFirstHitsFileName = strPHRPSynopsisFileName
            End If

            mMSGFSynopsisFileName = Path.GetFileNameWithoutExtension(strPHRPSynopsisFileName) & clsMSGFInputCreator.MSGF_RESULT_FILENAME_SUFFIX

            strPHRPFirstHitsFilePath = Path.Combine(mWorkDir, strPHRPFirstHitsFileName)
            strPHRPSynopsisFilePath = Path.Combine(mWorkDir, strPHRPSynopsisFileName)

            If Not File.Exists(strPHRPSynopsisFilePath) Then
                SetErrorMessage("File not found: " & strPHRPSynopsisFilePath)
                Return False
            End If

            '''''''''''''''''''''
            ' Determine the number of MS/MS spectra searched
            '
            If File.Exists(strPHRPFirstHitsFilePath) Then
                ExamineFirstHitsFile(strPHRPFirstHitsFilePath)
            End If

            ''''''''''''''''''''
            ' Load the PSMs
            '
            lstPSMs = New Dictionary(Of Integer, udtPSMInfoType)
            blnSuccess = LoadPSMs(strPHRPSynopsisFilePath, lstPSMs)


            ''''''''''''''''''''
            ' Filter on MSGF or EValue and compute the stats
            '
            blnUsingMSGFOrEValueFilter = True
            blnSuccess = FilterAndComputeStats(blnUsingMSGFOrEValueFilter, lstPSMs)

            ''''''''''''''''''''
            ' Filter on FDR and compute the stats
            '
            blnUsingMSGFOrEValueFilter = False
            blnSuccessViaFDR = FilterAndComputeStats(blnUsingMSGFOrEValueFilter, lstPSMs)

            If blnSuccess OrElse blnSuccessViaFDR Then
                If mSaveResultsToTextFile Then
                    ' Note: Continue processing even if this step fails
                    SaveResultsToFile()
                End If

                If mPostJobPSMResultsToDB Then
                    blnSuccess = PostJobPSMResults(mJob)
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

    Protected Function LoadPSMs(ByVal strPHRPSynopsisFilePath As String, ByVal lstPSMs As Dictionary(Of Integer, udtPSMInfoType)) As Boolean

        Dim dblSpecProb As Double = UNKNOWN_MSGF_SPECPROB
        Dim dblEValue As Double = UNKNOWN_EVALUE

        Dim blnSuccess As Boolean
        Dim udtPSMInfo As udtPSMInfoType

        Dim blnLoadMSGFResults = True

        Try

            If mResultType = clsPHRPReader.ePeptideHitResultType.MODa Then
                blnLoadMSGFResults = False
            End If

            Dim startupOptions As clsPHRPStartupOptions = clsMSGFInputCreator.GetMinimalMemoryPHRPStartupOptions()
            startupOptions.LoadMSGFResults = blnLoadMSGFResults

            Using objReader As New clsPHRPReader(strPHRPSynopsisFilePath, startupOptions)

                Do While objReader.MoveNext()

                    Dim objPSM As clsPSM
                    objPSM = objReader.CurrentPSM

                    Dim blnValid = False

                    If mResultType = clsPHRPReader.ePeptideHitResultType.MSAlign Then
                        ' Use the EValue reported by MSAlign

                        Dim strEValue = String.Empty
                        If objPSM.TryGetScore("EValue", strEValue) Then
                            blnValid = Double.TryParse(strEValue, dblEValue)
                        End If

                    ElseIf mResultType = clsPHRPReader.ePeptideHitResultType.MODa Then
                        ' MODa results don't have spectral probability, but they do have FDR
                        blnValid = True

                    Else
                        blnValid = Double.TryParse(objPSM.MSGFSpecProb, dblSpecProb)
                    End If

                    If blnValid Then

                        ' Store in lstPSMs
                        If Not lstPSMs.ContainsKey(objPSM.ResultID) Then

                            udtPSMInfo.Protein = objPSM.ProteinFirst
                            udtPSMInfo.MSGF = dblSpecProb
                            udtPSMInfo.EValue = dblEValue

                            If mResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Or
                               mResultType = clsPHRPReader.ePeptideHitResultType.MSAlign Then
                                udtPSMInfo.FDR = objPSM.GetScoreDbl(PHRPReader.clsPHRPParserMSGFDB.DATA_COLUMN_FDR, -1)
                                If udtPSMInfo.FDR < 0 Then
                                    udtPSMInfo.FDR = objPSM.GetScoreDbl(PHRPReader.clsPHRPParserMSGFDB.DATA_COLUMN_EFDR, -1)
                                End If

                            ElseIf mResultType = clsPHRPReader.ePeptideHitResultType.MODa Then
                                udtPSMInfo.FDR = objPSM.GetScoreDbl(PHRPReader.clsPHRPParserMODa.DATA_COLUMN_QValue, -1)

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
			strOutputFilePath = Path.Combine(strOutputFilePath, mDatasetName & "_PSM_Stats.txt")

			Using swOutFile As StreamWriter = New StreamWriter(New FileStream(strOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

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
	''' <param name="blnUsingMSGFOrEValueFilter"></param>
	''' <param name="lstFilteredPSMs"></param>
	''' <param name="lstResultToSeqMap"></param>
	''' <param name="lstSeqToProteinMap"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function SummarizeResults(ByVal blnUsingMSGFOrEValueFilter As Boolean, _
	  ByRef lstFilteredPSMs As Dictionary(Of Integer, udtPSMInfoType), _
	  ByRef lstResultToSeqMap As SortedList(Of Integer, Integer), _
	  ByRef lstSeqToProteinMap As SortedList(Of Integer, List(Of clsProteinInfo))) As Boolean

		' lstPSMs only contains the filter-passing results (keys are ResultID, values are the first protein for each ResultID)
		' Link up with lstResultToSeqMap to determine the unique number of filter-passing peptides
		' Link up with lstSeqToProteinMap to determine the unique number of proteins

		' The Keys in this dictionary are SeqID values; the values are observation count
		Dim lstUniqueSequences As Dictionary(Of Integer, Integer)

		' The Keys in this dictionary are protein names; the values are observation count
		Dim lstUniqueProteins As Dictionary(Of String, Integer)

		Dim intObsCount As Integer
		Dim lstProteins As List(Of clsProteinInfo) = Nothing

		Try
			lstUniqueSequences = New Dictionary(Of Integer, Integer)
			lstUniqueProteins = New Dictionary(Of String, Integer)

			For Each objResultID As KeyValuePair(Of Integer, udtPSMInfoType) In lstFilteredPSMs

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

						For Each objProtein As clsProteinInfo In lstProteins

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
			If blnUsingMSGFOrEValueFilter Then
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

#Region "Event Handlers"

    Private Sub m_ExecuteSP_DebugEvent(Message As String) Handles mStoredProcedureExecutor.DebugEvent
        Console.WriteLine(Message)
    End Sub

    Private Sub m_ExecuteSP_DBErrorEvent(Message As String) Handles mStoredProcedureExecutor.DBErrorEvent
        SetErrorMessage(Message)

        If Message.Contains("permission was denied") Then
            AnalysisManagerBase.clsLogTools.WriteLog(AnalysisManagerBase.clsLogTools.LoggerTypes.LogDb, AnalysisManagerBase.clsLogTools.LogLevels.ERROR, Message)
        End If        
    End Sub

#End Region

	Protected Class clsMSGFtoResultIDMapComparer
		Implements IComparer(Of KeyValuePair(Of Double, Integer))

		Public Function Compare(x As KeyValuePair(Of Double, Integer), y As KeyValuePair(Of Double, Integer)) As Integer Implements IComparer(Of KeyValuePair(Of Double, Integer)).Compare
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
