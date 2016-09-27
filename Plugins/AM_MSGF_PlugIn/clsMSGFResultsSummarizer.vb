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
Imports System.Runtime.InteropServices
Imports System.Text
Imports AnalysisManagerBase

Public Class clsMSGFResultsSummarizer

#Region "Constants and Enums"

    Public Const DEFAULT_MSGF_THRESHOLD As Double = 0.0000000001        ' 1E-10
    Public Const DEFAULT_EVALUE_THRESHOLD As Double = 0.0001            ' 1E-4   (only used when MSGF Scores are not available)
    Public Const DEFAULT_FDR_THRESHOLD As Double = 0.01                 ' 1% FDR

    Public Const DEFAULT_CONNECTION_STRING As String = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;"

    Private Const STORE_JOB_PSM_RESULTS_SP_NAME As String = "StoreJobPSMStats"

#End Region

#Region "Structures"

    Private Structure udtPSMStatsType
        ''' <summary>
        ''' Number of spectra with a match
        ''' </summary>
        ''' <remarks></remarks>
        Public TotalPSMs As Integer

        ''' <summary>
        ''' Number of distinct peptides
        ''' </summary>
        ''' <remarks>
        ''' For modified peptides, collapses peptides with the same sequence and same number of modifications
        ''' For example, peptides PEPT*IDES and PEPTIDES* are counted just once
        ''' Also, peptides PEPS*SS*IK and PEPS*S*SIK are counted just once
        ''' </remarks>
        Public UniquePeptideCount As Integer

        ''' <summary>
        ''' Number of distinct proteins
        ''' </summary>
        ''' <remarks></remarks>
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

    Private mDatasetScanStatsLookupError As Boolean
    Private mPostJobPSMResultsToDB As Boolean = False

    Private mSaveResultsToTextFile As Boolean = True
    Private mOutputFolderPath As String = String.Empty

    Private mSpectraSearched As Integer = 0

    ''' <summary>
    ''' Value between 0 and 100, indicating the percentage of the MS2 spectra with search results that are
    ''' more than 2 scans away from an adjacent spectrum
    ''' </summary>
    ''' <remarks></remarks>
    Private mPercentMSnScansNoPSM As Double = 0

    ''' <summary>
    ''' Maximum number of scans separating two MS2 spectra with search results
    ''' </summary>
    ''' <remarks></remarks>
    Private mMaximumScanGapAdjacentMSn As Integer = 0

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

    ''' <summary>
    ''' Dataset name
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks>
    ''' Used to contact DMS to lookup the total number of scans and total number of MSn scans
    ''' This information is used by 
    ''' </remarks>
    Public Property DatasetName As String

    ''' <summary>
    ''' Set this to false to disable contacting DMS to look up scan stats for the dataset
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks>When this is false, we cannot compute MaximumScanGapAdjacentMSn or PercentMSnScansNoPSM</remarks>
    Public Property ContactDatabase As Boolean

    Public ReadOnly Property DatasetScanStatsLookupError As Boolean
        Get
            Return mDatasetScanStatsLookupError
        End Get
    End Property

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

    Public ReadOnly Property MaximumScanGapAdjacentMSn As Integer
        Get
            Return mMaximumScanGapAdjacentMSn
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

    Public ReadOnly Property PercentMSnScansNoPSM As Double
        Get
            Return mPercentMSnScansNoPSM
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
    Public Sub New(eResultType As clsPHRPReader.ePeptideHitResultType, strDatasetName As String, intJob As Integer,
                   strSourceFolderPath As String)
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
    Public Sub New(eResultType As clsPHRPReader.ePeptideHitResultType, strDatasetName As String, intJob As Integer,
                   strSourceFolderPath As String, strConnectionString As String)
        mResultType = eResultType
        mDatasetName = strDatasetName
        mJob = intJob
        mWorkDir = strSourceFolderPath
        mConnectionString = strConnectionString

        mStoredProcedureExecutor = New PRISM.DataBase.clsExecuteDatabaseSP(mConnectionString)
        ContactDatabase = True
    End Sub

    Private Sub ExamineFirstHitsFile(strFirstHitsFilePath As String)

        Try

            ' Initialize the list that will be used to track the number of spectra searched
            ' Keys are Scan_Charge, values are Scan number
            Dim lstUniqueSpectra = New Dictionary(Of String, Integer)

            Dim startupOptions As clsPHRPStartupOptions = clsMSGFInputCreator.GetMinimalMemoryPHRPStartupOptions()

            Using objReader = New clsPHRPReader(strFirstHitsFilePath, startupOptions)
                While objReader.MoveNext()

                    Dim objPSM = objReader.CurrentPSM

                    If objPSM.Charge >= 0 Then
                        Dim strScanChargeCombo = objPSM.ScanNumber.ToString() & "_" & objPSM.Charge.ToString()

                        If Not lstUniqueSpectra.ContainsKey(strScanChargeCombo) Then
                            lstUniqueSpectra.Add(strScanChargeCombo, objPSM.ScanNumber)
                        End If

                    End If

                End While
            End Using

            mSpectraSearched = lstUniqueSpectra.Count

            ' Set these to defaults for now
            mMaximumScanGapAdjacentMSn = 0
            mPercentMSnScansNoPSM = 100

            If Not ContactDatabase Then
                Return
            End If

            Dim scanList = (From item In lstUniqueSpectra.Values.Distinct()).ToList()

            CheckForScanGaps(scanList)

            Return

        Catch ex As Exception
            SetErrorMessage(ex.Message)
            Return
        End Try
    End Sub

    Private Sub CheckForScanGaps(scanList As List(Of Integer))

        ' Look for scan range gaps in the spectra list
        ' The occurrence of large gaps indicates that a processing thread in MSGF+ crashed and the results may be incomplete
        scanList.Sort()

        Dim totalSpectra = 0
        Dim totalMSnSpectra = 0

        Dim success = LookupScanStats(totalSpectra, totalMSnSpectra)
        If Not success OrElse totalSpectra <= 0 Then
            mDatasetScanStatsLookupError = True
            Return
        End If

        Dim gapCount = 0
        mMaximumScanGapAdjacentMSn = 0

        For i = 1 To scanList.Count - 1
            Dim scanGap = scanList(i) - scanList(i - 1)
            If scanGap > 2 Then
                gapCount += 1
            End If

            If scanGap > mMaximumScanGapAdjacentMSn Then
                mMaximumScanGapAdjacentMSn = scanGap
            End If
        Next

        If totalMSnSpectra > 0 Then
            mPercentMSnScansNoPSM = (1 - scanList.Count / CDbl(totalMSnSpectra)) * 100.0
        Else
            ' Report 100% because we cannot accurately compute this value without knowing totalMSnSpectra
            mPercentMSnScansNoPSM = 100
        End If

        If totalSpectra > 0 Then
            ' Compare the last scan number seen to the total number of scans
            Dim scanGap = totalSpectra - scanList(scanList.Count - 1) - 1

            If scanGap > mMaximumScanGapAdjacentMSn Then
                mMaximumScanGapAdjacentMSn = scanGap
            End If

        End If
    End Sub

    ''' <summary>
    ''' Lookup the total scans and number of MS/MS scans for the dataset defined by property DatasetName
    ''' </summary>
    ''' <param name="totalSpectra"></param>
    ''' <param name="totalMSnSpectra"></param>
    ''' <returns></returns>
    ''' <remarks>True if success; false if an error, including if DatasetName is empty or if the dataset is not found in the database</remarks>
    Private Function LookupScanStats(<Out()> ByRef totalSpectra As Integer, <Out()> ByRef totalMSnSpectra As Integer) As Boolean
        totalSpectra = 0
        totalMSnSpectra = 0

        Try

            If String.IsNullOrEmpty(DatasetName) Then
                SetErrorMessage("Dataset name is empty; cannot lookup scan stats")
                Return False
            End If

            Dim queryScanStats = "" &
                " SELECT Scan_Count_Total, " &
                "        SUM(CASE WHEN Scan_Type LIKE '%MSn' THEN Scan_Count ELSE 0 END) AS ScanCountMSn" &
                " FROM V_Dataset_Scans_Export DSE" &
                " WHERE Dataset = '" & DatasetName & "'" &
                " GROUP BY Scan_Count_Total"

            Dim lstResults As List(Of List(Of String)) = Nothing
            Dim success = clsGlobal.GetQueryResults(queryScanStats, mConnectionString, lstResults,
                                                    "LookupScanStats_V_Dataset_Scans_Export")

            If success AndAlso lstResults.Count > 0 Then

                For Each resultRow In lstResults
                    Dim scanCountTotal = resultRow(0)
                    Dim scanCountMSn = resultRow(1)

                    If Not Integer.TryParse(scanCountTotal, totalSpectra) Then
                        success = False
                        Exit For
                    Else
                        Integer.TryParse(scanCountMSn, totalMSnSpectra)
                        Return True
                    End If
                Next

            End If

            Dim queryScanTotal = "" &
                " SELECT [Scan Count]" &
                " FROM V_Dataset_Export" &
                " WHERE Dataset = '" & DatasetName & "'"

            lstResults.Clear()
            success = clsGlobal.GetQueryResults(queryScanTotal, mConnectionString, lstResults,
                                                "LookupScanStats_V_Dataset_Export")

            If success AndAlso lstResults.Count > 0 Then

                For Each resultRow In lstResults
                    Dim scanCountTotal = resultRow(0)

                    Integer.TryParse(scanCountTotal, totalSpectra)
                    Return True
                Next

            End If

            SetErrorMessage("Dataset not found in the database; cannot retrieve scan counts: " & DatasetName)
            Return False

        Catch ex As Exception
            SetErrorMessage("Exception retrieving scan stats from the database: " & ex.Message)
            Return False
        End Try
    End Function

    ''' <summary>
    ''' Either filter by MSGF or filter by FDR, then update the stats
    ''' </summary>
    ''' <param name="blnUsingMSGFOrEValueFilter">When true, then filter by MSGF or EValue, otherwise filter by FDR</param>
    ''' <param name="lstNormalizedPSMs">PSM results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
    ''' <param name="lstSeqToProteinMap">Sequence to Protein map information (empty if the _resultToSeqMap file was not found)</param>
    ''' <param name="lstSeqInfo">Sequence information (empty if the _resultToSeqMap file was not found)</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function FilterAndComputeStats(
      blnUsingMSGFOrEValueFilter As Boolean,
      lstNormalizedPSMs As Dictionary(Of Integer, clsPSMInfo),
      lstSeqToProteinMap As SortedList(Of Integer, List(Of clsProteinInfo)),
      lstSeqInfo As SortedList(Of Integer, clsSeqInfo)) As Boolean

        Dim lstFilteredPSMs = New Dictionary(Of Integer, clsPSMInfo)

        Dim blnSuccess As Boolean
        Dim blnFilterPSMs = True

        ' Make sure .PassesFilter is false for all of the observations
        For Each kvEntry As KeyValuePair(Of Integer, clsPSMInfo) In lstNormalizedPSMs
            For Each observation In kvEntry.Value.Observations
                If observation.PassesFilter Then
                    observation.PassesFilter = False
                End If
            Next
        Next

        If blnUsingMSGFOrEValueFilter Then
            If mResultType = clsPHRPReader.ePeptideHitResultType.MSAlign Then
                ' Filter on EValue
                blnSuccess = FilterPSMsByEValue(mEValueThreshold, lstNormalizedPSMs, lstFilteredPSMs)
            ElseIf mMSGFThreshold < 1 Then
                ' Filter on MSGF (though for MSPathFinder we're using SpecEValue)
                blnSuccess = FilterPSMsByMSGF(mMSGFThreshold, lstNormalizedPSMs, lstFilteredPSMs)
            Else
                ' Do not filter
                blnFilterPSMs = False
            End If
        Else
            blnFilterPSMs = False
        End If

        If Not blnFilterPSMs Then
            ' Keep all PSMs
            For Each kvEntry As KeyValuePair(Of Integer, clsPSMInfo) In lstNormalizedPSMs
                For Each observation In kvEntry.Value.Observations
                    observation.PassesFilter = True
                Next
                lstFilteredPSMs.Add(kvEntry.Key, kvEntry.Value)
            Next
            blnSuccess = True
        End If

        If Not blnUsingMSGFOrEValueFilter AndAlso mFDRThreshold < 1 Then
            ' Filter on FDR (we'll compute the FDR using Reverse Proteins, if necessary)
            blnSuccess = FilterPSMsByFDR(lstFilteredPSMs)

            For Each entry In lstFilteredPSMs
                For Each observation In entry.Value.Observations
                    If observation.FDR > mFDRThreshold Then
                        observation.PassesFilter = False
                    End If
                Next
            Next
        End If

        If blnSuccess Then

            ' Summarize the results, counting the number of peptides, unique peptides, and proteins
            blnSuccess = SummarizeResults(blnUsingMSGFOrEValueFilter, lstFilteredPSMs, lstSeqToProteinMap, lstSeqInfo)

        End If

        Return blnSuccess
    End Function

    ''' <summary>
    ''' Filter the data using mFDRThreshold
    ''' </summary>
    ''' <param name="lstPSMs">PSM results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
    ''' <returns>True if success; false if no reverse hits are present or if none of the data has MSGF values</returns>
    ''' <remarks></remarks>
    Private Function FilterPSMsByFDR(lstPSMs As Dictionary(Of Integer, clsPSMInfo)) As Boolean

        Dim lstResultIDtoFDRMap As Dictionary(Of Integer, Double)

        Dim blnFDRAlreadyComputed = True
        For Each kvEntry As KeyValuePair(Of Integer, clsPSMInfo) In lstPSMs
            If kvEntry.Value.BestFDR < 0 Then
                blnFDRAlreadyComputed = False
                Exit For
            End If
        Next

        lstResultIDtoFDRMap = New Dictionary(Of Integer, Double)
        If blnFDRAlreadyComputed Then
            For Each kvEntry As KeyValuePair(Of Integer, clsPSMInfo) In lstPSMs
                lstResultIDtoFDRMap.Add(kvEntry.Key, kvEntry.Value.BestFDR)
            Next
        Else

            ' Sort the data by ascending SpecProb, then step through the list and compute FDR
            ' Use FDR = #Reverse / #Forward
            '
            ' Alternative FDR formula is:  FDR = 2 * #Reverse / (#Forward + #Reverse)
            ' But, since MSGF+ uses "#Reverse / #Forward" we'll use that here too
            '
            ' If no reverse hits are present or if none of the data has MSGF values, then we'll clear lstPSMs and update mErrorMessage			

            ' Populate a list with the MSGF values and ResultIDs so that we can step through the data and compute the FDR for each entry
            Dim lstMSGFtoResultIDMap = New List(Of KeyValuePair(Of Double, Integer))

            Dim blnValidMSGFOrEValue = False
            For Each kvEntry As KeyValuePair(Of Integer, clsPSMInfo) In lstPSMs
                If kvEntry.Value.BestMSGF < clsPSMInfo.UNKNOWN_MSGF_SPECPROB Then
                    lstMSGFtoResultIDMap.Add(New KeyValuePair(Of Double, Integer)(kvEntry.Value.BestMSGF, kvEntry.Key))
                    If kvEntry.Value.BestMSGF < 1 Then blnValidMSGFOrEValue = True
                Else
                    lstMSGFtoResultIDMap.Add(New KeyValuePair(Of Double, Integer)(kvEntry.Value.BestEValue, kvEntry.Key))
                    If kvEntry.Value.BestEValue < clsPSMInfo.UNKNOWN_EVALUE Then blnValidMSGFOrEValue = True
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

            Dim intForwardResults = 0
            Dim intDecoyResults = 0
            Dim strProtein As String
            Dim lstMissedResultIDsAtStart As List(Of Integer)
            lstMissedResultIDsAtStart = New List(Of Integer)

            For Each kvEntry As KeyValuePair(Of Double, Integer) In lstMSGFtoResultIDMap
                strProtein = lstPSMs(kvEntry.Value).Protein.ToLower()

                ' MTS reversed proteins                 'reversed[_]%'
                ' MTS scrambled proteins                'scrambled[_]%'
                ' X!Tandem decoy proteins               '%[:]reversed'
                ' Inspect reversed/scrambled proteins   'xxx.%'
                ' MSGFDB (aka MSGF+) reversed proteins  'rev[_]%'

                If strProtein.StartsWith("reversed_") OrElse
                   strProtein.StartsWith("scrambled_") OrElse
                   strProtein.EndsWith(":reversed") OrElse
                   strProtein.StartsWith("xxx.") OrElse
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

        For Each kvEntry As KeyValuePair(Of Integer, Double) In lstResultIDtoFDRMap
            If kvEntry.Value > mFDRThreshold Then
                lstPSMs.Remove(kvEntry.Key)
            End If
        Next

        Return True
    End Function

    Private Function FilterPSMsByEValue(
      dblEValueThreshold As Double,
      lstPSMs As Dictionary(Of Integer, clsPSMInfo),
                                        lstFilteredPSMs As Dictionary(Of Integer, clsPSMInfo)) As Boolean

        lstFilteredPSMs.Clear()

        Dim lstFilteredValues = From item In lstPSMs Where item.Value.BestEValue <= dblEValueThreshold

        For Each item In lstFilteredValues
            For Each observation In item.Value.Observations
                observation.PassesFilter = (observation.EValue <= dblEValueThreshold)
            Next
            lstFilteredPSMs.Add(item.Key, item.Value)
        Next

        Return True
    End Function

    Private Function FilterPSMsByMSGF(
      dblMSGFThreshold As Double,
      lstPSMs As Dictionary(Of Integer, clsPSMInfo),
                                      lstFilteredPSMs As Dictionary(Of Integer, clsPSMInfo)) As Boolean

        lstFilteredPSMs.Clear()

        Dim lstFilteredValues = From item In lstPSMs Where item.Value.BestMSGF <= dblMSGFThreshold

        For Each item In lstFilteredValues
            For Each observation In item.Value.Observations
                observation.PassesFilter = (observation.MSGF <= dblMSGFThreshold)
            Next
            lstFilteredPSMs.Add(item.Key, item.Value)
        Next

        Return True
    End Function

    Private Function GetNormalizedPeptide(peptideCleanSequence As String, modifications As String) As String
        Return peptideCleanSequence & "_" & modifications
    End Function

    Private Function PostJobPSMResults(intJob As Integer) As Boolean

        Const MAX_RETRY_COUNT = 3

        Dim blnSuccess As Boolean

        Try

            ' Call stored procedure StoreJobPSMStats in DMS5

            Dim objCommand = New SqlCommand(STORE_JOB_PSM_RESULTS_SP_NAME)
            With objCommand
                .CommandType = CommandType.StoredProcedure

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

                .Parameters.Add(New SqlParameter("@PercentMSnScansNoPSM", SqlDbType.Float))
                .Parameters.Item("@PercentMSnScansNoPSM").Direction = ParameterDirection.Input
                .Parameters.Item("@PercentMSnScansNoPSM").Value = mPercentMSnScansNoPSM

                .Parameters.Add(New SqlParameter("@MaximumScanGapAdjacentMSn", SqlDbType.Int))
                .Parameters.Item("@MaximumScanGapAdjacentMSn").Direction = ParameterDirection.Input
                .Parameters.Item("@MaximumScanGapAdjacentMSn").Value = mMaximumScanGapAdjacentMSn

            End With

            ' Execute the SP (retry the call up to 3 times)
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

        mDatasetScanStatsLookupError = False

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

            If mResultType = clsPHRPReader.ePeptideHitResultType.XTandem OrElse
               mResultType = clsPHRPReader.ePeptideHitResultType.MSAlign OrElse
               mResultType = clsPHRPReader.ePeptideHitResultType.MODa OrElse
               mResultType = clsPHRPReader.ePeptideHitResultType.MODPlus OrElse
               mResultType = clsPHRPReader.ePeptideHitResultType.MSPathFinder Then
                ' These tools do not have first-hits files; use the Synopsis file instead to determine scan counts
                strPHRPFirstHitsFileName = strPHRPSynopsisFileName
            End If

            mMSGFSynopsisFileName = Path.GetFileNameWithoutExtension(strPHRPSynopsisFileName) &
                                    clsMSGFInputCreator.MSGF_RESULT_FILENAME_SUFFIX

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
            ' Load the PSMs and sequence info
            '

            ' The keys in this dictionary are NormalizedSeqID values, which are custom-assigned 
            '   by this class to keep track of peptide sequences on a basis where modifications are tracked but not mapped to specific residues
            '   This is done so that peptides like PEPT*IDES and PEPTIDES* are counted as the same peptide
            ' The values contain mapped protein name, FDR, and MSGF SpecProb, and the scans that the normalized peptide was observed in
            ' We'll deal with multiple proteins for each peptide later when we parse the _ResultToSeqMap.txt and _SeqToProteinMap.txt files
            ' If those files are not found, then we'll simply use the protein information stored in lstPSMs
            Dim lstNormalizedPSMs = New Dictionary(Of Integer, clsPSMInfo)

            Dim lstResultToSeqMap = New SortedList(Of Integer, Integer)

            Dim lstSeqToProteinMap = New SortedList(Of Integer, List(Of clsProteinInfo))

            Dim lstSeqInfo = New SortedList(Of Integer, clsSeqInfo)

            blnSuccess = LoadPSMs(strPHRPSynopsisFilePath, lstNormalizedPSMs, lstResultToSeqMap, lstSeqToProteinMap,
                                  lstSeqInfo)
            If Not blnSuccess Then
                Return False
            End If

            ''''''''''''''''''''
            ' Filter on MSGF or EValue and compute the stats
            '
            blnUsingMSGFOrEValueFilter = True
            blnSuccess = FilterAndComputeStats(blnUsingMSGFOrEValueFilter, lstNormalizedPSMs, lstSeqToProteinMap,
                                               lstSeqInfo)

            ''''''''''''''''''''
            ' Filter on FDR and compute the stats
            '
            blnUsingMSGFOrEValueFilter = False
            blnSuccessViaFDR = FilterAndComputeStats(blnUsingMSGFOrEValueFilter, lstNormalizedPSMs, lstSeqToProteinMap,
                                                     lstSeqInfo)

            If blnSuccess OrElse blnSuccessViaFDR Then
                If mSaveResultsToTextFile Then
                    ' Note: Continue processing even if this step fails
                    SaveResultsToFile()
                End If

                If mPostJobPSMResultsToDB Then
                    If ContactDatabase Then
                        blnSuccess = PostJobPSMResults(mJob)
                        Return blnSuccess
                    Else
                        SetErrorMessage("Cannot post results to the database because ContactDatabase is False")
                        Return False
                    End If
                End If

                blnSuccess = True
            End If

            Return blnSuccess

        Catch ex As Exception
            SetErrorMessage(ex.Message)
            Return False
        End Try
    End Function

    ''' <summary>
    ''' Loads the PSMs (peptide identification for each scan)
    ''' Normalizes the peptide sequence (mods are tracked, but no longer associated with specific residues) and populates lstNormalizedPSMs
    ''' </summary>
    ''' <param name="strPHRPSynopsisFilePath"></param>
    ''' <param name="lstNormalizedPSMs"></param>
    ''' <param name="lstResultToSeqMap"></param>
    ''' <param name="lstSeqToProteinMap"></param>
    ''' <param name="lstSeqInfo"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function LoadPSMs(
      strPHRPSynopsisFilePath As String,
      lstNormalizedPSMs As Dictionary(Of Integer, clsPSMInfo),
      <Out()> ByRef lstResultToSeqMap As SortedList(Of Integer, Integer),
      <Out()> ByRef lstSeqToProteinMap As SortedList(Of Integer, List(Of clsProteinInfo)),
      <Out()> ByRef lstSeqInfo As SortedList(Of Integer, clsSeqInfo)) As Boolean

        Dim dblSpecProb As Double = clsPSMInfo.UNKNOWN_MSGF_SPECPROB
        Dim dblEValue As Double = clsPSMInfo.UNKNOWN_EVALUE

        Dim blnSuccess As Boolean

        Dim blnLoadMSGFResults = True

        lstResultToSeqMap = New SortedList(Of Integer, Integer)
        lstSeqToProteinMap = New SortedList(Of Integer, List(Of clsProteinInfo))
        lstSeqInfo = New SortedList(Of Integer, clsSeqInfo)

        Try

            If mResultType = clsPHRPReader.ePeptideHitResultType.MODa OrElse
               mResultType = clsPHRPReader.ePeptideHitResultType.MODPlus OrElse
               mResultType = clsPHRPReader.ePeptideHitResultType.MSPathFinder Then
                blnLoadMSGFResults = False
            End If

            Dim startupOptions As clsPHRPStartupOptions = clsMSGFInputCreator.GetMinimalMemoryPHRPStartupOptions()
            startupOptions.LoadMSGFResults = blnLoadMSGFResults

            ' Load the result to sequence mapping, sequence IDs, and protein information 
            Dim objSeqMapReader = New clsPHRPSeqMapReader(mDatasetName, mWorkDir, mResultType)

            Dim sequenceInfoAvailable = False

            If Not String.IsNullOrEmpty(objSeqMapReader.ResultToSeqMapFilename) Then

                Dim fiResultToSeqMapFile = New FileInfo(Path.Combine(objSeqMapReader.InputFolderPath,
                                                                     objSeqMapReader.ResultToSeqMapFilename))
                If fiResultToSeqMapFile.Exists Then

                    blnSuccess = objSeqMapReader.GetProteinMapping(lstResultToSeqMap, lstSeqToProteinMap, lstSeqInfo)

                    If Not blnSuccess Then
                        If String.IsNullOrEmpty(objSeqMapReader.ErrorMessage) Then
                            SetErrorMessage("GetProteinMapping returned false: unknown error")
                        Else
                            SetErrorMessage("GetProteinMapping returned false: " & objSeqMapReader.ErrorMessage)
                        End If

                        Return False
                    End If

                    sequenceInfoAvailable = True

                End If

            End If


            ' Keys in this dictionary are normalized peptide sequences (mod symbols have been moved to the end)
            '   For example, PEPT*IDES and PEPTIDES* are each normalized to PEPTIDES_**
            '   And ... P#EPT*IDES and PEP#T*IDES and P#EPTIDES* all become PEPTIDES_#*
            '   If sequenceInfoAvailable is True, then instead of using mod symbols we use ModNames from the Mod_Description column in the _SeqInfo.txt file
            '   For example, VGVEASEETPQT_Phosph or AGEPNSPDAEEANSPDVTAGCDPAGVHPPR_PhosphIodoAcet
            ' Values are the SeqID value of the first sequence to get normalized to the given peptide
            '   If sequenceInfoAvailable is False, then values are the ResultID value of the first peptide to get normalized to the given peptide
            Dim lstNormalizedPeptides = New Dictionary(Of String, Integer)

            Using objReader As New clsPHRPReader(strPHRPSynopsisFilePath, startupOptions)

                Do While objReader.MoveNext()

                    Dim objPSM As clsPSM = objReader.CurrentPSM

                    If objPSM.ScoreRank > 1 Then
                        ' Only keep the first match for each spectrum
                        Continue Do
                    End If

                    Dim blnValid = False

                    If mResultType = clsPHRPReader.ePeptideHitResultType.MSAlign Then
                        ' Use the EValue reported by MSAlign

                        Dim strEValue = String.Empty
                        If objPSM.TryGetScore("EValue", strEValue) Then
                            blnValid = Double.TryParse(strEValue, dblEValue)
                        End If

                    ElseIf mResultType = clsPHRPReader.ePeptideHitResultType.MODa Or
                           mResultType = clsPHRPReader.ePeptideHitResultType.MODPlus Then

                        ' MODa / MODPlus results don't have spectral probability, but they do have FDR
                        blnValid = True

                    ElseIf mResultType = clsPHRPReader.ePeptideHitResultType.MSPathFinder Then
                        ' Use SpecEValue in place of SpecProb
                        blnValid = True

                        Dim strSpecEValue = String.Empty
                        If objPSM.TryGetScore(clsPHRPParserMSPathFinder.DATA_COLUMN_SpecEValue, strSpecEValue) Then
                            If Not String.IsNullOrWhiteSpace(strSpecEValue) Then
                                blnValid = Double.TryParse(strSpecEValue, dblSpecProb)
                            End If
                        Else
                            ' SpecEValue was not present
                            ' That's OK, QValue should be present
                        End If

                    Else
                        blnValid = Double.TryParse(objPSM.MSGFSpecProb, dblSpecProb)
                    End If

                    If Not blnValid Then
                        Continue Do
                    End If

                    ' Store in lstNormalizedPSMs

                    Dim psmInfo As New clsPSMInfo
                    psmInfo.Clear()

                    psmInfo.Protein = objPSM.ProteinFirst
                    Dim psmMSGF = dblSpecProb
                    Dim psmEValue = dblEValue
                    Dim psmFDR As Double

                    If mResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Or
                       mResultType = clsPHRPReader.ePeptideHitResultType.MSAlign Then

                        psmFDR = objPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_FDR, clsPSMInfo.UNKNOWN_FDR)
                        If psmFDR < 0 Then
                            psmFDR = objPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_EFDR, clsPSMInfo.UNKNOWN_FDR)
                        End If

                    ElseIf mResultType = clsPHRPReader.ePeptideHitResultType.MODa Then
                        psmFDR = objPSM.GetScoreDbl(clsPHRPParserMODa.DATA_COLUMN_QValue, clsPSMInfo.UNKNOWN_FDR)

                    ElseIf mResultType = clsPHRPReader.ePeptideHitResultType.MODPlus Then
                        psmFDR = objPSM.GetScoreDbl(clsPHRPParserMODPlus.DATA_COLUMN_QValue, clsPSMInfo.UNKNOWN_FDR)

                    ElseIf mResultType = clsPHRPReader.ePeptideHitResultType.MSPathFinder Then
                        psmFDR = objPSM.GetScoreDbl(clsPHRPParserMSPathFinder.DATA_COLUMN_QValue, clsPSMInfo.UNKNOWN_FDR)

                    Else
                        psmFDR = clsPSMInfo.UNKNOWN_FDR
                    End If

                    Dim normalizedSequence As String = String.Empty
                    Dim normalized = False
                    Dim intSeqID = clsPSMInfo.UNKNOWN_SEQID

                    If sequenceInfoAvailable And Not lstResultToSeqMap Is Nothing Then

                        If Not lstResultToSeqMap.TryGetValue(objPSM.ResultID, intSeqID) Then
                            intSeqID = clsPSMInfo.UNKNOWN_SEQID

                            ' This result is not listed in the _ResultToSeqMap file, likely because it was already processed for this scan
                            ' Look for a match in lstNormalizedPeptides that starts with this peptide's clean sesquence
                            Dim comparisonSequence = GetNormalizedPeptide(objPSM.PeptideCleanSequence, String.Empty)
                            Dim query = From item In lstNormalizedPeptides Where item.Key.StartsWith(comparisonSequence) Select item

                            For Each result In query
                                ' Match found; use the given SeqID value
                                intSeqID = result.Value
                                Exit For
                            Next
                        End If

                        If intSeqID <> clsPSMInfo.UNKNOWN_SEQID Then
                            Dim oSeqInfo As clsSeqInfo = Nothing
                            If lstSeqInfo.TryGetValue(intSeqID, oSeqInfo) Then
                                normalizedSequence = NormalizeSequence(objPSM.PeptideCleanSequence, oSeqInfo)
                                normalized = True
                            End If
                        End If

                    End If

                    If Not normalized Then
                        normalizedSequence = NormalizeSequence(objPSM.Peptide)
                    End If

                    Dim normalizedSeqID As Integer
                    If lstNormalizedPeptides.TryGetValue(normalizedSequence, normalizedSeqID) Then
                        Dim normalizedPSM = lstNormalizedPSMs(normalizedSeqID)
                        Dim addObservation = True

                        For Each observation In normalizedPSM.Observations
                            If observation.Scan = objPSM.ScanNumber Then
                                ' Scan already stored

                                ' Update the scores if this PSM has a better score than the cached one
                                If psmFDR > clsPSMInfo.UNKNOWN_FDR Then
                                    If psmFDR < observation.FDR Then
                                        observation.FDR = psmFDR
                                    End If
                                End If

                                If psmMSGF < observation.MSGF Then
                                    observation.MSGF = psmMSGF
                                End If

                                If psmEValue < observation.EValue Then
                                    observation.EValue = psmEValue
                                End If

                                addObservation = False
                                Exit For

                            End If
                        Next

                        If addObservation Then
                            Dim observation = New clsPSMInfo.PSMObservation()

                            observation.Scan = objPSM.ScanNumber
                            observation.FDR = psmFDR
                            observation.MSGF = psmMSGF
                            observation.EValue = psmEValue

                            normalizedPSM.Observations.Add(observation)
                        End If

                    Else

                        If intSeqID = clsPSMInfo.UNKNOWN_SEQID Then
                            intSeqID = objPSM.ResultID
                        End If

                        lstNormalizedPeptides.Add(normalizedSequence, intSeqID)

                        psmInfo.SeqIdFirst = intSeqID

                        Dim observation = New clsPSMInfo.PSMObservation()

                        observation.Scan = objPSM.ScanNumber
                        observation.FDR = psmFDR
                        observation.MSGF = psmMSGF
                        observation.EValue = psmEValue

                        psmInfo.AddObservation(observation)

                        If lstNormalizedPSMs.ContainsKey(intSeqID) Then
                            Console.WriteLine("Warning: Duplicate key, intSeqID=" & intSeqID & "; skipping PSM with ResultID=" & objPSM.ResultID)
                        Else
                            lstNormalizedPSMs.Add(intSeqID, psmInfo)
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

    Private Function NormalizeSequence(sequenceWithMods As String) As String

        Dim sbAminoAcids = New StringBuilder(sequenceWithMods.Length)
        Dim sbModifications = New StringBuilder()

        Dim strPrefix = String.Empty
        Dim strSuffix = String.Empty
        Dim strPrimarySequence = String.Empty

        clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(sequenceWithMods, strPrimarySequence,
                                                                           strPrefix, strSuffix)

        For index = 0 To strPrimarySequence.Length - 1
            If clsPHRPReader.IsLetterAtoZ(strPrimarySequence(index)) Then
                sbAminoAcids.Append(strPrimarySequence(index))
            Else
                sbModifications.Append(strPrimarySequence(index))
            End If
        Next

        Return GetNormalizedPeptide(sbAminoAcids.ToString(), sbModifications.ToString())
    End Function

    Private Function NormalizeSequence(peptideCleanSequence As String, oSeqInfo As clsSeqInfo) As String

        Dim sbModifications = New StringBuilder()

        Dim lstMods = oSeqInfo.ModDescription.Split(","c)
        For Each modDescriptor In lstMods
            Dim colonIndex = modDescriptor.IndexOf(":"c)
            If colonIndex > 0 Then
                sbModifications.Append(modDescriptor.Substring(0, colonIndex))
            Else
                sbModifications.Append(modDescriptor)
            End If
        Next

        Return GetNormalizedPeptide(peptideCleanSequence, sbModifications.ToString())
    End Function

    Private Sub SaveResultsToFile()

        Dim strOutputFilePath = "??"

        Try
            If Not String.IsNullOrEmpty(mOutputFolderPath) Then
                strOutputFilePath = mOutputFolderPath
            Else
                strOutputFilePath = mWorkDir
            End If
            strOutputFilePath = Path.Combine(strOutputFilePath, mDatasetName & "_PSM_Stats.txt")

            Using swOutFile = New StreamWriter(New FileStream(strOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                ' Header line
                swOutFile.WriteLine(
                  "Dataset" & ControlChars.Tab &
                  "Job" & ControlChars.Tab &
                  "MSGF_Threshold" & ControlChars.Tab &
                  "FDR_Threshold" & ControlChars.Tab &
                  "Spectra_Searched" & ControlChars.Tab &
                  "Total_PSMs_MSGF_Filtered" & ControlChars.Tab &
                  "Unique_Peptides_MSGF_Filtered" & ControlChars.Tab &
                  "Unique_Proteins_MSGF_Filtered" & ControlChars.Tab &
                  "Total_PSMs_FDR_Filtered" & ControlChars.Tab &
                  "Unique_Peptides_FDR_Filtered" & ControlChars.Tab &
                  "Unique_Proteins_FDR_Filtered")

                ' Stats
                swOutFile.WriteLine(
                 mDatasetName & ControlChars.Tab &
                 mJob & ControlChars.Tab &
                 mMSGFThreshold.ToString("0.00E+00") & ControlChars.Tab &
                 mFDRThreshold.ToString("0.000") & ControlChars.Tab &
                 mSpectraSearched & ControlChars.Tab &
                 mMSGFBasedCounts.TotalPSMs & ControlChars.Tab &
                 mMSGFBasedCounts.UniquePeptideCount & ControlChars.Tab &
                 mMSGFBasedCounts.UniqueProteinCount & ControlChars.Tab &
                 mFDRBasedCounts.TotalPSMs & ControlChars.Tab &
                 mFDRBasedCounts.UniquePeptideCount & ControlChars.Tab &
                 mFDRBasedCounts.UniqueProteinCount)

            End Using

        Catch ex As Exception
            SetErrorMessage("Exception saving results to " & strOutputFilePath & ": " & ex.Message)
            Return
        End Try

        Return
    End Sub

    Private Sub SetErrorMessage(strMessage As String)
        Console.WriteLine(strMessage)
        mErrorMessage = strMessage
    End Sub

    ''' <summary>
    ''' Summarize the results by inter-relating lstFilteredPSMs, lstResultToSeqMap, and lstSeqToProteinMap
    ''' </summary>
    ''' <param name="blnUsingMSGFOrEValueFilter"></param>
    ''' <param name="lstFilteredPSMs">Filter-passing results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
    ''' <param name="lstSeqToProteinMap">Sequence to protein map (keys are sequence ID, values are proteins)</param>
    ''' <param name="lstSeqInfo">Sequence information (keys are sequence ID, values are sequences</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function SummarizeResults(
      blnUsingMSGFOrEValueFilter As Boolean,
      lstFilteredPSMs As Dictionary(Of Integer, clsPSMInfo),
      lstSeqToProteinMap As SortedList(Of Integer, List(Of clsProteinInfo)),
      lstSeqInfo As SortedList(Of Integer, clsSeqInfo)) As Boolean

        Try
            ' The Keys in this dictionary are SeqID values; the values are observation count
            Dim lstUniqueSequences = New Dictionary(Of Integer, Integer)

            ' The Keys in this dictionary are protein names; the values are observation count
            Dim lstUniqueProteins = New Dictionary(Of String, Integer)

            For Each result As KeyValuePair(Of Integer, clsPSMInfo) In lstFilteredPSMs

                Dim observations = result.Value.Observations
                Dim obsCountForResult = (From item In observations Where item.PassesFilter Select item).Count

                If obsCountForResult = 0 Then
                    Continue For
                End If

                ' If lstResultToSeqMap has data, the keys in lstFilteredPSMs are SeqID values
                ' Otherwise, the keys are ResultID values
                Dim intSeqID As Integer = result.Key

                Dim obsCountOverall As Integer
                If lstUniqueSequences.TryGetValue(intSeqID, obsCountOverall) Then
                    lstUniqueSequences(intSeqID) = obsCountOverall + obsCountForResult
                Else
                    lstUniqueSequences.Add(intSeqID, obsCountForResult)
                End If

                Dim addResultProtein = True

                Dim oSeqInfo As clsSeqInfo = Nothing
                If lstSeqInfo.Count > 0 AndAlso lstSeqInfo.TryGetValue(intSeqID, oSeqInfo) Then

                    ' Lookup the proteins for this peptide
                    Dim lstProteins As List(Of clsProteinInfo) = Nothing
                    If lstSeqToProteinMap.TryGetValue(intSeqID, lstProteins) Then
                        ' Update the observation count for each protein

                        For Each objProtein As clsProteinInfo In lstProteins

                            If lstUniqueProteins.TryGetValue(objProtein.ProteinName, obsCountOverall) Then
                                lstUniqueProteins(objProtein.ProteinName) = obsCountOverall + obsCountForResult
                            Else
                                lstUniqueProteins.Add(objProtein.ProteinName, obsCountForResult)
                            End If

                            ' Protein match found; we can ignore result.Value.Protein
                            addResultProtein = False
                        Next

                    End If

                End If

                If addResultProtein Then
                    Dim proteinName = result.Value.Protein

                    If lstUniqueProteins.TryGetValue(proteinName, obsCountOverall) Then
                        lstUniqueProteins(proteinName) = obsCountOverall + obsCountForResult
                    Else
                        lstUniqueProteins.Add(proteinName, obsCountForResult)
                    End If
                End If

            Next

            ' Store the stats
            If blnUsingMSGFOrEValueFilter Then
                mMSGFBasedCounts.TotalPSMs = (From item In lstUniqueSequences Select item.Value).Sum()
                mMSGFBasedCounts.UniquePeptideCount = lstUniqueSequences.Count
                mMSGFBasedCounts.UniqueProteinCount = lstUniqueProteins.Count
            Else
                mFDRBasedCounts.TotalPSMs = (From item In lstUniqueSequences Select item.Value).Sum()
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
            AnalysisManagerBase.clsLogTools.WriteLog(AnalysisManagerBase.clsLogTools.LoggerTypes.LogDb,
                                                     AnalysisManagerBase.clsLogTools.LogLevels.ERROR, Message)
        End If
    End Sub

#End Region

    Private Class clsMSGFtoResultIDMapComparer
        Implements IComparer(Of KeyValuePair(Of Double, Integer))

        Public Function Compare(x As KeyValuePair(Of Double, Integer), y As KeyValuePair(Of Double, Integer)) As Integer Implements IComparer(Of KeyValuePair(Of Double, Integer)).Compare
            If x.Key < y.Key Then
                Return - 1
            ElseIf x.Key > y.Key Then
                Return 1
            Else
                Return 0
            End If
        End Function
    End Class
End Class
