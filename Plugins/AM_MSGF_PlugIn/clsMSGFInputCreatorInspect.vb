
'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
' This class reads an Inspect _inspect_syn.txt file in support of creating the input file for MSGF 
'
'*********************************************************************************************************

Option Strict On

Public Class clsMSGFInputCreatorInspect
    Inherits clsMSGFInputCreator

    Public Const DATA_COLUMN_ResultID As String = "ResultID"
    Public Const DATA_COLUMN_Scan As String = "Scan"
    Public Const DATA_COLUMN_Peptide As String = "Peptide"
    Public Const DATA_COLUMN_Protein As String = "Protein"
    Public Const DATA_COLUMN_Charge As String = "Charge"
    Public Const DATA_COLUMN_MQScore As String = "MQScore"
    Public Const DATA_COLUMN_Length As String = "Length"
    Public Const DATA_COLUMN_TotalPRMScore As String = "TotalPRMScore"
    Public Const DATA_COLUMN_MedianPRMScore As String = "MedianPRMScore"
    Public Const DATA_COLUMN_FractionY As String = "FractionY"
    Public Const DATA_COLUMN_FractionB As String = "FractionB"
    Public Const DATA_COLUMN_Intensity As String = "Intensity"
    Public Const DATA_COLUMN_NTT As String = "NTT"
    Public Const DATA_COLUMN_PValue As String = "PValue"
    Public Const DATA_COLUMN_FScore As String = "FScore"
    Public Const DATA_COLUMN_DeltaScore As String = "DeltaScore"
    Public Const DATA_COLUMN_DeltaScoreOther As String = "DeltaScoreOther"
    Public Const DATA_COLUMN_DeltaNormMQScore As String = "DeltaNormMQScore"
    Public Const DATA_COLUMN_DeltaNormTotalPRMScore As String = "DeltaNormTotalPRMScore"
    Public Const DATA_COLUMN_RankTotalPRMScore As String = "RankTotalPRMScore"
    Public Const DATA_COLUMN_RankFScore As String = "RankFScore"
    Public Const DATA_COLUMN_MH As String = "MH"
    Public Const DATA_COLUMN_RecordNumber As String = "RecordNumber"
    Public Const DATA_COLUMN_DBFilePos As String = "DBFilePos"
    Public Const DATA_COLUMN_SpecFilePos As String = "SpecFilePos"
    Public Const DATA_COLUMN_PrecursorMZ As String = "PrecursorMZ"
    Public Const DATA_COLUMN_PrecursorError As String = "PrecursorError"

    Public Sub New(ByVal strDatasetName As String, _
                   ByVal strWorkDir As String, _
                   ByRef objDynamicMods As System.Collections.Generic.SortedDictionary(Of String, String), _
                   ByRef objStaticMods As System.Collections.Generic.SortedDictionary(Of String, String))

        MyBase.New(strDatasetName, strWorkDir, objDynamicMods, objStaticMods)

    End Sub

    Protected Overrides Sub DefineColumnHeaders()

        mColumnHeaders.Clear()

        ' Define the default column mapping
        mColumnHeaders.Add(DATA_COLUMN_ResultID, 0)
        mColumnHeaders.Add(DATA_COLUMN_Scan, 1)
        mColumnHeaders.Add(DATA_COLUMN_Peptide, 2)
        mColumnHeaders.Add(DATA_COLUMN_Protein, 3)
        mColumnHeaders.Add(DATA_COLUMN_Charge, 4)
        mColumnHeaders.Add(DATA_COLUMN_MQScore, 5)
        mColumnHeaders.Add(DATA_COLUMN_Length, 6)
        mColumnHeaders.Add(DATA_COLUMN_TotalPRMScore, 7)
        mColumnHeaders.Add(DATA_COLUMN_MedianPRMScore, 8)
        mColumnHeaders.Add(DATA_COLUMN_FractionY, 9)
        mColumnHeaders.Add(DATA_COLUMN_FractionB, 10)
        mColumnHeaders.Add(DATA_COLUMN_Intensity, 11)
        mColumnHeaders.Add(DATA_COLUMN_NTT, 12)
        mColumnHeaders.Add(DATA_COLUMN_PValue, 13)
        mColumnHeaders.Add(DATA_COLUMN_FScore, 14)
        mColumnHeaders.Add(DATA_COLUMN_DeltaScore, 15)
        mColumnHeaders.Add(DATA_COLUMN_DeltaScoreOther, 16)
        mColumnHeaders.Add(DATA_COLUMN_DeltaNormMQScore, 17)
        mColumnHeaders.Add(DATA_COLUMN_DeltaNormTotalPRMScore, 18)
        mColumnHeaders.Add(DATA_COLUMN_RankTotalPRMScore, 19)
        mColumnHeaders.Add(DATA_COLUMN_RankFScore, 20)
        mColumnHeaders.Add(DATA_COLUMN_MH, 21)
        mColumnHeaders.Add(DATA_COLUMN_RecordNumber, 22)
        mColumnHeaders.Add(DATA_COLUMN_DBFilePos, 23)
        mColumnHeaders.Add(DATA_COLUMN_SpecFilePos, 24)
        mColumnHeaders.Add(DATA_COLUMN_PrecursorMZ, 25)
        mColumnHeaders.Add(DATA_COLUMN_PrecursorError, 26)

    End Sub

    Public Shared Function GetPHRPFirstHitsFileName(ByVal strDatasetName As String) As String
        Return strDatasetName & "_inspect_fht.txt"
    End Function

    Public Shared Function GetPHRPSynopsisFileName(ByVal strDatasetName As String) As String
        Return strDatasetName & "_inspect_syn.txt"
    End Function

    Protected Overrides Sub InitializeFilePaths()

        ' Customize mPHRPResultFilePath for Inspect synopsis files
        mPHRPFirstHitsFilePath = System.IO.Path.Combine(mWorkDir, GetPHRPFirstHitsFileName(mDatasetName))
        mPHRPSynopsisFilePath = System.IO.Path.Combine(mWorkDir, GetPHRPSynopsisFileName(mDatasetName))

        UpdateMSGFInputOutputFilePaths()

    End Sub

    Protected Overrides Function ParsePHRPDataLine(ByVal intLineNumber As Integer, _
                                                   ByRef strPHRPSource As String, _
                                                   ByRef strColumns() As String, _
                                                   ByRef udtPHRPData As udtPHRPDataLine) As Boolean
        Dim dblPValue As Double
        Dim dblTotalPRMScore As Double
        Dim dblFScore As Double

        Dim intCleavageState As Short
        Dim blnIsProteinTerminus As Boolean

        Dim blnSuccess As Boolean

        Try

            udtPHRPData.Clear()

            With udtPHRPData
                .Title = String.Empty

                .ScanNumber = LookupColumnValue(strColumns, DATA_COLUMN_Scan, mColumnHeaders, -100)
                If .ScanNumber = -100 Then
                    ' Data line is not valid
                Else
                    .Peptide = LookupColumnValue(strColumns, DATA_COLUMN_Peptide, mColumnHeaders)
                    .Charge = CType(LookupColumnValue(strColumns, DATA_COLUMN_Charge, mColumnHeaders, 0), Short)
                    .ProteinFirst = LookupColumnValue(strColumns, DATA_COLUMN_Protein, mColumnHeaders)
                    .ResultID = LookupColumnValue(strColumns, DATA_COLUMN_ResultID, mColumnHeaders, 0)

                    dblPValue = LookupColumnValue(strColumns, DATA_COLUMN_PValue, mColumnHeaders, CDbl(1))
                    dblTotalPRMScore = LookupColumnValue(strColumns, DATA_COLUMN_TotalPRMScore, mColumnHeaders, CDbl(0))
                    dblFScore = LookupColumnValue(strColumns, DATA_COLUMN_FScore, mColumnHeaders, CDbl(-1))

                    intCleavageState = CType(LookupColumnValue(strColumns, DATA_COLUMN_NTT, mColumnHeaders, 0), Short)

                    ' This will be updated to true below if the peptide passes the filters
                    .PassesFilters = False

                    blnSuccess = True
                End If
            End With

            If blnSuccess Then
                ' Examine the score values and possibly filter out this line

                ' Keep Inspect results with pValue <= 0.2 Or TotalPRMScore >= 50 or FScore >= 0
                ' PHRP has likely already filtered the _inspect_syn.txt file using these filters

                If udtPHRPData.Peptide.StartsWith("-"c) OrElse udtPHRPData.Peptide.EndsWith("-") Then
                    blnIsProteinTerminus = True
                Else
                    blnIsProteinTerminus = False
                End If

                If dblPValue <= 0.2 OrElse dblTotalPRMScore >= 50 OrElse dblFScore >= 0 Then
                    udtPHRPData.PassesFilters = True
                End If

            End If

        Catch ex As Exception
            MyBase.ReportError("Error parsing line " & intLineNumber & " in the Inspect PHRP " & strPHRPSource & " file: " & ex.Message)
        End Try

        Return blnSuccess

    End Function

End Class
