'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
' This class reads a Sequest _syn.txt file in support of creating the input file for MSGF 
'
'*********************************************************************************************************

Option Strict On

Public Class clsMSGFInputCreatorSequest
    Inherits clsMSGFInputCreator

    Public Const DATA_COLUMN_HitNum As String = "HitNum"
    Public Const DATA_COLUMN_ScanNum As String = "ScanNum"
    Public Const DATA_COLUMN_ScanCount As String = "ScanCount"
    Public Const DATA_COLUMN_ChargeState As String = "ChargeState"
    Public Const DATA_COLUMN_MH As String = "MH"
    Public Const DATA_COLUMN_XCorr As String = "XCorr"
    Public Const DATA_COLUMN_DelCn As String = "DelCn"
    Public Const DATA_COLUMN_Sp As String = "Sp"
    Public Const DATA_COLUMN_Reference As String = "Reference"
    Public Const DATA_COLUMN_MultiProtein As String = "MultiProtein"
    Public Const DATA_COLUMN_Peptide As String = "Peptide"
    Public Const DATA_COLUMN_DelCn2 As String = "DelCn2"
    Public Const DATA_COLUMN_RankSp As String = "RankSp"
    Public Const DATA_COLUMN_RankXc As String = "RankXc"
    Public Const DATA_COLUMN_DelM As String = "DelM"
    Public Const DATA_COLUMN_XcRatio As String = "XcRatio"
    Public Const DATA_COLUMN_PassFilt As String = "PassFilt"
    Public Const DATA_COLUMN_MScore As String = "MScore"
    Public Const DATA_COLUMN_Ions_Observed As String = "Ions_Observed"
    Public Const DATA_COLUMN_Ions_Expected As String = "Ions_Expected"
    Public Const DATA_COLUMN_NumTrypticEnds As String = "NumTrypticEnds"
    Public Const DATA_COLUMN_DelM_PPM As String = "DelM_PPM"

    Public Sub New(ByVal strDatasetName As String, _
                   ByVal strWorkDir As String, _
                   ByRef objDynamicMods As System.Collections.Generic.SortedDictionary(Of String, String), _
                   ByRef objStaticMods As System.Collections.Generic.SortedDictionary(Of String, String))

        MyBase.New(strDatasetName, strWorkDir, objDynamicMods, objStaticMods)

    End Sub

    Protected Overrides Sub DefineColumnHeaders()

        mColumnHeaders.Clear()

        ' Define the default column mapping
        mColumnHeaders.Add(DATA_COLUMN_HitNum, 0)
        mColumnHeaders.Add(DATA_COLUMN_ScanNum, 1)
        mColumnHeaders.Add(DATA_COLUMN_ScanCount, 2)
        mColumnHeaders.Add(DATA_COLUMN_ChargeState, 3)
        mColumnHeaders.Add(DATA_COLUMN_MH, 4)
        mColumnHeaders.Add(DATA_COLUMN_XCorr, 5)
        mColumnHeaders.Add(DATA_COLUMN_DelCn, 6)
        mColumnHeaders.Add(DATA_COLUMN_Sp, 7)
        mColumnHeaders.Add(DATA_COLUMN_Reference, 8)
        mColumnHeaders.Add(DATA_COLUMN_MultiProtein, 9)
        mColumnHeaders.Add(DATA_COLUMN_Peptide, 10)
        mColumnHeaders.Add(DATA_COLUMN_DelCn2, 11)
        mColumnHeaders.Add(DATA_COLUMN_RankSp, 12)
        mColumnHeaders.Add(DATA_COLUMN_RankXc, 13)
        mColumnHeaders.Add(DATA_COLUMN_DelM, 14)
        mColumnHeaders.Add(DATA_COLUMN_XcRatio, 15)

        mColumnHeaders.Add(DATA_COLUMN_PassFilt, -1)
        mColumnHeaders.Add(DATA_COLUMN_MScore, -1)
        mColumnHeaders.Add(DATA_COLUMN_Ions_Observed, 16)
        mColumnHeaders.Add(DATA_COLUMN_Ions_Expected, 17)

        mColumnHeaders.Add(DATA_COLUMN_NumTrypticEnds, 18)
        mColumnHeaders.Add(DATA_COLUMN_DelM_PPM, 19)

    End Sub

    Public Shared Function GetPHRPFirstHitsFileName(ByVal strDatasetName As String) As String
        Return strDatasetName & "_fht.txt"
    End Function

    Public Shared Function GetPHRPSynopsisFileName(ByVal strDatasetName As String) As String
        Return strDatasetName & "_syn.txt"
    End Function

	Public Shared Function GetPHRPResultToSeqMapFileName(ByVal strDatasetName As String) As String
		Return strDatasetName & "_syn_ResultToSeqMap.txt"
	End Function

	Public Shared Function GetPHRPSeqToProteinMapFileName(ByVal strDatasetName As String) As String
		Return strDatasetName & "_syn_SeqToProteinMap.txt"
	End Function

    Protected Overrides Sub InitializeFilePaths()

        ' Customize mPHRPResultFilePath for Sequest synopsis files
        mPHRPFirstHitsFilePath = System.IO.Path.Combine(mWorkDir, GetPHRPFirstHitsFileName(mDatasetName))
        mPHRPSynopsisFilePath = System.IO.Path.Combine(mWorkDir, GetPHRPSynopsisFileName(mDatasetName))

        UpdateMSGFInputOutputFilePaths()

    End Sub

    Protected Overrides Function ParsePHRPDataLine(ByVal intLineNumber As Integer, _
                                                   ByRef strPHRPSource As String, _
                                                   ByRef strColumns() As String, _
                                                   ByRef udtPHRPData As udtPHRPDataLine) As Boolean
        Dim dblXCorr As Double
        Dim dblDeltaCN As Double
        Dim intCleavageState As Short
        Dim blnIsProteinTerminus As Boolean

        Dim blnSuccess As Boolean

        Try

            udtPHRPData.Clear()

            With udtPHRPData
                .Title = String.Empty

                .ScanNumber = LookupColumnValue(strColumns, DATA_COLUMN_ScanNum, mColumnHeaders, -100)
                If .ScanNumber = -100 Then
                    ' Data line is not valid
                Else
                    .Peptide = LookupColumnValue(strColumns, DATA_COLUMN_Peptide, mColumnHeaders)
                    .Charge = CType(LookupColumnValue(strColumns, DATA_COLUMN_ChargeState, mColumnHeaders, 0), Short)
                    .ProteinFirst = LookupColumnValue(strColumns, DATA_COLUMN_Reference, mColumnHeaders)
                    .ResultID = LookupColumnValue(strColumns, DATA_COLUMN_HitNum, mColumnHeaders, 0)

                    dblXCorr = LookupColumnValue(strColumns, DATA_COLUMN_XCorr, mColumnHeaders, CDbl(0))
                    dblDeltaCN = LookupColumnValue(strColumns, DATA_COLUMN_DelCn, mColumnHeaders, CDbl(0))
                    intCleavageState = CType(LookupColumnValue(strColumns, DATA_COLUMN_NumTrypticEnds, mColumnHeaders, 0), Short)

                    ' This will be updated to true below if the peptide passes the filters
                    .PassesFilters = False

                    blnSuccess = True
                End If
            End With

            If blnSuccess Then
                ' Examine the score values and possibly filter out this line

                ' Sequest filter rules are relaxed forms of the MTS Peptide DB Minima (Peptide DB minima 6, filter set 149)
                ' All data must have DelCn <= 0.25
                ' For Partially or fully tryptic, or protein terminal; 
                '    XCorr >= 1.5 for 1+ or 2+
                '    XCorr >= 2.2 for >=3+
                ' For non-tryptic:
                '    XCorr >= 1.5 for 1+
                '    XCorr >= 2.0 for 2+
                '    XCorr >= 2.5 for >=3+

                If udtPHRPData.Peptide.StartsWith("-"c) OrElse udtPHRPData.Peptide.EndsWith("-") Then
                    blnIsProteinTerminus = True
                Else
                    blnIsProteinTerminus = False
                End If

                If dblDeltaCN <= 0.25 Then
                    If intCleavageState >= 1 OrElse blnIsProteinTerminus Then
                        ' Partially or fully tryptic, or protein terminal
                        If udtPHRPData.Charge = 1 Or udtPHRPData.Charge = 2 Then
                            If dblXCorr >= 1.5 Then udtPHRPData.PassesFilters = True
                        Else
                            ' Charge is 3 or higher (or zero)
                            If dblXCorr >= 2.2 Then udtPHRPData.PassesFilters = True
                        End If
                    Else
                        ' Non-tryptic
                        If udtPHRPData.Charge = 1 Then
                            If dblXCorr >= 1.5 Then udtPHRPData.PassesFilters = True
                        ElseIf udtPHRPData.Charge = 2 Then
                            If dblXCorr >= 2.0 Then udtPHRPData.PassesFilters = True
                        Else
                            ' Charge is 3 or higher (or zero)
                            If dblXCorr >= 2.5 Then udtPHRPData.PassesFilters = True
                        End If
                    End If
                End If

            End If

        Catch ex As Exception
            MyBase.ReportError("Error parsing line " & intLineNumber & " in the Sequest PHRP " & strPHRPSource & " file: " & ex.Message)
        End Try

        Return blnSuccess

    End Function

End Class
