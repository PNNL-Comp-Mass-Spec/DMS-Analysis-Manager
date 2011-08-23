'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
' This class reads a msgfdb_syn.txt file in support of creating the input file for MSGF 
'
'*********************************************************************************************************

Option Strict On

Public Class clsMSGFInputCreatorMSGFDB
    Inherits clsMSGFInputCreator
	    
    Protected Const DATA_COLUMN_ResultID As String = "ResultID"
    Protected Const DATA_COLUMN_Scan As String = "Scan"
    Protected Const DATA_COLUMN_FragMethod As String = "FragMethod"
    Protected Const DATA_COLUMN_SpecIndex As String = "SpecIndex"
    Protected Const DATA_COLUMN_Charge As String = "Charge"
    Protected Const DATA_COLUMN_PrecursorMZ As String = "PrecursorMZ"
    Protected Const DATA_COLUMN_DelM As String = "DelM"
    Protected Const DATA_COLUMN_DelMPPM As String = "DelM_PPM"
    Protected Const DATA_COLUMN_MH As String = "MH"
    Protected Const DATA_COLUMN_Peptide As String = "Peptide"
    Protected Const DATA_COLUMN_Protein As String = "Protein"
    Protected Const DATA_COLUMN_NTT As String = "NTT"
    Protected Const DATA_COLUMN_DeNovoScore As String = "DeNovoScore"
    Protected Const DATA_COLUMN_MSGFScore As String = "MSGFScore"
    Protected Const DATA_COLUMN_SpecProb As String = "SpecProb"
    Protected Const DATA_COLUMN_RankSpecProb As String = "RankSpecProb"
    Protected Const DATA_COLUMN_PValue As String = "PValue"

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
        mColumnHeaders.Add(DATA_COLUMN_FragMethod, 2)
        mColumnHeaders.Add(DATA_COLUMN_SpecIndex, 3)
        mColumnHeaders.Add(DATA_COLUMN_Charge, 4)
        mColumnHeaders.Add(DATA_COLUMN_PrecursorMZ, 5)
        mColumnHeaders.Add(DATA_COLUMN_DelM, 6)
        mColumnHeaders.Add(DATA_COLUMN_DelMPPM, 7)
        mColumnHeaders.Add(DATA_COLUMN_MH, 8)
        mColumnHeaders.Add(DATA_COLUMN_Peptide, 9)
        mColumnHeaders.Add(DATA_COLUMN_Protein, 10)
        mColumnHeaders.Add(DATA_COLUMN_NTT, 11)
        mColumnHeaders.Add(DATA_COLUMN_DeNovoScore, 12)
        mColumnHeaders.Add(DATA_COLUMN_MSGFScore, 13)
        mColumnHeaders.Add(DATA_COLUMN_SpecProb, 14)
        mColumnHeaders.Add(DATA_COLUMN_RankSpecProb, 15)
        mColumnHeaders.Add(DATA_COLUMN_PValue, 16)
        
    End Sub

    Public Shared Function GetPHRPFirstHitsFileName(ByVal strDatasetName As String) As String
        Return strDatasetName & "_msgfdb_fht.txt"
    End Function

    Public Shared Function GetPHRPSynopsisFileName(ByVal strDatasetName As String) As String
        Return strDatasetName & "_msgfdb_syn.txt"
    End Function

    Protected Overrides Sub InitializeFilePaths()

        ' Customize mPHRPResultFilePath for MSGFDB synopsis files
        mPHRPFirstHitsFilePath = System.IO.Path.Combine(mWorkDir, GetPHRPFirstHitsFileName(mDatasetName))
        mPHRPSynopsisFilePath = System.IO.Path.Combine(mWorkDir, GetPHRPSynopsisFileName(mDatasetName))

        UpdateMSGFInputOutputFilePaths()

    End Sub

    Protected Overrides Function ParsePHRPDataLine(ByVal intLineNumber As Integer, _
                                                   ByRef strPHRPSource As String, _
                                                   ByRef strColumns() As String, _
                                                   ByRef udtPHRPData As udtPHRPDataLine) As Boolean
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

                    .PassesFilters = True

                    blnSuccess = True
                End If
            End With

        Catch ex As Exception
            MyBase.ReportError("Error parsing line " & intLineNumber & " in the MSGFDB PHRP " & strPHRPSource & " file: " & ex.Message)
        End Try

        Return blnSuccess

    End Function

End Class
