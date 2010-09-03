'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
' This class reads an X!Tandem _xt.txt file in support of creating the input file for MSGF 
'
'*********************************************************************************************************

Option Strict On

Public Class clsMSGFInputCreatorXTandem
    Inherits clsMSGFInputCreator

    Protected Const DATA_COLUMN_Result_ID As String = "Result_ID"
    Protected Const DATA_COLUMN_Group_ID As String = "Group_ID"
    Protected Const DATA_COLUMN_Scan As String = "Scan"
    Protected Const DATA_COLUMN_Charge As String = "Charge"
    Protected Const DATA_COLUMN_Peptide_MH As String = "Peptide_MH"
    Protected Const DATA_COLUMN_Peptide_Hyperscore As String = "Peptide_Hyperscore"
    Protected Const DATA_COLUMN_Peptide_Expectation_Value_LogE As String = "Peptide_Expectation_Value_Log(e)"
    Protected Const DATA_COLUMN_Multiple_Protein_Count As String = "Multiple_Protein_Count"
    Protected Const DATA_COLUMN_Peptide_Sequence As String = "Peptide_Sequence"
    Protected Const DATA_COLUMN_DeltaCn2 As String = "DeltaCn2"
    Protected Const DATA_COLUMN_y_score As String = "y_score"
    Protected Const DATA_COLUMN_y_ions As String = "y_ions"
    Protected Const DATA_COLUMN_b_score As String = "b_score"
    Protected Const DATA_COLUMN_b_ions As String = "b_ions"
    Protected Const DATA_COLUMN_Delta_Mass As String = "Delta_Mass"
    Protected Const DATA_COLUMN_Peptide_Intensity_LogI As String = "Peptide_Intensity_Log(I)"

    Public Sub New(ByVal strDatasetName As String, _
                   ByVal strWorkDir As String, _
                   ByRef objDynamicMods As System.Collections.Generic.SortedDictionary(Of String, String), _
                   ByRef objStaticMods As System.Collections.Generic.SortedDictionary(Of String, String))

        MyBase.New(strDatasetName, strWorkDir, objDynamicMods, objStaticMods)

    End Sub

    Protected Overrides Sub DefineColumnHeaders()

        mColumnHeaders.Clear()

        ' Define the default column mapping
        mColumnHeaders.Add(DATA_COLUMN_Result_ID, 0)
        mColumnHeaders.Add(DATA_COLUMN_Group_ID, 1)
        mColumnHeaders.Add(DATA_COLUMN_Scan, 2)
        mColumnHeaders.Add(DATA_COLUMN_Charge, 3)
        mColumnHeaders.Add(DATA_COLUMN_Peptide_MH, 4)
        mColumnHeaders.Add(DATA_COLUMN_Peptide_Hyperscore, 5)
        mColumnHeaders.Add(DATA_COLUMN_Peptide_Expectation_Value_LogE, 6)
        mColumnHeaders.Add(DATA_COLUMN_Multiple_Protein_Count, 7)
        mColumnHeaders.Add(DATA_COLUMN_Peptide_Sequence, 8)
        mColumnHeaders.Add(DATA_COLUMN_DeltaCn2, 9)
        mColumnHeaders.Add(DATA_COLUMN_y_score, 10)
        mColumnHeaders.Add(DATA_COLUMN_y_ions, 11)
        mColumnHeaders.Add(DATA_COLUMN_b_score, 12)
        mColumnHeaders.Add(DATA_COLUMN_b_ions, 13)
        mColumnHeaders.Add(DATA_COLUMN_Delta_Mass, 14)
        mColumnHeaders.Add(DATA_COLUMN_Peptide_Intensity_LogI, 15)

    End Sub

    Public Shared Function GetPHRPFirstHitsFileName(ByVal strDatasetName As String) As String
        ' X!Tandem does not have a first-hits file; just the _xt.txt file
        Return String.Empty
    End Function

    Public Shared Function GetPHRPSynopsisFileName(ByVal strDatasetName As String) As String
        Return strDatasetName & "_xt.txt"
    End Function

    Protected Overrides Sub InitializeFilePaths()

        ' Customize mPHRPResultFilePath for X!Tandem _xt.txt files
        mPHRPFirstHitsFilePath = String.Empty
        mPHRPSynopsisFilePath = System.IO.Path.Combine(mWorkDir, GetPHRPSynopsisFileName(mDatasetName))

        UpdateMSGFInputOutputFilePaths()

    End Sub

    Protected Overrides Function ParsePHRPDataLine(ByVal intLineNumber As Integer, _
                                                   ByRef strPHRPSource As String, _
                                                   ByRef strColumns() As String, _
                                                   ByRef udtPHRPData As udtPHRPDataLine) As Boolean
        Dim dblHyperscore As Double
        Dim dblLogEValue As Double

        Dim blnSuccess As Boolean

        Try

            udtPHRPData.Clear()

            With udtPHRPData
                .Title = String.Empty

                .ScanNumber = LookupColumnValue(strColumns, DATA_COLUMN_Scan, mColumnHeaders, -100)
                If .ScanNumber = -100 Then
                    ' Data line is not valid
                Else
                    .Peptide = LookupColumnValue(strColumns, DATA_COLUMN_Peptide_Sequence, mColumnHeaders)
                    .Charge = CType(LookupColumnValue(strColumns, DATA_COLUMN_Charge, mColumnHeaders, 0), Short)
                    .ProteinFirst = ""                       ' Protein name is not stored in the _xt.txt file, so we just keep this column blank
                    .ResultID = LookupColumnValue(strColumns, DATA_COLUMN_Result_ID, mColumnHeaders, 0)

                    dblHyperscore = LookupColumnValue(strColumns, DATA_COLUMN_Peptide_Hyperscore, mColumnHeaders, CDbl(0))
                    dblLogEValue = LookupColumnValue(strColumns, DATA_COLUMN_Peptide_Expectation_Value_LogE, mColumnHeaders, CDbl(0))

                    ' This will be updated to true below if the peptide passes the filters
                    .PassesFilters = False

                    blnSuccess = True
                End If
            End With

            If blnSuccess Then
                ' Examine the score values and possibly filter out this line

                ' Keep X!Tandem results with Peptide_Expectation_Value_Log(e) <= -0.3
                ' This will typically keep all data in the _xt.txt file

                If dblLogEValue <= -0.3 Then
                    udtPHRPData.PassesFilters = True
                End If

            End If

        Catch ex As Exception
            MyBase.ReportError("Error parsing line " & intLineNumber & " in the X!Tandem PHRP " & strPHRPSource & " file: " & ex.Message)
        End Try

        Return blnSuccess

    End Function

End Class
