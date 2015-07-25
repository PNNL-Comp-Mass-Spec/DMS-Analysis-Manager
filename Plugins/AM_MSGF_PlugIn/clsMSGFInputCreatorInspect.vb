
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

Imports PHRPReader

Public Class clsMSGFInputCreatorInspect
    Inherits clsMSGFInputCreator

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="strDatasetName">Dataset name</param>
	''' <param name="strWorkDir">Working directory</param>
	''' <remarks></remarks>
	Public Sub New(strDatasetName As String, strWorkDir As String)

        MyBase.New(strDatasetName, strWorkDir, clsPHRPReader.ePeptideHitResultType.Inspect)

    End Sub

    Protected Overrides Sub InitializeFilePaths()

        ' Customize mPHRPResultFilePath for Inspect synopsis files
        mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserInspect.GetPHRPFirstHitsFileName(mDatasetName))
        mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserInspect.GetPHRPSynopsisFileName(mDatasetName))

    End Sub

    Protected Overrides Function PassesFilters(ByVal objPSM As clsPSM) As Boolean
        Dim dblPValue As Double
        Dim dblTotalPRMScore As Double
        Dim dblFScore As Double

        Dim blnIsProteinTerminus As Boolean
        Dim blnPassesFilters As Boolean

        ' Keep Inspect results with pValue <= 0.2 Or TotalPRMScore >= 50 or FScore >= 0
        ' PHRP has likely already filtered the _inspect_syn.txt file using these filters

        If objPSM.Peptide.StartsWith("-"c) OrElse objPSM.Peptide.EndsWith("-") Then
            blnIsProteinTerminus = True
        Else
            blnIsProteinTerminus = False
        End If

        dblPValue = objPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_PValue)
        dblTotalPRMScore = objPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_TotalPRMScore)
        dblFScore = objPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_FScore)

        If dblPValue <= 0.2 OrElse dblTotalPRMScore >= 50 OrElse dblFScore >= 0 Then
            blnPassesFilters = True
        End If

        Return blnPassesFilters

    End Function

End Class
