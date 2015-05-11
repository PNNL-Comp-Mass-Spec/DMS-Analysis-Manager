'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 04/23/2014
'
' This class reads an MODa _syn.txt file in support of creating the input file for MSGF 
'
'*********************************************************************************************************

Option Strict On

Imports PHRPReader

Public Class clsMSGFInputCreatorMODa
    Inherits clsMSGFInputCreator

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="strDatasetName">Dataset name</param>
    ''' <param name="strWorkDir">Working directory</param>
    ''' <remarks></remarks>
    Public Sub New(ByVal strDatasetName As String, ByVal strWorkDir As String)

        MyBase.New(strDatasetName, strWorkDir, clsPHRPReader.ePeptideHitResultType.MODa)

    End Sub

    Protected Overrides Sub InitializeFilePaths()

        ' Customize mPHRPResultFilePath for MODa _syn.txt files
        mPHRPFirstHitsFilePath = String.Empty
        mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, PHRPReader.clsPHRPParserMODa.GetPHRPSynopsisFileName(mDatasetName))

    End Sub

    Protected Overrides Function PassesFilters(ByRef objPSM As PHRPReader.clsPSM) As Boolean
        Dim dblProbability As Double

        Dim blnPassesFilters As Boolean

        ' Keep MODa results with Probability >= 0.2  (higher probability values are better)
        ' This will typically keep all data in the _syn.txt file

        dblProbability = objPSM.GetScoreDbl(PHRPReader.clsPHRPParserMODa.DATA_COLUMN_Probability, 0)
        If dblProbability >= 0.2 Then
            blnPassesFilters = True
        End If

        Return blnPassesFilters

    End Function

End Class
