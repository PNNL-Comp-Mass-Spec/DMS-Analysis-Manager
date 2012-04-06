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

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="strDatasetName">Dataset name</param>
	''' <param name="strWorkDir">Working directory</param>
	''' <remarks></remarks>
	Public Sub New(ByVal strDatasetName As String, ByVal strWorkDir As String)

		MyBase.New(strDatasetName, strWorkDir, PHRPReader.clsPHRPReader.ePeptideHitResultType.XTandem)

	End Sub

    Protected Overrides Sub InitializeFilePaths()

        ' Customize mPHRPResultFilePath for X!Tandem _xt.txt files
		mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, PHRPReader.clsPHRPParserXTandem.GetPHRPFirstHitsFileName(mDatasetName))
		mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, PHRPReader.clsPHRPParserXTandem.GetPHRPSynopsisFileName(mDatasetName))

    End Sub

    Protected Overrides Function PassesFilters(ByRef objPSM As PHRPReader.clsPSM) As Boolean
		Dim dblLogEValue As Double

		Dim blnPassesFilters As Boolean

		' Keep X!Tandem results with Peptide_Expectation_Value_Log(e) <= -0.3
		' This will typically keep all data in the _xt.txt file

		dblLogEValue = objPSM.GetScoreDbl(PHRPReader.clsPHRPParserXTandem.DATA_COLUMN_Peptide_Expectation_Value_LogE, 0)
		If dblLogEValue <= -0.3 Then
			blnPassesFilters = True
		End If

		Return blnPassesFilters

    End Function

End Class
